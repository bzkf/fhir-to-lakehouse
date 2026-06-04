#!/usr/bin/env python3
"""
FHIR Server to Lakehouse

Downloads all resources from a FHIR server using blazectl and stores them as
NDJSON in an S3 bucket.  Supports:

  - All resource types (auto-discovered from the server's CapabilityStatement)
  - Incremental downloads via the FHIR ``_lastUpdated`` search parameter
  - Concurrent downloads per resource type
  - Importing the resulting files into Pathling via the ``$import`` operation

Usage:
    # Full export of all resource types
    python server_to_lakehouse.py export \\
        --server http://fhir-server/fhir \\
        --bucket my-bucket --prefix fhir-export

    # Incremental export (only resources updated since the last run)
    python server_to_lakehouse.py export \\
        --server http://fhir-server/fhir \\
        --bucket my-bucket --prefix fhir-export \\
        --incremental

    # Import NDJSON files from S3 into Pathling
    python server_to_lakehouse.py import \\
        --pathling http://pathling-server \\
        --bucket my-bucket --prefix fhir-export
"""

from __future__ import annotations

import contextlib
import json
import re
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

import boto3
import click
import requests
from botocore.exceptions import ClientError
from loguru import logger

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Polling backoff settings for async $import status polling.
_POLL_BACKOFF_INITIAL = 2.0  # seconds for first poll interval
_POLL_BACKOFF_FACTOR = 1.5  # multiplier applied after each 202 response
_POLL_BACKOFF_MAX = 30.0  # maximum interval in seconds

_FHIR_JSON_MIME_TYPE = "application/fhir+json"


def _s3_key(prefix: str, resource_type: str, run_at: str) -> str:
    """Return the S3 key for one run's NDJSON file for *resource_type*.

    Layout: ``<prefix>/<ResourceType>/<run_at>.ndjson``

    Each export run produces a new, independent file.  Old files are cleaned up
    automatically via an S3 lifecycle rule (see ``--expiration-days``).
    """
    return f"{prefix.rstrip('/')}/{resource_type}/{run_at}.ndjson"


def _state_key(prefix: str) -> str:
    return f"{prefix.rstrip('/')}/.export_state.json"


def _manifest_key(prefix: str) -> str:
    return f"{prefix.rstrip('/')}/.manifest.json"


# ---------------------------------------------------------------------------
# FHIR server discovery
# ---------------------------------------------------------------------------


def get_resource_types_from_server(
    server_url: str,
    *,
    user: str | None = None,
    password: str | None = None,
    timeout: int = 30,
) -> list[str]:
    """Fetch supported resource types from the FHIR CapabilityStatement.

    Raises ``RuntimeError`` if the CapabilityStatement cannot be retrieved or
    contains no resource types.
    """
    headers: dict[str, str] = {"Accept": _FHIR_JSON_MIME_TYPE}
    auth = (user, password) if user and password else None
    resp = requests.get(
        f"{server_url.rstrip('/')}/metadata",
        headers=headers,
        auth=auth,
        timeout=timeout,
    )
    resp.raise_for_status()
    cs = resp.json()
    resource_types = [
        r["type"] for rest in cs.get("rest", []) for r in rest.get("resource", [])
    ]
    if not resource_types:
        raise RuntimeError(
            "CapabilityStatement returned no resource types. "
            "Use --resource-types to specify them explicitly."
        )
    logger.info(
        "Discovered {} resource type(s) from CapabilityStatement", len(resource_types)
    )
    return resource_types


# ---------------------------------------------------------------------------
# Incremental state (stored as JSON in S3)
# ---------------------------------------------------------------------------


def load_state(s3_client, bucket: str, prefix: str) -> dict[str, str]:
    """Load per-resource-type last-export timestamps from S3."""
    key = _state_key(prefix)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        state: dict[str, str] = json.loads(obj["Body"].read())
        logger.debug("Loaded export state for {} resource type(s)", len(state))
        return state
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {}
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not load export state: {}", exc)
        return {}


def save_state(s3_client, bucket: str, prefix: str, state: dict[str, str]) -> None:
    """Persist per-resource-type last-export timestamps to S3."""
    s3_client.put_object(
        Bucket=bucket,
        Key=_state_key(prefix),
        Body=json.dumps(state, indent=2).encode(),
        ContentType="application/json",
    )
    logger.debug("Saved export state for {} resource type(s)", len(state))


# ---------------------------------------------------------------------------
# blazectl download
# ---------------------------------------------------------------------------


_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$"
)


def _validate_timestamp(value: str) -> str:
    """Raise ValueError if *value* is not a safe ISO-8601 date/datetime string."""
    if not _ISO8601_RE.match(value):
        raise ValueError(
            f"Invalid timestamp {value!r}: must be ISO-8601 (e.g. 2024-01-01T00:00:00Z)"
        )
    return value


def blazectl_download(
    server_url: str,
    resource_type: str,
    output_file: Path,
    *,
    blazectl_bin: str = "blazectl",
    last_updated_gt: datetime | None = None,
    user: str | None = None,
    password: str | None = None,
    insecure: bool = False,
) -> None:
    """
    Run ``blazectl download`` for *resource_type*, writing NDJSON to *output_file*.

    *output_file* is provided by the caller and must not already exist; blazectl
    will not overwrite an existing file.  The caller is responsible for
    supplying a fresh path (e.g. inside a ``TemporaryDirectory``).

    When the FHIR server returns no matching resources for the given type,
    blazectl exits with return code 0 without creating the output file.

    The ``_lastUpdated`` FHIR search parameter is used for incremental exports:
    ``_lastUpdated=gt{last_updated_gt}``.
    """
    cmd: list[str] = [
        blazectl_bin,
        "download",
        resource_type,
        "--server",
        server_url,
        "--no-progress",
        "--output-file",
        str(output_file),
    ]

    if user and password:
        cmd += ["--user", user, "--password", password]

    if insecure:
        cmd += ["--insecure"]

    if last_updated_gt:
        cmd += ["--query", f"_lastUpdated=gt{last_updated_gt.isoformat()}"]

    logger.info(
        "Downloading {}{} via {}",
        resource_type,
        f" (since {last_updated_gt})" if last_updated_gt else "",
        " ".join(cmd),
    )

    result = subprocess.run(cmd, capture_output=True)  # noqa: S603

    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace")
        raise RuntimeError(
            f"blazectl exited with code {result.returncode} for {resource_type}:"
            + f"\n{stderr}"
        )


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def s3_upload_file(s3_client, bucket: str, key: str, file_path: Path) -> None:
    s3_client.upload_file(
        str(file_path),
        bucket,
        key,
        ExtraArgs={"ContentType": "application/ndjson"},
    )


# ---------------------------------------------------------------------------
# S3 lifecycle rule for automatic expiration
# ---------------------------------------------------------------------------


def ensure_lifecycle_rule(
    s3_client,
    bucket: str,
    prefix: str,
    expiration_days: int,
) -> None:
    """Create or update a bucket lifecycle rule that expires NDJSON export files.

    The rule applies to all objects under ``<prefix>/`` so it covers every
    resource-type sub-prefix.  Any previously registered rule for this prefix
    is replaced.  Passing *expiration_days* = 0 removes the rule.
    """
    rule_id = f"fhir-export-expiry-{prefix.strip('/').replace('/', '-')}"
    ndjson_prefix = f"{prefix.rstrip('/')}/"

    try:
        resp = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
        existing_rules: list[dict] = resp.get("Rules", [])
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
            existing_rules = []
        else:
            raise

    # Remove any existing rule with our ID so we can re-add the updated one.
    rules = [r for r in existing_rules if r["ID"] != rule_id]

    if expiration_days > 0:
        rules.append(
            {
                "ID": rule_id,
                "Status": "Enabled",
                "Filter": {"Prefix": ndjson_prefix},
                "Expiration": {"Days": expiration_days},
            }
        )
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={"Rules": rules},
        )
        logger.info(
            "S3 lifecycle rule set: objects under s3://{}/{} expire after {} day(s)",
            bucket,
            ndjson_prefix,
            expiration_days,
        )
    elif rules != existing_rules:
        # expiration_days == 0: the caller wants to disable the rule.
        # If our rule was removed above, persist the remaining rules (or
        # delete the lifecycle configuration entirely if no rules remain).
        if rules:
            s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket,
                LifecycleConfiguration={"Rules": rules},
            )
        else:
            with contextlib.suppress(ClientError):
                s3_client.delete_bucket_lifecycle(Bucket=bucket)


# ---------------------------------------------------------------------------
# Per-resource-type worker
# ---------------------------------------------------------------------------


def process_resource_type(
    *,
    s3_client,
    server_url: str,
    resource_type: str,
    bucket: str,
    prefix: str,
    run_at: str,
    last_updated_gt: datetime | None,
    blazectl_bin: str,
    user: str | None,
    password: str | None,
    insecure: bool,
) -> tuple[str, str | None]:
    """
    Download one resource type into a temp file and upload to S3.

    Each run writes a new, independent file at
    ``<prefix>/<ResourceType>/<run_at>.ndjson``.  No in-memory merge is
    performed, so this is safe for arbitrarily large resource sets.

    Returns ``(resource_type, s3_url)``; *s3_url* is ``None`` when the
    download produced no data (resource type not present on the server).
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_file = Path(tmp_dir) / f"{resource_type}.ndjson"

        blazectl_download(
            server_url,
            resource_type,
            tmp_file,
            blazectl_bin=blazectl_bin,
            last_updated_gt=last_updated_gt,
            user=user,
            password=password,
            insecure=insecure,
        )

        # blazectl exits 0 without creating the file when no resources match,
        # and creates an empty file when the result set is genuinely empty.
        # Both cases mean there is nothing to upload.
        if not tmp_file.exists() or tmp_file.stat().st_size == 0:
            logger.info("{} - no resources returned, skipping", resource_type)
            return resource_type, None

        key = _s3_key(prefix, resource_type, run_at)
        s3_upload_file(s3_client, bucket, key, tmp_file)

    s3_url = f"s3://{bucket}/{key}"
    logger.info("{} - uploaded to {}", resource_type, s3_url)
    return resource_type, s3_url


# ---------------------------------------------------------------------------
# export command
# ---------------------------------------------------------------------------


def cmd_export(
    *,
    server: str,
    bucket: str,
    prefix: str,
    resource_types: str | None,
    exclude: str | None,
    incremental: bool,
    since: str | None,
    concurrency: int,
    blazectl_bin: str,
    user: str | None,
    password: str | None,
    insecure: bool,
    s3_endpoint: str | None,
    aws_region: str,
    expiration_days: int,
) -> int:
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint or None,
        region_name=aws_region,
    )

    # ---- Determine resource types ----------------------------------------
    resource_types_list: list[str]
    if resource_types:
        resource_types_list = [
            rt.strip() for rt in resource_types.split(",") if rt.strip()
        ]
    else:
        resource_types_list = get_resource_types_from_server(
            server,
            user=user,
            password=password,
        )

    if exclude:
        excluded = {rt.strip() for rt in exclude.split(",")}
        resource_types_list = [rt for rt in resource_types_list if rt not in excluded]

    logger.info("Will export {} resource type(s)", len(resource_types_list))

    # ---- Load incremental state ------------------------------------------
    state: dict[str, str] = {}
    if incremental:
        if since:
            logger.info("Incremental mode: using --since override {}", since)
            state = {rt: datetime.fromisoformat(since) for rt in resource_types_list}
        else:
            state = load_state(s3_client, bucket, prefix)
            logger.info(
                "Incremental mode: loaded state for {} resource type(s)", len(state)
            )

    # ---- Concurrent downloads -------------------------------------------
    new_state: dict[str, str] = dict(state)
    uploaded_urls: dict[str, str] = {}
    errors: list[tuple[str, str]] = []
    _now = datetime.now(UTC)
    # Millisecond-precision key fragment used as the NDJSON filename component
    # (e.g. 20240101T000000123Z).  Millisecond resolution prevents S3 key
    # collisions when two runs start within the same second.
    run_at_key = (
        _now.strftime("%Y%m%dT%H%M%S") + f"{_now.microsecond // 1000:03d}" + "Z"
    )
    export_run_at = _now.isoformat()

    def _worker(rt: str) -> tuple[str, str | None]:
        return process_resource_type(
            s3_client=s3_client,
            server_url=server,
            resource_type=rt,
            bucket=bucket,
            prefix=prefix,
            run_at=run_at_key,
            last_updated_gt=datetime.fromisoformat(state.get(rt))
            if incremental
            else None,
            blazectl_bin=blazectl_bin,
            user=user,
            password=password,
            insecure=insecure,
        )

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(_worker, rt): rt for rt in resource_types_list}
        for future in as_completed(futures):
            rt = futures[future]
            try:
                resource_type, s3_url = future.result()
                new_state[resource_type] = export_run_at
                if s3_url:
                    uploaded_urls[resource_type] = s3_url
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to export {}: {}", rt, exc)
                errors.append((rt, str(exc)))

    # ---- S3 lifecycle rule (auto-expiration) ----------------------------
    # Apply regardless of whether this run produced uploads so the rule is
    # always configured when --expiration-days is specified.
    if expiration_days > 0:
        try:
            ensure_lifecycle_rule(s3_client, bucket, prefix, expiration_days)
        except ClientError as exc:
            logger.warning("Could not set S3 lifecycle rule: {}", exc)

    # ---- Persist state and manifest --------------------------------------
    if incremental:
        save_state(s3_client, bucket, prefix, new_state)

    if uploaded_urls:
        manifest = {
            "exported_at": export_run_at,
            "incremental": incremental,
            "resources": uploaded_urls,
        }
        s3_client.put_object(
            Bucket=bucket,
            Key=_manifest_key(prefix),
            Body=json.dumps(manifest, indent=2).encode(),
            ContentType="application/json",
        )
        logger.info("Saved manifest with {} resource type(s)", len(uploaded_urls))

    # ---- Summary ---------------------------------------------------------
    logger.info(
        "Export finished: {} uploaded, {} skipped (no data), {} error(s)",
        len(uploaded_urls),
        len(resource_types_list) - len(uploaded_urls) - len(errors),
        len(errors),
    )
    if errors:
        for rt, msg in errors:
            logger.error("  {}: {}", rt, msg)
        return 1

    return 0


def _poll_import_job(
    status_url: str,
    *,
    auth: tuple[str, str] | None,
    request_timeout: int,
) -> dict:
    """Poll the Pathling async $import job status URL until completion.

    Polls indefinitely with exponential backoff (2 s → 30 s cap).
    Returns the final response body on success (HTTP 200).
    Raises ``requests.HTTPError`` for any unexpected HTTP status.
    """
    interval = _POLL_BACKOFF_INITIAL

    while True:
        response = requests.get(
            status_url,
            headers={"Accept": _FHIR_JSON_MIME_TYPE},
            auth=auth,
            timeout=request_timeout,
        )

        if response.status_code == 200:
            response_data = response.json()
            logger.info("Pathling $import completed successfully")
            logger.debug("Response:\n{}", json.dumps(response_data, indent=2))
            return response_data
        elif response.status_code == 202:
            progress = response.headers.get("X-Progress", "in progress")
            logger.info("Pathling $import in progress: {}", progress)
            time.sleep(interval)
            interval = min(interval * _POLL_BACKOFF_FACTOR, _POLL_BACKOFF_MAX)
        else:
            response.raise_for_status()


def cmd_import(
    *,
    pathling: str,
    bucket: str,
    prefix: str,
    mode: str,
    no_manifest: bool,
    user: str | None,
    password: str | None,
    timeout: int,
    s3_endpoint: str | None,
    aws_region: str,
    pathling_url_prefix: str | None,
) -> int:
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint or None,
        region_name=aws_region,
    )

    # ---- Discover NDJSON files in S3 ------------------------------------
    resource_urls: dict[str, str] = {}

    if not no_manifest:
        manifest_key = _manifest_key(prefix)
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=manifest_key)
            manifest = json.loads(obj["Body"].read())
            resource_urls = manifest.get("resources", {})
            logger.info("Loaded manifest with {} resource type(s)", len(resource_urls))
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                logger.warning(
                    "Manifest not found; listing objects under prefix instead"
                )
            else:
                raise

    if not resource_urls:
        s3_prefix = prefix.rstrip("/") + "/"
        paginator = s3_client.get_paginator("list_objects_v2")
        # Collect the most-recently uploaded .ndjson per resource type.
        # Layout: <prefix>/<ResourceType>/<run_at>.ndjson
        latest: dict[
            str, tuple[str, str]
        ] = {}  # resource_type -> (last_modified_str, key)
        for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                key: str = obj["Key"]
                if not key.endswith(".ndjson"):
                    continue
                # Strip the prefix and split into [resource_type, filename].
                # Skip keys that don't match the expected two-part layout
                # (e.g. the .export_state.json or .manifest.json files).
                rel = key[len(s3_prefix) :]
                parts = rel.split("/")
                if len(parts) != 2:  # expected: <ResourceType>/<run_at>.ndjson
                    continue
                resource_type = parts[0]
                last_modified: str = obj["LastModified"].isoformat()
                if (
                    resource_type not in latest
                    or last_modified > latest[resource_type][0]
                ):
                    latest[resource_type] = (last_modified, key)
        resource_urls = {rt: f"s3://{bucket}/{key}" for rt, (_, key) in latest.items()}
        logger.info(
            "Found {} NDJSON file(s) in S3 (latest per resource type)",
            len(resource_urls),
        )

    if not resource_urls:
        logger.warning("No NDJSON files found to import - nothing to do")
        return 0

    # ---- Build Pathling Parameters resource -----------------------------
    def _pathling_url(s3_url: str) -> str:
        if not pathling_url_prefix:
            return s3_url
        # s3://bucket/key → prefix/bucket/key
        return f"{pathling_url_prefix.rstrip('/')}/{s3_url[len('s3://') :]}"

    parameters = {
        "resourceType": "Parameters",
        "parameter": [
            {
                "name": "input",
                "part": [
                    {"name": "resourceType", "valueCode": resource_type},
                    {"name": "url", "valueUrl": _pathling_url(s3_url)},
                ],
            }
            for resource_type, s3_url in sorted(resource_urls.items())
        ]
        + [
            {"name": "inputFormat", "valueCode": _FHIR_JSON_MIME_TYPE},
            {"name": "saveMode", "valueCode": mode},
        ],
    }

    logger.debug("Pathling Parameters:\n{}", json.dumps(parameters, indent=2))

    # ---- POST to Pathling $import ----------------------------------------
    headers = {
        "Content-Type": _FHIR_JSON_MIME_TYPE,
        "Accept": _FHIR_JSON_MIME_TYPE,
        "Prefer": "respond-async",
    }
    auth = (user, password) if user and password else None

    url = f"{pathling.rstrip('/')}/fhir/$import"
    logger.info(
        "Triggering Pathling $import for {} resource type(s) at {}",
        len(resource_urls),
        url,
    )

    resp = requests.post(
        url, json=parameters, headers=headers, auth=auth, timeout=timeout
    )

    if resp.status_code == 202:
        status_url = resp.headers.get("Content-Location")
        logger.info("Pathling $import accepted asynchronously (HTTP 202)")
        if not status_url:
            logger.warning(
                "No Content-Location header in 202 response; "
                "import was accepted but final status is unknown"
            )
            return 0
        logger.info("Polling status at: {}", status_url)
        _poll_import_job(status_url, auth=auth, request_timeout=timeout)
    elif resp.status_code == 200:
        logger.info("Pathling $import completed synchronously (HTTP 200)")
        logger.debug("Response:\n{}", json.dumps(resp.json(), indent=2))
    else:
        logger.error(
            "Pathling $import failed: HTTP {}\n{}", resp.status_code, resp.text
        )
        resp.raise_for_status()

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
)
def cli() -> None:
    """FHIR Server to Lakehouse - download FHIR resources from a server via blazectl and
    store them as NDJSON in S3, with optional Pathling import."""


@cli.command("export")
@click.option("--server", required=True, help="FHIR server base URL")
@click.option("--bucket", required=True, help="S3 bucket name")
@click.option(
    "--prefix", default="fhir-export", show_default=True, help="S3 key prefix"
)
@click.option(
    "--resource-types",
    default=None,
    help=(
        "Comma-separated list of resource types to export. "
        "Defaults to all types returned by the CapabilityStatement."
    ),
)
@click.option(
    "--exclude", default=None, help="Comma-separated list of resource types to skip"
)
@click.option(
    "--incremental",
    is_flag=True,
    default=False,
    help=(
        "Only download resources updated since the last export "
        "(uses _lastUpdated FHIR search parameter). "
        "Each run writes a new timestamped NDJSON file"
    ),
)
@click.option(
    "--since",
    default=None,
    help=(
        "ISO-8601 timestamp to use as the _lastUpdated lower bound "
        "(overrides saved state, implies --incremental)."
    ),
)
@click.option(
    "--concurrency",
    type=int,
    default=4,
    show_default=True,
    help="Number of resource types downloaded concurrently",
)
@click.option(
    "--blazectl",
    "blazectl_bin",
    default="blazectl",
    show_default=True,
    help="Path to the blazectl binary",
)
@click.option(
    "--user", default=None, help="Username for FHIR server basic authentication"
)
@click.option(
    "--password", default=None, help="Password for FHIR server basic authentication"
)
@click.option(
    "--insecure",
    is_flag=True,
    default=False,
    help="Allow insecure (self-signed) TLS connections",
)
@click.option(
    "--s3-endpoint", default=None, help="Custom S3 endpoint URL (e.g. for MinIO)"
)
@click.option("--aws-region", default="us-east-1", show_default=True, help="AWS region")
@click.option(
    "--expiration-days",
    type=int,
    default=0,
    show_default=True,
    help=(
        "Number of days after which exported NDJSON files are automatically "
        "deleted from S3 via a bucket lifecycle rule. "
        "0 (default) means no automatic expiration."
    ),
)
def export_cmd(
    server: str,
    bucket: str,
    prefix: str,
    resource_types: str | None,
    exclude: str | None,
    incremental: bool,
    since: str | None,
    concurrency: int,
    blazectl_bin: str,
    user: str | None,
    password: str | None,
    insecure: bool,
    s3_endpoint: str | None,
    aws_region: str,
    expiration_days: int,
) -> int:
    """Download FHIR resources via blazectl and upload NDJSON to S3."""

    # --since implies --incremental
    if since and not incremental:
        incremental = True

    return cmd_export(
        server=server,
        bucket=bucket,
        prefix=prefix,
        resource_types=resource_types,
        exclude=exclude,
        incremental=incremental,
        since=since,
        concurrency=concurrency,
        blazectl_bin=blazectl_bin,
        user=user,
        password=password,
        insecure=insecure,
        s3_endpoint=s3_endpoint,
        aws_region=aws_region,
        expiration_days=expiration_days,
    )


@cli.command("import")
@click.option(
    "--pathling",
    required=True,
    help="Pathling server base URL (e.g. http://pathling:8080)",
)
@click.option("--bucket", required=True, help="S3 bucket name")
@click.option(
    "--prefix", default="fhir-export", show_default=True, help="S3 key prefix"
)
@click.option(
    "--mode",
    default="overwrite",
    show_default=True,
    type=click.Choice(["overwrite", "merge"]),
    help=(
        "Pathling import mode. "
        "'overwrite' replaces all existing data for a resource type; "
        "'merge' adds/updates records while keeping others."
    ),
)
@click.option(
    "--no-manifest",
    "no_manifest",
    is_flag=True,
    default=False,
    help="Ignore the manifest file and list S3 objects directly",
)
@click.option("--user", default=None, help="Username for Pathling basic authentication")
@click.option(
    "--password", default=None, help="Password for Pathling basic authentication"
)
@click.option(
    "--timeout",
    type=int,
    default=300,
    show_default=True,
    help="HTTP request timeout in seconds",
)
@click.option(
    "--s3-endpoint", default=None, help="Custom S3 endpoint URL (e.g. for MinIO)"
)
@click.option("--aws-region", default="us-east-1", show_default=True, help="AWS region")
@click.option(
    "--pathling-url-prefix",
    default=None,
    help=(
        "If set, NDJSON source URLs sent to Pathling are translated from "
        "'s3://bucket/key' to '{prefix}/bucket/key'. "
        "Useful when Pathling should fetch files via HTTP from an S3-compatible "
        "store (e.g. MinIO) rather than using native S3 access."
    ),
)
def import_cmd(
    pathling: str,
    bucket: str,
    prefix: str,
    mode: str,
    no_manifest: bool,
    user: str | None,
    password: str | None,
    timeout: int,
    s3_endpoint: str | None,
    aws_region: str,
    pathling_url_prefix: str | None,
) -> int:
    """Import NDJSON files from S3 into Pathling via the $import operation."""

    return cmd_import(
        pathling=pathling,
        bucket=bucket,
        prefix=prefix,
        mode=mode,
        no_manifest=no_manifest,
        user=user,
        password=password,
        timeout=timeout,
        s3_endpoint=s3_endpoint,
        aws_region=aws_region,
        pathling_url_prefix=pathling_url_prefix,
    )


def main(argv: list[str] | None = None) -> int:
    try:
        result = cli.main(args=argv, standalone_mode=False)
        return result if isinstance(result, int) else 0
    except click.exceptions.Exit as exc:
        return exc.exit_code
    except click.ClickException as exc:
        exc.show()
        return exc.exit_code
    except click.Abort:
        click.echo("Aborted!", err=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
