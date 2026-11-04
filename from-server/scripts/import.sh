#!/bin/bash
# Merges the resources changed since the last run into the Delta Lake tables by id. The first run
# merges all resources. To limit the disk space used, the resources are downloaded in batches of
# BATCH_SIZE resources to /data/ndjson, as one <ResourceType>.ndjson file per type, and each batch
# is merged and deleted before the next one is downloaded.
set -euo pipefail

server="http://blaze:8080/fhir"
# the checkpoint stores the start time of the last download in S3, next to the tables
checkpoint_url="http://seaweedfs:8333/fhir/checkpoints/_last-download-checkpoint.json"
s3_curl=(curl -sS --aws-sigv4 "aws:amz:${AWS_REGION}:s3" --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}")

query="_count=1000"
status="$("${s3_curl[@]}" -o /tmp/checkpoint.json -w '%{http_code}' "${checkpoint_url}")"
if [[ "${status}" == 200 ]]; then
  last_download_started_at="$(jq -er .lastDownloadStartedAt /tmp/checkpoint.json)"
  echo "Downloading the resources changed since ${last_download_started_at}"
  # `ge` instead of `gt`, as the timestamp only has second precision
  query+="&_lastUpdated=ge${last_download_started_at}"
elif [[ "${status}" != 404 ]]; then
  echo "Failed to read the checkpoint, HTTP status ${status}" >&2
  exit 1
fi
# taken before downloading, so resources changing during the download are downloaded again next time
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# only lists the resource types with at least one resource
resource_types="$(blazectl count-resources --server "${server}" | awk '$2 == ":" && $1 != "total" { print $1 }')"

# `pathling convert` can't be used for this, as it mangles s3a:// paths
merge_batch() {
  echo "Merging a batch of ${batch_count} resources into the tables"
  # the files of resource types without changes
  find /data/ndjson -type f -empty -delete
  pathling --config /etc/pathling/pathling.toml run \
    -c "pc.read.ndjson('/data/ndjson').write.delta('s3a://fhir/warehouse', save_mode='merge')"
  rm -rf /data/ndjson
  mkdir /data/ndjson
  batch_count=0
}

rm -rf /data/ndjson
mkdir /data/ndjson
batch_count=0
for resource_type in ${resource_types}; do
  echo "Downloading the ${resource_type} resources"
  # follows the next links Blaze returns, which stay valid for 4 hours without a request, so a batch
  # can be merged in between
  url="${server}/${resource_type}?${query}"
  while [[ -n "${url}" ]]; do
    curl -sSf --retry 3 -o /tmp/page.json "${url}"
    jq -c '.entry[]? | select(.search.mode == "match") | .resource' /tmp/page.json \
      >>"/data/ndjson/${resource_type}.ndjson"
    batch_count=$((batch_count + $(jq '[.entry[]? | select(.search.mode == "match")] | length' /tmp/page.json)))
    url="$(jq -r '.link[]? | select(.relation == "next") | .url' /tmp/page.json)"
    if ((batch_count >= BATCH_SIZE)); then
      merge_batch
    fi
  done
done
if ((batch_count > 0)); then
  merge_batch
fi

# only written once all batches are merged, so a failed run starts over from the previous checkpoint
jq -n --arg started_at "${started_at}" '{lastDownloadStartedAt: $started_at}' |
  "${s3_curl[@]}" -f -X PUT -H "Content-Type: application/json" --data-binary @- "${checkpoint_url}"
