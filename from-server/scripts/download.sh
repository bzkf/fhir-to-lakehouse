#!/bin/bash
# Downloads the resources changed since the last download to /data/ndjson, as one
# <ResourceType>.ndjson file per type. The first run downloads all resources.
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

rm -rf /data/ndjson
mkdir /data/ndjson
for resource_type in ${resource_types}; do
  blazectl download --no-progress --server "${server}" "${resource_type}" --query "${query}" \
    --output-file "/data/ndjson/${resource_type}.ndjson"
done
# the files of resource types without changes
find /data/ndjson -type f -empty -delete

jq -n --arg started_at "${started_at}" '{lastDownloadStartedAt: $started_at}' |
  "${s3_curl[@]}" -f -X PUT -H "Content-Type: application/json" --data-binary @- "${checkpoint_url}"
