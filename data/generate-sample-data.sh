#!/bin/bash

SYNTHEA_JAR="synthea-with-dependencies.jar"
SYNTHEA_URL="https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/$SYNTHEA_JAR"

# Check if the file already exists
if [[ ! -f "$SYNTHEA_JAR" ]]; then
  echo "File $SYNTHEA_JAR not found. Downloading..."
  curl -LO "$SYNTHEA_URL"
  echo "Download completed."
else
  echo "File $SYNTHEA_JAR already exists. Skipping download."
fi

java -jar synthea-with-dependencies.jar -p "10" --exporter.baseDirectory="./synthea"

OUTPUT_FILE="bundles.ndjson"

echo "" >"$OUTPUT_FILE"

for file in "./synthea/fhir"/*.json; do
  # Ensure the file is a valid JSON file
  if [[ -f "$file" ]]; then
    # Convert the JSON file into NDJSON format (assuming it's a JSON array)
    jq -c '.' "$file" >>"$OUTPUT_FILE"
  fi
done

# TODO: need to path the transactions entry.request.method to PUT and .url to resourceType/id
