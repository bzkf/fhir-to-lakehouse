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

java -jar synthea-with-dependencies.jar -p "25000" -c synthea.properties

OUTPUT_FILE="bundles.ndjson"

echo "" >"$OUTPUT_FILE"

for file in "./synthea/fhir"/*.json; do
  if [[ -f "$file" ]]; then
    # Synthea always generates Patient & Encounter resources, but we only care about Patients
    jq -c '.entry |= map(
      select(.resource.resourceType == "Patient") |
      .request.method = "PUT" |
      .request.url = "\(.resource.resourceType)/\(.resource.id | sub("^urn:uuid:"; ""))"
    )' "$file" >>"$OUTPUT_FILE"
  fi
done
