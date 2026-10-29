CREATE SECRET seaweedfs_secret  (
     TYPE S3,
     KEY_ID 'admin',
     SECRET 'seaweedpass',
     REGION 'us-east-1',
     ENDPOINT 'localhost:8333',
     USE_SSL false,
     URL_STYLE 'path'
);

SELECT COUNT(*)
FROM delta_scan('s3://fhir/warehouse/Patient.parquet');

SELECT COUNT(*)
FROM delta_scan('s3://fhir/warehouse/Condition.parquet');
