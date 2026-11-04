# An example query of the Delta Lake tables: the most common conditions by number of patients.
# Run via `pathling run`, which provides the Pathling context as `pc`.
from pyspark.sql import functions as F

conditions = pc.read.delta("s3a://fhir/warehouse").view(  # noqa: F821
    "Condition",
    select=[
        {
            "column": [
                {"path": "code.coding.first().display", "name": "condition"},
                {"path": "subject.reference", "name": "patient"},
            ]
        }
    ],
)

conditions.groupBy("condition").agg(F.countDistinct("patient").alias("patients")).orderBy(
    F.desc("patients")
).show(10, truncate=False)
