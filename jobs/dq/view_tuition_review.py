
from jobs.silver.clean_admissions import spark_session
from pyspark.sql import functions as F

spark = spark_session()
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.format("delta").load(
        "s3a://silver/education/admissions"
    )

    review = df.filter(F.col("tuition_status") == "REVIEW")

    print("\n=== SO BAN GHI HOC PHI CAN REVIEW ===")
    print(review.count())

    print("\n=== CHI TIET CAC BAN GHI ===")
    review.select(
        "admission_year",
        "source_major_code",
        "tuition_fee_raw",
        "tuition_min",
        "tuition_max",
        "tuition_unit",
        "tuition_status"
    ).orderBy(
        "admission_year", "source_major_code"
    ).show(200, truncate=False)

    print("\n=== CAC GIA TRI HOC PHI GOC KHAC NHAU ===")
    review.groupBy("tuition_fee_raw").count().orderBy(
        F.desc("count")
    ).show(200, truncate=False)

finally:
    spark.stop()
