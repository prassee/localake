from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (
        SparkSession.builder
        .appName("localake-sample-job")
        .getOrCreate()
    )

    data = [(1, "alpha"), (2, "beta"), (3, "gamma")]
    df = spark.createDataFrame(data, ["id", "value"])
    df.show()

    spark.stop()
