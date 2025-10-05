import findspark
findspark.init()

from pyspark.sql import SparkSession
from resources.dev import config
from src.main.utility.logging_config import logger

def spark_session():
    spark = (
        SparkSession.builder
            .master("local[*]")
            .appName("jeevan_spark")

            # 1) Ship your local MySQL jar
            .config("spark.jars", "C:\\my_sql_jar\\mysql-connector-j-9.3.0.jar")

            # 2) Pull in Hadoop AWS + AWS SDK from Maven
            .config(
              "spark.jars.packages",
              ",".join([
                "org.apache.hadoop:hadoop-aws:3.3.1",
                "com.amazonaws:aws-java-sdk-bundle:1.12.406"
              ])
            )

            # 3) Point to your Hadoop home (for winutils.exe on Windows)
            .config("spark.hadoop.home.dir", "C:\\hadoop")

            # 4) Enable S3A
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key",    config.aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key",    config.aws_secret_key)
            # Optional: region or path‑style if needed
            .config("spark.hadoop.fs.s3a.endpoint",      "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")

            # 5) Keep your other committer/workaround configs
            .config("spark.hadoop.hadoop.native.lib", "false")
            .config("spark.hadoop.hadoop.nativeio.nativeio", "false")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config(
              "spark.sql.sources.commitProtocolClass",
              "org.apache.spark.internal.io.HadoopMapReduceCommitProtocol"
            )
            .getOrCreate()
    )

    logger.info("Spark session initialized: %s", spark)
    return spark
