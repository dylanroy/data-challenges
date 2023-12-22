from pyspark.sql import SparkSession

from lib.sql.sql_file_reader import SqlFileReader
from lib.tables.repair_order import RepairOrder
from lib.utils.logger import Logger


class DynatronSoftware(object):
    logger = Logger()
    logger.start("JOB")
    sql_file_reader = SqlFileReader()

    # Job instantiation
    logger.start("CREATE SPARK SESSION")
    spark = SparkSession.builder.getOrCreate()
    logger.finish("CREATE SPARK SESSION")
    logger.job_info(spark=spark)

    # Extract
    logger.start(message="Extract")
    repair_orders = RepairOrder()
    raw_df = (
        spark.read.format("com.databricks.spark.xml")
        .option("rowTag", "event")
        .schema(repair_orders.schema)
        .load("/root/data/")
    )
    raw_df.show()
    logger.info(message=raw_df.where("_corrupt_record is not null").collect())
    raw_df.createTempView("repair_order_event")
    logger.finish(message="Extract")

    # Transform
    logger.start(message="TRANSFORM")
    queries = [
        {"file_name": "clean_repair_order", "substitutions": None},
        {
            "file_name": "ranked_repair_order",
            "substitutions": {"partition_by": "order_id", "order_by": "date_time"},
        },
        {"file_name": "latest_repair_order", "substitutions": None},
    ]

    dfs = []
    for query_info in queries:
        file_name = query_info["file_name"]
        substitutions = query_info["substitutions"]

        query = sql_file_reader.get_sql_query(
            file_name=f"{file_name}.sql", substitutions=substitutions
        )

        df = spark.sql(query)
        df.createTempView(file_name)
        dfs.append(df)

    logger.finish(message="TRANSFORM")

    # Load
    logger.start("LOAD")
    jdbc_url = "jdbc:sqlite:/db/dynatron.db"
    logger.info(f"CONNECTION_URL: {jdbc_url}")
    dfs[-1].write.format("jdbc").options(driver="org.sqlite.JDBC").option(
        "url", jdbc_url
    ).option("dbtable", "repair_orders").mode("overwrite").save()
    logger.finish("LOAD")
    logger.finish(message="JOB")


if __name__ == "__main__":
    logger = Logger()
    DynatronSoftware()
