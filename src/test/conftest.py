# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pytest import fixture
import logging


@fixture(scope="session")
def spark_context(request):
    spark_conf = (
        SparkConf()
        .setMaster("local[2]")
        .setAppName("pytest-pyspark-local-testing")
        .set(
            "spark.jars", "/home/glue_user/workspace/src/jars/spark-xml_2.12-0.17.0.jar"
        )
    )
    spark_context = SparkContext(conf=spark_conf)
    request.addfinalizer(lambda: spark_context.stop())
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)
    return spark_context


@fixture(scope="function")
def spark_session(spark_context):
    return SparkSession(sparkContext=spark_context).newSession()
