import logging
import time
from platform import python_version
from pprint import pformat
from sys import stdout

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
stdout_handler = logging.StreamHandler(stream=stdout)
formatter = logging.Formatter(
    fmt="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
formatter.converter = time.gmtime
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)
logger.setLevel(logging.INFO)


class Logger:
    def __init__(self) -> None:
        pass

    def info(self, message: str) -> None:
        logger.info(pformat(message, indent=2))

    def raw_info(self, message: str) -> None:
        logger.info(message)

    def start(self, message: str) -> None:
        logger.info(f"STARTING: {message}")

    def finish(self, message: str) -> None:
        logger.info(f"FINISHED: {message}")

    def error(self, message: str) -> None:
        logger.info(f"ERROR: {message}")

    def job_info(
        self, spark: SparkSession, known_arguments: dict, unknown_arguments: dict
    ) -> None:
        self.start("JOB INFO")
        py_version = python_version()
        self.info(f"Python Version: {py_version}")
        self.info(f"Spark Version:  {spark.version}")
        hadoop_version = (
            spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        )
        self.info(f"Hadoop Version: {hadoop_version}")
        java_version = spark.sparkContext._jvm.java.lang.System.getProperty(
            "java.runtime.version"
        )
        self.info(f"Java Version:   {java_version}")
        java_vendor = spark.sparkContext._jvm.java.lang.System.getProperty(
            "java.vendor"
        )
        self.info(f"Java Vendor:    {java_vendor}")
        configuration = spark.sparkContext.getConf().getAll()
        ascii_configuration = {}
        for unicode_key, unicode_value in configuration:
            ascii_key = unicode_key.encode("ascii", "ignore")
            ascii_value = unicode_value.encode("ascii", "ignore")
            ascii_configuration[ascii_key] = ascii_value
        formatted_configuration = pformat(ascii_configuration, indent=2)
        self.info(f"Spark Configuration:\n{formatted_configuration}")
        known_arguments = pformat(known_arguments, indent=2)
        unknown_arguments = ", ".join(unknown_arguments)
        self.info(
            "Known Arguments:\n{known_arguments}".format(
                known_arguments=known_arguments
            )
        )
        self.info(
            "Unknown Arguments:\n{unknown_arguments}".format(
                unknown_arguments=unknown_arguments
            )
        )
        self.finish("JOB INFO")
