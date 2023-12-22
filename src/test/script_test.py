from test.data.repair_order_event import DataROE
from test.data.clean_repair_order import DataCRO
from test.data.ranked_repair_order import DataRRO
from test.data.latest_repair_order import DataLRO
from lib.sql.sql_file_reader import SqlFileReader


def test_clean_repair_order(spark_session):
    # Arrange
    repair_order_event = DataROE.repair_order_event(spark_session=spark_session)
    repair_order_event.createTempView("repair_order_event")
    clean_repair_order = DataCRO.clean_repair_order(spark_session=spark_session)

    # Act
    query = SqlFileReader().get_sql_query(file_name="clean_repair_order.sql")
    actual_data_frame = spark_session.sql(sqlQuery=query)
    actual_count = clean_repair_order.intersect(actual_data_frame).count()

    # Assert
    assert 12 == actual_count

def test_ranked_repair_order_by_order_id(spark_session):
    # Arrange
    clean_repair_order = DataCRO.clean_repair_order(spark_session=spark_session)
    clean_repair_order.createTempView("clean_repair_order")
    ranked_repair_order = DataRRO.ranked_repair_order_by_order_id(spark_session=spark_session)

    # Act
    query = SqlFileReader().get_sql_query(
        file_name="ranked_repair_order.sql", 
        substitutions={"PARTITION_BY": "order_id", "ORDER_BY": "date_time"}
    )
    actual_data_frame = spark_session.sql(sqlQuery=query)
    actual_count = ranked_repair_order.intersect(actual_data_frame).count()

    # Assert
    assert 12 == actual_count

def test_latest_repair_order_by_order_id(spark_session):
    # Arrange
    ranked_repair_order = DataRRO.ranked_repair_order_by_order_id(spark_session=spark_session)
    ranked_repair_order.createTempView("ranked_repair_order")
    latest_repair_order = DataLRO.ranked_repair_order_by_order_id(spark_session=spark_session)

    # Act
    query = SqlFileReader().get_sql_query(
        file_name="latest_repair_order.sql"
    )
    actual_data_frame = spark_session.sql(sqlQuery=query)

    actual_count = latest_repair_order.intersect(actual_data_frame).count()

    # Assert
    assert 4 == actual_count
