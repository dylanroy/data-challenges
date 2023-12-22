import datetime

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class DataLRO:
    @staticmethod
    def ranked_repair_order_by_order_id(spark_session):
        schema = StructType(fields=[
            StructField('cost',             dataType=DoubleType(),      nullable=True),
            StructField('date_time',        dataType=TimestampType(),   nullable=True),
            StructField('order_id',         dataType=IntegerType(),     nullable=True),
            StructField('technician',       dataType=StringType(),      nullable=True),
            StructField('part_name',        dataType=StringType(),      nullable=True),
            StructField('part_quantity',    dataType=IntegerType(),     nullable=True),
            StructField('status',           dataType=StringType(),      nullable=True)
        ])
        return spark_session.createDataFrame(data = [
            Row(cost=65.0, date_time=datetime.datetime(2023, 8, 10, 18, 0), order_id=101, technician='Jane Smith', part_name='Oil Filter', part_quantity=1, status='Reopened'),
            Row(cost=85.0, date_time=datetime.datetime(2023, 8, 11, 9, 0), order_id=102, technician='James Brown', part_name='Air Filter', part_quantity=1, status='In Progress'),
            Row(cost=120.0, date_time=datetime.datetime(2023, 8, 10, 16, 0), order_id=103, technician='Mary Johnson', part_name='Air Filter', part_quantity=1, status='Completed'),
            Row(cost=110.0, date_time=datetime.datetime(2023, 8, 11, 12, 0), order_id=104, technician='Robert White', part_name='Brake Fluid', part_quantity=1, status='Completed')
        ], schema=schema)
