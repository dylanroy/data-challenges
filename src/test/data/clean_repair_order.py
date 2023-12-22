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


class DataCRO:
    @staticmethod
    def clean_repair_order(spark_session):
        schema = StructType(fields=[
            StructField(name="cost",                dataType=DoubleType()),
            StructField(name="date_time",           dataType=TimestampType()),
            StructField(name="order_id",            dataType=IntegerType()),
            StructField(name="repair_details",
                dataType=StructType(fields=[
                    StructField(name="technician",  dataType=StringType()),
                    StructField(name="repair_parts",
                        dataType=StructType(fields=[
                            StructField(name="part",
                                dataType=StructType(fields=[
                                    StructField(name="_name",   dataType=StringType(), nullable=False, metadata={"xmlname": "name"}),
                                    StructField(name="_quantity", dataType=IntegerType(), nullable=False, metadata={"xmlname": "quantity"}),
                    ]))
                ]))
            ])),
            StructField(name="status",              dataType=StringType())
        ])
        return spark_session.createDataFrame(data = [
            Row(cost=85.0, date_time=datetime.datetime(2023, 8, 11, 9, 0), order_id=102, repair_details=Row(technician='James Brown', repair_parts=Row(part=Row(_name='Air Filter', _quantity=1))), status='In Progress'),
            Row(cost=45.0, date_time=datetime.datetime(2023, 8, 11, 10, 0), order_id=104, repair_details=Row(technician='Robert White', repair_parts=Row(part=Row(_name='Tire', _quantity=2))), status='In Progress'),
            Row(cost=65.0, date_time=datetime.datetime(2023, 8, 10, 18, 0), order_id=101, repair_details=Row(technician='Jane Smith', repair_parts=Row(part=Row(_name='Oil Filter', _quantity=1))), status='Reopened'),
            Row(cost=120.0, date_time=datetime.datetime(2023, 8, 10, 16, 0), order_id=103, repair_details=Row(technician='Mary Johnson', repair_parts=Row(part=Row(_name='Air Filter', _quantity=1))), status='Completed'),
            Row(cost=90.0, date_time=datetime.datetime(2023, 8, 10, 10, 0), order_id=103, repair_details=Row(technician='Mary Johnson', repair_parts=Row(part=Row(_name='Brake Pad', _quantity=2))), status='Received'),
            Row(cost=60.0, date_time=datetime.datetime(2023, 8, 10, 12, 30), order_id=101, repair_details=Row(technician='Jane Smith', repair_parts=Row(part=Row(_name='Oil Filter', _quantity=1))), status='Completed'),
            Row(cost=110.0, date_time=datetime.datetime(2023, 8, 11, 12, 0), order_id=104, repair_details=Row(technician='Robert White', repair_parts=Row(part=Row(_name='Brake Fluid', _quantity=1))), status='Completed'),
            Row(cost=40.0, date_time=datetime.datetime(2023, 8, 10, 8, 0), order_id=101, repair_details=Row(technician='Jane Smith', repair_parts=Row(part=Row(_name='Air Filter', _quantity=1))), status='Received'),
            Row(cost=80.0, date_time=datetime.datetime(2023, 8, 10, 13, 0), order_id=102, repair_details=Row(technician='James Brown', repair_parts=Row(part=Row(_name='Fuel Filter', _quantity=1))), status='Completed'),
            Row(cost=50.25, date_time=datetime.datetime(2023, 8, 10, 10, 0), order_id=101, repair_details=Row(technician='Jane Smith', repair_parts=Row(part=Row(_name='Air Filter', _quantity=1))), status='In Progress'),
            Row(cost=75.0, date_time=datetime.datetime(2023, 8, 10, 12, 0), order_id=102, repair_details=Row(technician='James Brown', repair_parts=Row(part=Row(_name='Oil Filter', _quantity=1))), status='Completed'),
            Row(cost=100.5, date_time=datetime.datetime(2023, 8, 10, 14, 0), order_id=103, repair_details=Row(technician='Mary Johnson', repair_parts=Row(part=Row(_name='Brake Pad', _quantity=2))), status='In Progress')
        ], schema=schema)
