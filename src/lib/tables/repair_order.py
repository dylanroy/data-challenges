from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class RepairOrder:
    @property
    def name(self):
        return "repair_order"

    @property
    def schema(self):
        return StructType(fields=[
            StructField(name="_corrupt_record",     dataType=StringType()),
            StructField(name="order_id",            dataType=IntegerType()),
            StructField(name="date_time",           dataType=TimestampType()),
            StructField(name="status",              dataType=StringType()),
            StructField(name="cost",                dataType=DoubleType()),
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
            ]))
        ])
                