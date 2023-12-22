import datetime

from pyspark.sql.types import Row

from src.lib.tables.repair_order import RepairOrder


class DataROE:
    @staticmethod
    def repair_order_event(spark_session):
        schema = RepairOrder().schema
        return spark_session.createDataFrame(data=[
            Row(_corrupt_record=None, order_id=102, date_time=datetime.datetime(2023, 8, 11, 9, 0), status='In Progress', cost=85.0, repair_details=Row(technician='James Brown', repair_parts=Row(part=Row(_name='Air Filter', _quantity=1)))),
            Row(_corrupt_record=None, order_id=104, date_time=datetime.datetime(2023, 8, 11, 10, 0), status='In Progress', cost=45.0, repair_details=Row(technician='Robert White', repair_parts=Row(part=Row(_name='Tire', _quantity=2)))),
            Row(_corrupt_record='<event>\n    <order_id>104</order_id>\n    <date_time>2023-08-11T08:00:00</date_time>\n    <status>Received</status>\n    <cost>40.00</cost>\n    <repair_details>\n        <technician>Robert White</technician>\n        <repair_parts>\n            <part name="Tire" quantity="2"/>\n        </pair_parts>\n    </repair_details>\n</event>', order_id=None, date_time=None, status=None, cost=None, repair_details=None),
            Row(_corrupt_record=None, order_id=101, date_time=datetime.datetime(2023, 8, 10, 18, 0), status='Reopened', cost=65.0, repair_details=Row(technician='Jane Smith', repair_parts=Row(part=Row(_name='Oil Filter', _quantity=1)))),
            Row(_corrupt_record=None, order_id=103, date_time=datetime.datetime(2023, 8, 10, 16, 0), status='Completed', cost=120.0, repair_details=Row(technician='Mary Johnson', repair_parts=Row(part=Row(_name='Air Filter', _quantity=1)))),
            Row(_corrupt_record=None, order_id=103, date_time=datetime.datetime(2023, 8, 10, 10, 0), status='Received', cost=90.0, repair_details=Row(technician='Mary Johnson', repair_parts=Row(part=Row(_name='Brake Pad', _quantity=2)))),
            Row(_corrupt_record=None, order_id=101, date_time=datetime.datetime(2023, 8, 10, 12, 30), status='Completed', cost=60.0, repair_details=Row(technician='Jane Smith', repair_parts=Row(part=Row(_name='Oil Filter', _quantity=1)))),
            Row(_corrupt_record=None, order_id=104, date_time=datetime.datetime(2023, 8, 11, 12, 0), status='Completed', cost=110.0, repair_details=Row(technician='Robert White', repair_parts=Row(part=Row(_name='Brake Fluid', _quantity=1)))),
            Row(_corrupt_record=None, order_id=101, date_time=datetime.datetime(2023, 8, 10, 8, 0), status='Received', cost=40.0, repair_details=Row(technician='Jane Smith', repair_parts=Row(part=Row(_name='Air Filter', _quantity=1)))),
            Row(_corrupt_record=None, order_id=102, date_time=datetime.datetime(2023, 8, 10, 13, 0), status='Completed', cost=80.0, repair_details=Row(technician='James Brown', repair_parts=Row(part=Row(_name='Fuel Filter', _quantity=1)))),
            Row(_corrupt_record=None, order_id=101, date_time=datetime.datetime(2023, 8, 10, 10, 0), status='In Progress', cost=50.25, repair_details=Row(technician='Jane Smith', repair_parts=Row(part=Row(_name='Air Filter', _quantity=1)))),
            Row(_corrupt_record=None, order_id=102, date_time=datetime.datetime(2023, 8, 10, 12, 0), status='Completed', cost=75.0, repair_details=Row(technician='James Brown', repair_parts=Row(part=Row(_name='Oil Filter', _quantity=1)))),
            Row(_corrupt_record=None, order_id=103, date_time=datetime.datetime(2023, 8, 10, 14, 0), status='In Progress', cost=100.5, repair_details=Row(technician='Mary Johnson', repair_parts=Row(part=Row(_name='Brake Pad', _quantity=2))))
        ],schema=schema)
    