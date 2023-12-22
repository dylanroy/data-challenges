select cost, date_time, order_id, repair_details, status
from repair_order_event
where _corrupt_record is null
