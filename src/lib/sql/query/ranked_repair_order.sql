select
    cost,
    date_time,
    order_id,
    repair_details,
    status,
    row_number() over (partition by {partition_by} order by {order_by} desc) as rank
from clean_repair_order
