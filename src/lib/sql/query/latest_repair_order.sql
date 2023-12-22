select
    cost,
    date_time,
    order_id,
    repair_details.technician,
    repair_details.repair_parts.part._name as part_name,
    repair_details.repair_parts.part._quantity as part_quantity,
    status
from ranked_repair_order
where rank = 1
