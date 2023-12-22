SELECT
    cost,
    date_time,
    order_id,
    repair_details,
    status
FROM
    repair_order_event
WHERE _corrupt_record IS NULL