SELECT
    cost,
    date_time,
    order_id,
    repair_details,
    status,
    ROW_NUMBER() OVER (
        PARTITION BY {PARTITION_BY}
        ORDER BY {ORDER_BY} DESC
    ) AS rank
FROM
    clean_repair_order