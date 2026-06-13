SELECT
    DATE_FORMAT(
        order_purchase_timestamp,
        '%Y-%m'
    ) AS month,

    SUM(
        oi.price + oi.freight_value
    ) AS monthly_revenue

FROM orders o

JOIN order_items oi
    ON o.order_id = oi.order_id

GROUP BY month

ORDER BY month;