SELECT
    DATE(order_purchase_timestamp) AS order_date,
    SUM(price + freight_value) AS daily_revenue
FROM orders o
JOIN order_items oi
    ON o.order_id = oi.order_id
GROUP BY DATE(order_purchase_timestamp)
ORDER BY order_date;