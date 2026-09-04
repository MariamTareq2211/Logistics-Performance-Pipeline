/*-----------------------------------------Delivery Reliability-------------------------*/

/* ON-Time Delivary Rate */
SELECT 
  ROUND(100.0 * SUM(CASE WHEN is_late_delivery = 0 THEN 1 ELSE 0 END) / COUNT(*),2) AS on_time_rate_pct,
  ROUND(100.0 * SUM(CASE WHEN is_late_delivery = 1 THEN 1 ELSE 0 END) / COUNT(*),2) AS late_rate_pct,
  COUNT(*) AS total_orders
FROM fact_logistics;

/* Median Delivery Delta */
/* (delivery_time_delta_days if negative = early else (positive) = late) */
SELECT ROUND(AVG(delivery_time_delta_days) , 2) AS avg_delta_days,
       PERCENTILE(delivery_time_delta_days , 0.5)AS median_delta_days,
       MIN(delivery_time_delta_days) AS best_case_days,
       MAX(delivery_time_delta_days) AS worst_case_days
FROM fact_logistics;

/*------------------------------Speed / Lead Time (Bottleneck Analysis)------------------------------*/

/* Average Order-to-Delivery Lead Time */
SELECT
    ROUND(AVG(order_to_delivery_days), 2) AS avg_lead_time_days,
    PERCENTILE(order_to_delivery_days, 0.5) AS median_lead_time_days
FROM fact_logistics;

/*Approval-to-Ship Time*/
SELECT
    ROUND(AVG(approval_to_ship_days), 2) AS avg_approval_to_ship_days,
    ROUND(AVG(order_to_delivery_days), 2) AS avg_total_lead_time_days,
    ROUND(100.0 * AVG(approval_to_ship_days) / NULLIF(AVG(order_to_delivery_days), 0), 2) AS pct_of_lead_time_is_prep
FROM fact_logistics;

/*Does Order Size Slow Things Down?*/
SELECT items_count, COUNT(*) AS order_count,
    ROUND(AVG(delivery_time_delta_days), 2) AS avg_delta_days,
    ROUND(100.0 * SUM(is_late_delivery) / COUNT(*), 2) AS late_rate_pct
FROM fact_logistics
GROUP BY items_count
ORDER BY items_count;

/*------------------------------Cost Efficiency---------------------------------------*/
/*Shipping Cost as a Share of Order Value*/
SELECT ROUND(SUM(shipping_cost), 2) AS total_shipping_cost,
    ROUND(SUM(total_order_price), 2) AS total_product_value,
    ROUND(100.0 * SUM(shipping_cost) / NULLIF(SUM(total_order_price), 0), 2) AS freight_to_price_ratio_pct
FROM fact_logistics;


/*-------------------------------Geographic Performance------------------------------*/
/*On-Time Rate by Customer State (Destination)*/
SELECT c.customer_state, COUNT(*) AS order_count,
    ROUND(AVG(f.delivery_time_delta_days), 2) AS avg_delta_days,
    ROUND(100.0 * SUM(f.is_late_delivery) / COUNT(*), 2) AS late_rate_pct
FROM fact_logistics f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY late_rate_pct DESC;

/*On-Time Rate by Seller State (Origin)*/
SELECT s.seller_state, COUNT(*) AS order_count,
    ROUND(AVG(f.delivery_time_delta_days), 2) AS avg_delta_days,
    ROUND(100.0 * SUM(f.is_late_delivery) / COUNT(*), 2) AS late_rate_pct
FROM fact_logistics f
JOIN dim_seller s ON f.seller_id = s.seller_id
GROUP BY s.seller_state
ORDER BY late_rate_pct DESC;

/*Cross-State Shipping (Interstate Penalty)*/
SELECT CASE WHEN s.seller_state = c.customer_state THEN 'Same State' ELSE 'Cross-State' END AS shipping_type,
    COUNT(*) AS order_count,
    ROUND(AVG(f.delivery_time_delta_days), 2) AS avg_delta_days,
    ROUND(AVG(f.shipping_cost), 2) AS avg_shipping_cost,
    ROUND(100.0 * SUM(f.is_late_delivery) / COUNT(*), 2) AS late_rate_pct
FROM fact_logistics f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_seller s ON f.seller_id = s.seller_id
GROUP BY CASE WHEN s.seller_state = c.customer_state THEN 'Same State' ELSE 'Cross-State' END;


/*---------------------------------Time Trends--------------------------------*/
/*On-Time Rate by Month*/
SELECT d.year, d.month, d.month_name, COUNT(*) AS order_count,
    ROUND(100.0 * SUM(f.is_late_delivery) / COUNT(*), 2) AS late_rate_pct,
    ROUND(AVG(f.delivery_time_delta_days), 2) AS avg_delta_days
FROM fact_logistics f
JOIN dim_date d ON f.order_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

/*Weekday vs. Weekend Order Placement Effect*/
SELECT d.is_weekend, COUNT(*) AS order_count,
    ROUND(AVG(f.approval_to_ship_days), 2) AS avg_approval_to_ship_days,
    ROUND(100.0 * SUM(f.is_late_delivery) / COUNT(*), 2) AS late_rate_pct
FROM fact_logistics f
JOIN dim_date d ON f.order_date_key = d.date_key
GROUP BY d.is_weekend;
