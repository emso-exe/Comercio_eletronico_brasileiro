 -- VIEW vw_order_costumer

-- DROP VIEW IF EXISTS ecommerce.vw_order_costumer;
CREATE OR REPLACE VIEW ecommerce.vw_order_costumer AS 
    SELECT 
        oo.customer_id,
        oo.order_id,
        cc.customer_unique_id,
        cc.customer_city,
        cc.customer_state        
    FROM ecommerce.tb_orders oo
    INNER JOIN ecommerce.tb_customers cc 
    ON cc.customer_id = oo.customer_id
    
    
-- VIEW vw_price_freight

-- DROP VIEW IF EXISTS ecommerce.vw_price_freight;
CREATE OR REPLACE VIEW ecommerce.vw_price_freight AS 
    SELECT
        cc.customer_city,
        cc.customer_state,
        oo.order_id,
        oo.order_purchase_timestamp,
        oi.price,
        oi.freight_value
    FROM ecommerce.tb_orders oo
    INNER JOIN ecommerce.tb_customers cc 
    ON cc.customer_id = oo.customer_id
    INNER JOIN ecommerce.tb_order_items oi 
    ON oi.order_id = oo.order_id 
    
    
-- 01 - Qual o total(quantidade) de vendas divididas por estado?

SELECT 
    customer_state, 
    COUNT(order_id) amount_of_sales
    FROM ecommerce.vw_order_costumer 
        GROUP BY 
            customer_state 
        ORDER BY 
            amount_of_sales DESC;


-- 02 - Qual o total(valor) de vendas e fretes divididos por estado?

SELECT
    customer_state,
    to_char(SUM (price), 'L9G999G999D99') AS total_price,
    to_char(SUM (freight_value), 'L9G999G999D99') AS total_freight
    FROM ecommerce.vw_price_freight
        GROUP BY 
            customer_state
        ORDER BY 
            total_price DESC;


-- 03 - Qual o total(quantidade) e distribuição(%) de vendas por hora?
            
SELECT
    to_char(order_purchase_timestamp, 'HH24') AS hour_24,
    COUNT(order_id) AS amount_per_hour,
    CAST(COUNT(*) * 100 / SUM(COUNT(*)) OVER() AS DECIMAL(10,2)) AS perc_score
    FROM ecommerce.tb_orders
        GROUP BY 
            hour_24
        ORDER BY 
            hour_24;


-- 04 - Qual a média(valor) de vendas por hora?

SELECT 
    hour_24,
    to_char(AVG (sum_total), 'L9G999G999D99') AS avg_total
    FROM (
            SELECT 
                oo.order_id, 
                to_char(order_purchase_timestamp, 'HH24') AS hour_24, 
                SUM (price) AS sum_total
                FROM ecommerce.tb_orders oo
                INNER JOIN ecommerce.tb_order_items oi 
                ON oi.order_id = oo.order_id 
                    GROUP BY 
                        oo.order_id
                    ORDER BY 
                        hour_24
         ) AS total_amount_per_order
        GROUP BY 
            hour_24
        ORDER BY 
            hour_24;


-- 05 - Qual o ticket médio nos anos de 2016, 2017 e 2018?

SELECT 
    ext_year,
    to_char(AVG (sum_total), 'L9G999G999D99') avg_ticket
    FROM (
            SELECT 
                oo.order_id,
                EXTRACT (YEAR FROM order_purchase_timestamp) AS ext_year, 
                SUM (price) AS sum_total
                FROM ecommerce.tb_orders oo
                INNER JOIN ecommerce.tb_order_items oi 
                ON oi.order_id = oo.order_id 
                    GROUP BY 
                        oo.order_id
                    ORDER BY 
                        ext_year
         ) AS total_amount_per_order
        GROUP BY 
            ext_year
        ORDER BY 
            ext_year;


-- 06 - Qual a distribuição(%) da pontuação do pedidos?

SELECT 
    review_score,
    COUNT(*) AS qty_score,
    CAST(COUNT(*) * 100 / SUM(COUNT(*)) OVER() AS DECIMAL(10,2)) AS perc_score
    FROM ecommerce.tb_order_reviews
        GROUP BY
            review_score
        ORDER BY
            review_score DESC;


-- 07 - Quais as 10 cidades com as maiores volumes(quantidade) de vendas?

SELECT 
    customer_city, 
    customer_state, 
    COUNT(*) AS top_10
    FROM ecommerce.vw_order_costumer 
        GROUP BY 
            customer_city, 
            customer_state 
        ORDER BY 
            top_10 DESC
        LIMIT 10;


-- 08 - Quais as 10 cidades com os maiores volumes(valores) de vendas e fretes?

SELECT 
    customer_city, 
    customer_state, 
    to_char(SUM (price), 'L9G999G999D99') total_price,
    to_char(SUM (freight_value), 'L9G999G999D99') total_freight
    FROM ecommerce.vw_price_freight 
        GROUP BY 
            customer_city, 
            customer_state 
        ORDER BY 
            total_price DESC
        LIMIT 10;
        

-- 09 - Qual a quantidade de produtos cadastrados por categoria?

SELECT 
    product_category_name,
    COUNT(product_id) AS qty_product
    FROM ecommerce.tb_products
        GROUP BY
            product_category_name
        ORDER BY
            qty_product DESC;   
            
            
-- 10 - Qual a quantidade e distribuição(%) das categorias nos pedidos?

SELECT
    product_category_name,
    COUNT(oi.product_id) AS qty_product,
    CAST(COUNT(oi.product_id) * 100 / SUM(COUNT(oi.product_id)) OVER() AS DECIMAL(10,2)) AS perc_category
    FROM ecommerce.tb_products po
    INNER JOIN ecommerce.tb_order_items oi 
    ON po.product_id = oi.product_id
        GROUP BY
            product_category_name
        ORDER BY
            perc_category DESC; 
   
   
-- 11 - Qual a quantidade de vendas por vendedor?

SELECT
    se.seller_id,
    COUNT(DISTINCT oi.order_id) AS qty_order_by_seller
    FROM ecommerce.tb_sellers se
    INNER JOIN ecommerce.tb_order_items oi 
    ON se.seller_id = oi.seller_id
        GROUP BY 
            se.seller_id
        ORDER BY
            qty_order_by_seller DESC;