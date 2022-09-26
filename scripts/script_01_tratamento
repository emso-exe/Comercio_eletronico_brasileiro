-- Criação das querys para visualização dos dados das tabelas criadas no batabase Olist


SELECT customer_id, customer_unique_id, 
       customer_zip_code_prefix, 
       customer_city, 
       customer_state
	FROM ecommerce.tb_customers;

SELECT geolocation_zip_code_prefix, 
       geolocation_lat, geolocation_lng, 
       geolocation_city, 
       geolocation_state
	FROM ecommerce.tb_geolocation;
       
SELECT order_id, order_item_id, product_id, seller_id, 
       shipping_limit_date, 
       price, freight_value
	FROM ecommerce.tb_order_items;    
    
SELECT order_id, 
       payment_sequential, 
       payment_type, 
       payment_installments, 
       payment_value
	FROM ecommerce.tb_order_payments;    
    
SELECT review_id, order_id, 
       review_score, 
       review_comment_title, review_comment_message, 
       review_creation_date, review_answer_timestamp
	FROM ecommerce.tb_order_reviews;
    
SELECT order_id, customer_id, 
       order_status, 
       order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, 
       order_delivered_customer_date, order_estimated_delivery_date
	FROM ecommerce.tb_orders;

SELECT product_id, 
       product_category_name, 
       product_name_lenght, product_description_lenght, 
       product_photos_qty, 
       product_weight_g, product_length_cm, product_height_cm, product_width_cm
	FROM ecommerce.tb_products;
    
SELECT seller_id, 
       seller_zip_code_prefix, 
       seller_city, 
       seller_state
	FROM ecommerce.tb_sellers;
    
    
-- Verificando registros duplicados e incoerentes nas tabelas 


-- tb_customers

SELECT * FROM ecommerce.tb_customers 
    WHERE 
        customer_unique_id IS NULL
     OR customer_zip_code_prefix IS NULL
     OR customer_city IS NULL
     OR customer_state IS NULL;

SELECT customer_unique_id, 
    COUNT(customer_unique_id) AS quantity
    FROM ecommerce.tb_customers
        GROUP BY 
            customer_unique_id
        HAVING 
            COUNT(customer_unique_id) > 1
        ORDER BY 
            COUNT(customer_unique_id) DESC;

SELECT * FROM ecommerce.tb_customers 
    WHERE customer_unique_id='8d50f5eadf50201ccdcedfb9e2ac8455';

-- Existem dados duplicados na coluna customer_unique_id, porém conforme a descrição da tabela
-- no site Kaggle está correto conforme regra estabelecida pela Olist.
  
  
-- tb_geolocation

SELECT * FROM ecommerce.tb_geolocation
    WHERE 
        geolocation_zip_code_prefix IS NULL
     OR geolocation_lat IS NULL
     OR geolocation_lng IS NULL
     OR geolocation_city IS NULL
     OR geolocation_state IS NULL;

SELECT geolocation_lat, geolocation_lng, 
    COUNT(*) AS quantity
    FROM ecommerce.tb_geolocation
        GROUP BY 
            geolocation_lat, geolocation_lng
        HAVING 
            COUNT(*) > 1
        ORDER BY 
            COUNT(*) DESC;

CREATE TABLE ecommerce.tb_geolocation_TEMP AS 
    SELECT DISTINCT * FROM ecommerce.tb_geolocation;
    
ALTER TABLE 
    IF EXISTS ecommerce.tb_geolocation_TEMP 
    OWNER to postgres;

DROP TABLE ecommerce.tb_geolocation;

ALTER TABLE ecommerce.tb_geolocation_TEMP 
    RENAME TO tb_geolocation;

-- Existiam linhas duplicadas, porém nessa tabela não há uma coluna com um identificador 
-- exclusivo para cada linha então foi criada uma tabela com os dados exclusivos e substituída
-- a tabela anterior.


-- tb_order_items

SELECT * FROM ecommerce.tb_order_items
    WHERE 
        order_item_id IS NULL
     OR product_id IS NULL
     OR seller_id IS NULL
     OR shipping_limit_date IS NULL
     OR price IS NULL
     OR freight_value IS NULL;

SELECT order_id, 
    COUNT(order_id) AS quantity
    FROM ecommerce.tb_order_items
        GROUP BY 
            order_id
        HAVING 
            COUNT(order_id) > 1
        ORDER BY 
            COUNT(order_id) DESC;

SELECT * FROM ecommerce.tb_order_items 
    WHERE order_id='8272b63d03f5f79c56e9e4120aec44ef';

-- Uma ordem de venda pode conter um ou vários produtos distintos ou não, portanto não
-- há necessidade de efetuar qualquer ajuste


-- tb_order_payments

SELECT * FROM ecommerce.tb_order_payments
    WHERE 
        payment_sequential IS NULL
     OR payment_type IS NULL
     OR payment_installments IS NULL
     OR payment_value IS NULL;
    
SELECT order_id, 
    COUNT(order_id) AS quantity
    FROM ecommerce.tb_order_payments
        GROUP BY 
            order_id
        HAVING 
            COUNT(order_id) > 1
        ORDER BY 
            COUNT(order_id) DESC;

SELECT * FROM ecommerce.tb_order_payments 
    WHERE order_id='fa65dad1b0e818e3ccc5cb0e39231352';

-- Uma ordem de venda pode ser com uma ou várias formas de pagamento e estas 
-- podem ser parceladas em várias vezes, nenhuma alteração executada.


-- tb_order_reviews

SELECT * FROM ecommerce.tb_order_reviews
    WHERE 
        review_score IS NULL
     OR review_comment_title IS NULL
     OR review_comment_message IS NULL
     OR review_creation_date IS NULL
     OR review_answer_timestamp IS NULL;

SELECT order_id, 
    COUNT(order_id) AS quantity
    FROM ecommerce.tb_order_reviews
        GROUP BY 
            order_id
        HAVING 
            COUNT(order_id) > 1
        ORDER BY 
            COUNT(order_id) DESC;

SELECT * FROM ecommerce.tb_order_reviews 
    WHERE order_id='c88b1d1b157a9999ce368f218a407141';

-- Após a conclusão da entrega do pedido o cliente recebe um e-mail convidando a efetuar
-- uma avaliação, entretanto tanto o título quanto o mensagem da review são opcionais.
-- Apesar de existirem pedidos com mais de uma avaliação os demais campos apresentam notas
-- reviews, data e horas distintas, não foram feitas alterações.


-- tb_orders

SELECT * FROM ecommerce.tb_orders
    WHERE 
        order_status IS NULL
     OR order_purchase_timestamp IS NULL
     OR order_approved_at IS NULL
     OR order_delivered_carrier_date IS NULL
     OR order_delivered_customer_date IS NULL
     OR order_estimated_delivery_date IS NULL;

SELECT * FROM ecommerce.tb_orders LIMIT 100;

-- Um cliente pode efetuar um ou várias compras e que podem estar em várias estágios desde
-- a criação da ordem até a sua entrega, ainda entre esses estágios algum outro processo
-- pode determinar a continuidade da venda ou não, resultando em por exemplo o cancelamento,
-- sendo assim pode ocorrer de existir registros de operações de venda ainda não concluídos.
-- Esta é a principal tabela do banco de dados, alimentada por pelas demais.


-- tb_products

SELECT * FROM ecommerce.tb_products LIMIT 100;
    
SELECT * FROM ecommerce.tb_products 
    WHERE product_category_name IS NULL;    
   
SELECT * FROM ecommerce.tb_products po
    INNER JOIN ecommerce.tb_order_items oi 
    ON oi.product_id = po.product_id
    WHERE 
        product_category_name IS NULL LIMIT 100; 

-- Há produtos sem categoria, nome, descrição e/ou informações de dimensão e volume,
-- porém possuem um identificador e constam nas ordens de pedido dos clientes, logo
-- esses registros serão mantidos.
    
    
-- tb_sellers  

SELECT * FROM ecommerce.tb_sellers 
    WHERE 
        seller_zip_code_prefix IS NULL
     OR seller_city IS NULL
     OR seller_state IS NULL;

SELECT * FROM ecommerce.tb_sellers LIMIT 100;

-- Não há dados duplicados ou nulos na tabela de vendedores