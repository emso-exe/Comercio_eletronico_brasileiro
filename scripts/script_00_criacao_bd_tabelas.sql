-- Database: olist

-- DROP DATABASE IF EXISTS olist;

CREATE DATABASE olist
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Portuguese_Brazil.1252'
    LC_CTYPE = 'Portuguese_Brazil.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
    
    
-- SCHEMA: ecommerce

-- DROP SCHEMA IF EXISTS ecommerce ;

CREATE SCHEMA IF NOT EXISTS ecommerce
    AUTHORIZATION postgres;
    
    
-- Table: ecommerce.tb_customers

-- DROP TABLE IF EXISTS ecommerce.tb_customers;

CREATE TABLE IF NOT EXISTS ecommerce.tb_customers
(
    customer_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    customer_unique_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    customer_zip_code_prefix character varying(5) COLLATE pg_catalog."default" NOT NULL,
    customer_city character varying(50) COLLATE pg_catalog."default" NOT NULL,
    customer_state character varying(2) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT tb_customers_pkey PRIMARY KEY (customer_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecommerce.tb_customers
    OWNER to postgres;

COMMENT ON TABLE ecommerce.tb_customers
    IS 'Tabela referente ao dataset olist_customers_dataset
        customer_id: identificação do cliente
        customer_unique_id:	ID exclusivo
        customer_zip_code_prefix: prefixo do código postal
        customer_city: cidade
        customer_state:	estado';


-- Table: ecommerce.tb_geolocation

-- DROP TABLE IF EXISTS ecommerce.tb_geolocation;

CREATE TABLE IF NOT EXISTS ecommerce.tb_geolocation
(
    geolocation_zip_code_prefix character varying(5) COLLATE pg_catalog."default" NOT NULL,
    geolocation_lat double precision NOT NULL,
    geolocation_lng double precision NOT NULL,
    geolocation_city character varying(50) COLLATE pg_catalog."default" NOT NULL,
    geolocation_state character varying(2) COLLATE pg_catalog."default" NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecommerce.tb_geolocation
    OWNER to postgres;

COMMENT ON TABLE ecommerce.tb_geolocation
    IS 'Tabela referente ao dataset olist_geolocation_dataset
        geolocation_zip_code_prefix: prefixo de código postal
        geolocation_lat: latitude
        geolocation_lng: longitude
        geolocation_city: cidade
        geolocation_state: estado de geolocalização';
    
    
-- Table: ecommerce.tb_order_items

-- DROP TABLE IF EXISTS ecommerce.tb_order_items;

CREATE TABLE IF NOT EXISTS ecommerce.tb_order_items
(
    order_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    order_item_id integer NOT NULL,
    product_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    seller_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    shipping_limit_date timestamp without time zone NOT NULL,
    price double precision NOT NULL,
    freight_value double precision NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecommerce.tb_order_items
    OWNER to postgres;

COMMENT ON TABLE ecommerce.tb_order_items
    IS 'Tabela referente ao dataset olist_order_items_dataset
        order_id: ID do pedido
        order_item_id: ID do item do pedido
        product_id: ID do produto
        seller_id: ID do vendedor
        shipping_limit_date: data limite de envio
        price: preço
        freight_value: valor do frete';


-- Table: ecommerce.tb_order_payments

-- DROP TABLE IF EXISTS ecommerce.tb_order_payments;

CREATE TABLE IF NOT EXISTS ecommerce.tb_order_payments
(
    order_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    payment_sequential integer NOT NULL,
    payment_type character varying(50) COLLATE pg_catalog."default" NOT NULL,
    payment_installments integer NOT NULL,
    payment_value double precision NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecommerce.tb_order_payments
    OWNER to postgres;

COMMENT ON TABLE ecommerce.tb_order_payments
    IS 'Tabela referente ao dataset olist_order_payments_dataset
        order_id: ID do pedido
        payment_sequential: pagamento sequencial
        payment_type: tipo de pagamento
        payment_installments: pagamento parcelado
        payment_value: valor de pagamento';


-- Table: ecommerce.tb_order_reviews

-- DROP TABLE IF EXISTS ecommerce.tb_order_reviews;

CREATE TABLE IF NOT EXISTS ecommerce.tb_order_reviews
(
    review_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    order_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    review_score integer NOT NULL,
    review_comment_title character varying(100) COLLATE pg_catalog."default",
    review_comment_message character varying(300) COLLATE pg_catalog."default",
    review_creation_date date NOT NULL,
    review_answer_timestamp timestamp without time zone NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecommerce.tb_order_reviews
    OWNER to postgres;

COMMENT ON TABLE ecommerce.tb_order_reviews
    IS 'Tabela referente ao dataset olist_order_reviews_dataset
        review_id: ID de revisão
        order_id: ID do pedido
        review_score: pontuação
        review_comment_title: título do comentário
        review_comment_message: mensagem do comentário
        review_creation_date: data de criação
        review_answer_timestamp: data e hora da resposta';
    
    
-- Table: ecommerce.tb_orders

-- DROP TABLE IF EXISTS ecommerce.tb_orders;

CREATE TABLE IF NOT EXISTS ecommerce.tb_orders
(
    order_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    customer_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    order_status character varying(50) COLLATE pg_catalog."default" NOT NULL,
    order_purchase_timestamp timestamp without time zone NOT NULL,
    order_approved_at timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date date NOT NULL,
    CONSTRAINT tb_orders_dataset_pkey PRIMARY KEY (order_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecommerce.tb_orders
    OWNER to postgres;

COMMENT ON TABLE ecommerce.tb_orders
    IS 'Tabela referente ao dataset olist_orders_dataset.csv
        order_id: ID do pedido
        customer_id: ID do Cliente
        order_status: status do pedido
        order_purchase_timestamp: data e hora da compra
        order_approved_at: data e hora da aprovação
        order_delivered_carrier_date: data e hora de entrega do pedido a transportadora
        order_delivered_customer_date: data e hora de entrega do pedido ao cliente
        order_estimated_delivery_date: data estimada de entrega do pedido';
    
    
-- Table: ecommerce.tb_products

-- DROP TABLE IF EXISTS ecommerce.tb_products;

CREATE TABLE IF NOT EXISTS ecommerce.tb_products
(
    product_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    product_category_name character varying(50) COLLATE pg_catalog."default",
    product_name_lenght integer,
    product_description_lenght integer,
    product_photos_qty integer,
    product_weight_g integer,
    product_length_cm integer,
    product_height_cm integer,
    product_width_cm integer,
    CONSTRAINT tb_products_pkey PRIMARY KEY (product_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecommerce.tb_products
    OWNER to postgres;

COMMENT ON TABLE ecommerce.tb_products
    IS 'Tabela referente ao dataset olist_products_dataset
        product_id: ID do produto
        product_category_name: nome da categoria do produto
        product_name_lenght: comprimento do nome do produto
        product_description_lenght: comprimento da descrição do produto
        product_photos_qty: quantidade de fotos
        product_weight_g: peso (g)
        product_length_cm: comprimento (cm)
        product_height_cm: altura (cm)
        product_width_cm: largura (cm)';
    
    
-- Table: ecommerce.tb_sellers

-- DROP TABLE IF EXISTS ecommerce.tb_sellers;

CREATE TABLE IF NOT EXISTS ecommerce.tb_sellers
(
    seller_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    seller_zip_code_prefix character varying(5) COLLATE pg_catalog."default" NOT NULL,
    seller_city character varying(50) COLLATE pg_catalog."default" NOT NULL,
    seller_state character varying(2) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT "olist_sellers_dataset.csv_pkey" PRIMARY KEY (seller_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecommerce.tb_sellers
    OWNER to postgres;

COMMENT ON TABLE ecommerce.tb_sellers
    IS 'Tabela referente ao dataset olist_sellers_dataset
        seller_id: ID do vendedor
        seller_zip_code_prefix: prefixo do código postal
        seller_city: cidade
        seller_state: estado';