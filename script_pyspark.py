# -*- coding: utf-8 -*-
'''
Análise de dados: Comércio eletrônico brasileiro

Este projeto é composto por um conjunto de dados públicos de comércio eletrônico brasileiro, 
disponibilizados pelo site Olist, são registros que compõem todo o processo de venda de um 
produto, da compra, pagamento, entrega e avaliação, além de dados de geolocalização, produtos 
e vendedores. Estas informações serão tratadas e analisadas, de modo a responder questões 
de negócio.
'''

# Importação de bibliotecas

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, sum, hour

print('\nCriação e iniciação de uma sessão Spark\n')

appName = 'PySpark - Olist'

spark = SparkSession.builder \
    .appName('PySpark - Olist') \
    .config('spark.driver.memory', '8g') \
    .config('spark.driver.cores', '2') \
    .config('spark.executor.memory', '8g') \
    .config('spark.executor.cores', '4') \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print(spark)

# print(spark.sparkContext.getConf().getAll())


# Criação dos datasets a partir da leitura dos arquivos *.csv

df_orders = spark.read.csv('dados\olist_orders_dataset.csv', sep=',', header=True,
                           encoding='utf-8', inferSchema=True)
df_customers = spark.read.csv('dados\olist_customers_dataset.csv', sep=',', header=True,
                              encoding='utf-8', inferSchema=True)
df_geolocation = spark.read.csv('dados\olist_geolocation_dataset.csv', sep=',', header=True,
                                encoding='utf-8', inferSchema=True)
df_order_items = spark.read.csv('dados\olist_order_items_dataset.csv', sep=',', header=True,
                                encoding='utf-8', inferSchema=True)
df_order_payments = spark.read.csv('dados\olist_order_payments_dataset.csv', sep=',', header=True,
                                   encoding='utf-8', inferSchema=True)
df_order_reviews = spark.read.csv('dados\olist_order_reviews_dataset.csv', sep=',', header=True,
                                  encoding='utf-8', inferSchema=True,
                                  multiLine=True, ignoreLeadingWhiteSpace=True, escape='"', quote='"')
df_products = spark.read.csv('dados\olist_products_dataset.csv', sep=',', header=True,
                             encoding='utf-8', inferSchema=True)
df_sellers = spark.read.csv('dados\olist_sellers_dataset.csv', sep=',', header=True,
                            encoding='utf-8', inferSchema=True)

print('\nVerificando os tipos das colunas\n')

df_orders.printSchema()
df_customers.printSchema()
df_geolocation.printSchema()
df_order_items.printSchema()
df_order_payments.printSchema()
df_order_reviews.printSchema()
df_products.printSchema()
df_sellers.printSchema()


print('Verificando a existência de registros nulos')


def check_nulls(dataframe, name) -> None:
    '''
    Verifica e exibe a quantidade de valores nulos em cada coluna do dataframe.

    :param dataframe: DataFrame
        Dataframe a ser analisado.
    :param name: str
        Nome identificando o dataframe para exibição na saída.
    '''
    print(f'\n{name.upper()} { "-" * (100 - len(name))}')
    for coluna in dataframe.columns:
        qty = dataframe.filter(dataframe[coluna].isNull()).count()
        if qty >= 1:
            print(f'{coluna}: {qty}')


check_nulls(df_orders, 'df_orders')
check_nulls(df_customers, 'df_customers')
check_nulls(df_geolocation, 'df_geolocation')
check_nulls(df_order_items, 'df_order_items')
check_nulls(df_order_payments, 'df_order_payments')
check_nulls(df_order_reviews, 'df_order_reviews')
check_nulls(df_products, 'df_products')
check_nulls(df_sellers, 'df_sellers')

print('''\nForam identificados valores nulos em 3 dataframes, df_orders, df_order_reviews e df_products,
entretando no caso de df_orders os dados representam operações de venda, logo possui vários
estágios podendo ser uma venda concluída, cancelada, processamento ou mesmo em trânsito, ou seja,
dependendo do estágio algumas colunas podem ficarem vazias (nulas), em df_order_reviews há campos
com reviews dos compradores sobre suas compras, não é obrigatório um cliente escrever um review e
em df_products há produtos com nome e descrição ausentes, porém constam em pedidos de clientes.\n''')


print('Verificando a existência de registros duplicados\n')

def check_duplicates(dataframe, fields) -> None:
    '''
    Verifica e exibe uma amostra de 5 registros duplicados com base em um ou mais campos especificados.

    :param dataframe: DataFrame
        Dataframe a ser analisado.
    :param fields: str ou list de str
        Nome do campo ou lista de campos a serem usados como referência para identificar duplicatas.
    '''
    duplicate = dataframe.groupBy(fields) \
        .agg(count('*').alias('qty')) \
        .where(col('qty') > 1) \
        .orderBy(desc('qty'))
    duplicate.show(5, truncate=False)


check_duplicates(df_customers, 'customer_unique_id')

print('''Em df_customers existem dados duplicados na coluna customer_unique_id, porém conforme a  descrição
da tabela no site Kaggle está correto conforme regra estabelecida pela Olist, basicamente esse
campo permite que se identifique clientes que fizeram recompras.\n''')

check_duplicates(df_geolocation, ['geolocation_lat', 'geolocation_lng'])

df_geolocation = df_geolocation.dropDuplicates()

df_geolocation.show(5, truncate=False)

print('''Existiam linhas duplicadas em df_geolocation, porém nesse dataframe não há uma coluna com um
identificador exclusivo para cada linha então foi criada uma tabela com os dados exclusivos baseado
nas colunas geolocation_lat e geolocation_lng e substituído dataframe anterior.\n''')

check_duplicates(df_order_items, 'order_id')

print('''Há linhas duplicadas em df_order_items, este dataframe contêm os itens que compõem uma compra, logo
uma ordem de venda pode conter um ou vários produtos distintos ou não, portanto não há necessidade
de efetuar qualquer ajuste.\n''')

check_duplicates(df_order_payments, 'order_id')

print('''O dataframe df_order_payments consiste na cadastro de meios de pagamentos, parcelas (quando aplicável),
então uma ordem de venda pode ser com uma ou várias formas de pagamento e estas podem ser parceladas em
várias vezes, nenhuma alteração executada.\n''')

check_duplicates(df_order_reviews, 'order_id')

print('''Após a conclusão da entrega do pedido o cliente recebe um e-mail convidando a efetuar uma avaliação,
entretanto tanto o título quanto o mensagem da review são opcionais. Apesar de existirem pedidos com
mais de uma avaliação os demais campos apresentam notas reviews, data e horas distintas, são essas
informações que alimentam o dataframe df_order_reviews. Não foram feitas alterações.\n''')

check_duplicates(df_orders, 'order_id')

print('''Um cliente pode efetuar um ou várias compras e que podem estar em várias estágios desde a criação da
ordem até a sua entrega, ainda entre esses estágios algum outro processo pode determinar a continuidade
da venda ou não, resultando em por exemplo o cancelamento, sendo assim pode ocorrer de existir registros
de operações de venda ainda não concluídos, estas são as informaçõe que compõem o df_orders.\n''')

check_duplicates(df_products, 'product_id')

print('''O dataframe df_products contém o cadastro de produtos, descrição, dimensões, entre outras informações.\n''')

check_duplicates(df_sellers, 'seller_id')

print('''O dataframe df_sellers contém o cadastro de vendedores e sua localização (cep, cidade e estado).\n''')


# Criando views temporárias para uso do Spark SQL

df_orders.createOrReplaceTempView('tb_orders')
df_customers.createOrReplaceTempView('tb_customers')
df_geolocation.createOrReplaceTempView('tb_geolocation')
df_order_items.createOrReplaceTempView('tb_order_items')
df_order_payments.createOrReplaceTempView('tb_order_payments')
df_order_reviews.createOrReplaceTempView('tb_order_reviews')
df_products.createOrReplaceTempView('tb_products')
df_sellers.createOrReplaceTempView('tb_sellers')


# Criando views SQL temporárias

spark.sql('''
    CREATE TEMP VIEW vw_order_costumer AS 
    SELECT 
        oo.customer_id,
        oo.order_id,
        cc.customer_unique_id,
        cc.customer_city,
        cc.customer_state        
    FROM tb_orders oo
    INNER JOIN tb_customers cc 
    ON cc.customer_id = oo.customer_id;
''')

spark.sql('''
    CREATE TEMP VIEW vw_price_freight AS 
        SELECT
            cc.customer_city,
            cc.customer_state,
            oo.order_id,
            oo.order_purchase_timestamp,
            oi.price,
            oi.freight_value
        FROM tb_orders oo
        INNER JOIN tb_customers cc 
        ON cc.customer_id = oo.customer_id
        INNER JOIN tb_order_items oi 
        ON oi.order_id = oo.order_id;
''')

print('\n01 - Qual o total(quantidade) de vendas divididas por estado?\n')

spark.sql('''
    SELECT 
        customer_state, 
        COUNT(order_id) amount_of_sales
    FROM vw_order_costumer 
        GROUP BY 
            customer_state 
        ORDER BY 
            amount_of_sales DESC;
''').show(31, truncate=False)

print('02 - Qual o total(valor) de vendas e fretes divididos por estado?\n')

spark.sql('''
    SELECT
        customer_state,
        CAST(SUM (price) AS DECIMAL(10,2)) AS total_price,
        CAST(SUM (freight_value) AS DECIMAL(10,2)) AS total_freight
    FROM vw_price_freight
        GROUP BY 
            customer_state
        ORDER BY 
            total_price DESC;
''').show(31, truncate=False)

print('03 - Qual o total(quantidade) e distribuição(%) de vendas por hora?\n')

spark.sql('''
    SELECT
        date_format(order_purchase_timestamp, 'HH') AS hour_24,
        COUNT(order_id) AS amount_per_hour,
        CAST(COUNT(*) * 100 / SUM(COUNT(*)) OVER() AS DECIMAL(10,2)) AS perc_score
    FROM tb_orders
        GROUP BY 
            hour_24
        ORDER BY 
            hour_24;
''').show(28, truncate=False)

print('04 - Qual a média(valor) de vendas por hora?\n')

spark.sql('''
    SELECT 
        hour_24,
        CAST(AVG (sum_total) AS DECIMAL(10,2)) AS avg_total
    FROM (
            SELECT 
                oo.order_id, 
                date_format(order_purchase_timestamp, 'HH') AS hour_24,
                SUM (price) AS sum_total
            FROM tb_orders oo
                INNER JOIN tb_order_items oi 
                ON oi.order_id = oo.order_id 
                GROUP BY 
                    oo.order_id, 
                    hour_24,
                    oo.order_purchase_timestamp
                ORDER BY 
                    hour_24
            ) AS total_amount_per_order
        GROUP BY 
            hour_24
        ORDER BY 
            hour_24;
''').show(28, truncate=False)

print('05 - Qual o ticket médio nos anos de 2016, 2017 e 2018?\n')

spark.sql('''
    SELECT 
        ext_year,
        CAST(AVG (sum_total) AS DECIMAL(10,2)) AS avg_ticket
    FROM (
            SELECT 
                oo.order_id,
                EXTRACT (YEAR FROM order_purchase_timestamp) AS ext_year, 
                SUM (price) AS sum_total
            FROM tb_orders oo
                INNER JOIN tb_order_items oi 
                ON oi.order_id = oo.order_id 
                GROUP BY 
                    ext_year,
                    oo.order_id
                ORDER BY 
                    ext_year
            ) AS total_amount_per_order
        GROUP BY 
            ext_year
        ORDER BY 
            ext_year;
''').show(truncate=False)

print('06 - Qual a distribuição(%) da pontuação do pedidos?\n')

spark.sql('''
    SELECT 
        review_score,
        COUNT(*) AS qty_score,
        CAST(COUNT(*) * 100 / SUM(COUNT(*)) OVER() AS DECIMAL(10,2)) AS perc_score
    FROM tb_order_reviews
        GROUP BY
            review_score
        ORDER BY
            review_score DESC;
''').show(truncate=False)

print('07 - Quais as 10 cidades com as maiores volumes(quantidade) de vendas?\n')

spark.sql('''
    SELECT 
        customer_city, 
        customer_state, 
        COUNT(*) AS top_10
    FROM vw_order_costumer 
        GROUP BY 
            customer_city, 
            customer_state 
        ORDER BY 
            top_10 DESC
        LIMIT 10;
''').show(truncate=False)

print('08 - Quais as 10 cidades com os maiores volumes(valores) de vendas e fretes?\n')

spark.sql('''
    SELECT 
        customer_city, 
        customer_state, 
        CAST(SUM (price) AS DECIMAL(10,2)) AS total_price,
        CAST(SUM (freight_value) AS DECIMAL(10,2)) AS total_freight
    FROM vw_price_freight 
        GROUP BY 
            customer_city, 
            customer_state 
        ORDER BY 
            total_price DESC
        LIMIT 10;
''').show(truncate=False)

print('09 - Qual a quantidade de produtos cadastrados por categoria?\n')

spark.sql('''
    SELECT 
        product_category_name,
        COUNT(product_id) AS qty_product
    FROM tb_products
        GROUP BY
            product_category_name
        ORDER BY
            qty_product DESC;
''').show(78, truncate=False)

print('10 - Qual a quantidade e distribuição(%) das categorias nos pedidos?\n')

spark.sql('''
    SELECT
        product_category_name,
        COUNT(oi.product_id) AS qty_product,
        CAST(COUNT(oi.product_id) * 100 / SUM(COUNT(oi.product_id)) OVER() AS DECIMAL(10,2)) AS perc_category
    FROM tb_products po
        INNER JOIN tb_order_items oi 
        ON po.product_id = oi.product_id
        GROUP BY
            product_category_name
        ORDER BY
            perc_category DESC;
''').show(78, truncate=False)

print('11 - Qual a quantidade de vendas por vendedor?\n')

spark.sql('''
    SELECT
        se.seller_id,
        COUNT(DISTINCT oi.order_id) AS qty_order_by_seller
    FROM tb_sellers se
        INNER JOIN tb_order_items oi 
        ON se.seller_id = oi.seller_id
        GROUP BY 
            se.seller_id
        ORDER BY
            qty_order_by_seller DESC;
''').show(truncate=False)

spark.stop()
