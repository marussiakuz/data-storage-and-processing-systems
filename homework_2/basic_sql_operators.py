"""
Script for creating and updating tables, filling them with data
"""

from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

CREATE_TABLE_QUERY = """
ALTER TABLE order_items DROP CONSTRAINT IF EXISTS fk_order_items;
ALTER TABLE orders DROP CONSTRAINT IF EXISTS fk_order_customer;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer; 
DROP TABLE IF EXISTS product_temp;
DROP TABLE IF EXISTS product;

CREATE TABLE IF NOT EXISTS customer (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR,
    gender VARCHAR NOT NULL,
    DOB DATE,
    job_title VARCHAR,
    job_industry_category VARCHAR NOT NULL,
    wealth_segment VARCHAR NOT NULL,
    deceased_indicator CHAR(1) NOT NULL,
    owns_car VARCHAR NOT NULL CHECK (owns_car IN ('Yes','No')),
    address TEXT NOT NULL,
    postcode INTEGER NOT NULL,
    state VARCHAR NOT NULL,
    country VARCHAR NOT NULL,
    property_valuation INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS product_temp (
    product_id BIGINT NOT NULL,
    brand VARCHAR,
    product_line VARCHAR,
    product_class VARCHAR CHECK (product_class IN 
    ('low', 'medium', 'high')
    ),
    product_size VARCHAR CHECK (product_size IN 
    ('small', 'medium', 'large')
    ),
    list_price FLOAT NOT NULL,
    standard_cost FLOAT
);

CREATE TABLE IF NOT EXISTS product (
    product_id BIGINT PRIMARY KEY,
    brand VARCHAR,
    product_line VARCHAR,
    product_class VARCHAR CHECK (product_class IN 
    ('low', 'medium', 'high')
    ),
    product_size VARCHAR CHECK (product_size IN 
    ('small', 'medium', 'large')
    ),
    list_price FLOAT NOT NULL,
    standard_cost FLOAT
);

CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  order_date DATE NOT NULL,
  online_order BOOLEAN,
  order_status VARCHAR NOT NULL CHECK (order_status IN 
  ('Approved', 'Cancelled')
  )
);

CREATE TABLE IF NOT EXISTS order_items (
  order_item_id BIGINT NOT NULL,
  order_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  quantity INTEGER NOT NULL,
  item_list_price_at_sale FLOAT NOT NULL,
  item_standard_cost_at_sale FLOAT
);
"""


ALTER_TABLE_QUERY = """
ALTER TABLE IF EXISTS order_items
ADD CONSTRAINT fk_order_items_order
FOREIGN KEY (order_id) REFERENCES orders(order_id);
"""


DROP_AND_ALTER_TABLE_AFTER_CLEAR_DATA_QUERY = """
DROP TABLE IF EXISTS product_temp;

ALTER TABLE IF EXISTS orders
ADD CONSTRAINT fk_order_customer
FOREIGN KEY (customer_id) REFERENCES customer(customer_id);

ALTER TABLE IF EXISTS order_items
ADD CONSTRAINT fk_order_items_product
FOREIGN KEY (product_id) REFERENCES product(product_id);
"""


def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="homework_2",
        user="postgres",
        password="iamroot",
        port="5432"
    )

# Connecting to the database
connection = get_connection()
cursor = connection.cursor()

# Creating tables
cursor.execute(CREATE_TABLE_QUERY)
cursor.execute(ALTER_TABLE_QUERY)

print('Таблицы созданы')

data_to_insert = {'customer': ';', 'orders': ',', 'product': ',', 'order_items': ','}

for table_name, delimiter_for_table in data_to_insert.items():
    data = pd.read_csv(
        Path(f'data_to_insert/{table_name}.csv'),
        keep_default_na=False,
        delimiter=delimiter_for_table
    )
    if table_name == 'product':
        table_name = 'product_temp'
    insert_query = f'INSERT INTO {table_name} VALUES %s'
    data = data.replace('', None)
    data = data.where(pd.notna(data), None)
    execute_values(cursor, insert_query, [data_list for data_list in data.values.tolist()])

# Fixing changes in the database
connection.commit()

print('Таблицы заполнены данными')

INSERT_TO_PRODUCTION_QUERY = """
INSERT INTO product 
SELECT product_id, brand, product_line, product_class, product_size, list_price, standard_cost
FROM (
    SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY list_price DESC) AS rn 
    FROM product_temp 
    ORDER BY product_id, brand, rn
    ) AS temp 
WHERE rn = 1
"""

cursor.execute(INSERT_TO_PRODUCTION_QUERY)
print('Таблица product заполнена данными')

cursor.execute(DROP_AND_ALTER_TABLE_AFTER_CLEAR_DATA_QUERY)
# Fixing changes in the database
connection.commit()

print('Таблицы заполнены данными')

# Closing the connection
cursor.close()
connection.close()