"""
Script for creating and updating tables, filling them with data
"""

from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values, DictCursor
import pandas as pd

CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS transaction (
    id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    online_order BOOLEAN,
    order_status VARCHAR NOT NULL CHECK (order_status IN 
    ('Approved', 'Cancelled')
    ),
    transaction_date DATE NOT NULL,
    list_price FLOAT NOT NULL,
    standard_cost FLOAT
);

CREATE TABLE IF NOT EXISTS customer (
    id BIGINT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR,
    gender VARCHAR NOT NULL,
    date_of_birthday DATE,
    job_id INTEGER NOT NULL,
    deceased_indicator CHAR(1) NOT NULL,
    owns_car VARCHAR NOT NULL CHECK (owns_car IN ('Yes','No')),
    address TEXT NOT NULL,
    postal_object_id INTEGER NOT NULL,
    property_valuation INTEGER NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS product_id_seq;

CREATE TABLE IF NOT EXISTS product (
    id BIGINT PRIMARY KEY DEFAULT nextval('product_id_seq'),
    product_number INTEGER NOT NULL,
    brand VARCHAR,
    product_line VARCHAR,
    product_class VARCHAR CHECK (product_class IN 
    ('low', 'medium', 'high')
    ),
    product_size VARCHAR CHECK (product_size IN 
    ('small', 'medium', 'large')
    )
);

CREATE SEQUENCE IF NOT EXISTS job_id_seq;

CREATE TABLE IF NOT EXISTS job (
  id INTEGER PRIMARY KEY DEFAULT nextval('job_id_seq'),
  job_title VARCHAR,
  job_industry_category VARCHAR NOT NULL,
  wealth_segment VARCHAR NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS postal_object_id_seq;

CREATE TABLE IF NOT EXISTS postal_object (
  id INTEGER PRIMARY KEY DEFAULT nextval('postal_object_id_seq'),
  postcode INTEGER NOT NULL,
  state VARCHAR NOT NULL,
  country VARCHAR NOT NULL
)
"""


ALTER_TABLE_QUERY = """
ALTER TABLE IF EXISTS transaction
ADD CONSTRAINT fk_transaction_customer
FOREIGN KEY (customer_id) REFERENCES customer(id);

ALTER TABLE IF EXISTS transaction
ADD CONSTRAINT fk_transaction_product
FOREIGN KEY (product_id) REFERENCES product(id);

ALTER TABLE IF EXISTS customer
ADD CONSTRAINT fk_customer_job
FOREIGN KEY (job_id) REFERENCES job(id);

ALTER TABLE IF EXISTS customer
ADD CONSTRAINT fk_customer_postal_object
FOREIGN KEY (postal_object_id) REFERENCES postal_object(id);
"""


def insert_data(pd_data: pd.DataFrame, db_table_name: str) -> None:
    """
    Inserts data from the DataFrame into the specified database table.

    Args:
        pd_data (pd.DataFrame): A DataFrame with the data to insert
        db_table_name (str): The name of the data insertion table
    """
    columns = pd_data.columns.tolist()

    for column in columns:
        pd_data[column] = pd_data[column].apply(
            lambda val: None if pd.isna(val) or val == '' else val
        )

    insert_query = f"""
        INSERT INTO {db_table_name} ({', '.join(columns)})
        VALUES %s
    """

    execute_values(cursor, insert_query, pd_data.values.tolist())


def get_ids_by_composite_key(db_table_name: str, columns: list[str]) \
        -> dict[str, int]:
    """
    Retrieves a dictionary of the correspondence of composite keys to the IDs
    of records in the table.

    Args:
        db_table_name (str): The name of the table in the database from which the
        data is extracted
        columns (list): List of columns for a composite key

    Returns:
        dict: A dictionary where the key is a composite key and the value is ID
    """
    result = dict.fromkeys(columns)

    with connection.cursor(cursor_factory=DictCursor) as dict_cursor:
        dict_cursor.execute(
            f"SELECT id, {', '.join(columns)} FROM {db_table_name}")
        rows = dict_cursor.fetchall()

        for row in rows:
            composite_key = '_'.join(
                [str(row[val]) if row[val] else '_' for val in columns]
            )
            result[composite_key] = row['id']
        return result

# Connecting to the database
connection = psycopg2.connect(
    host="localhost",
    database="homework_1",
    user="postgres",
    password="iamroot",
    port="5432"
)
cursor = connection.cursor()

# Creating tables
cursor.execute(CREATE_TABLE_QUERY)
cursor.execute(ALTER_TABLE_QUERY)

# Inserting data into the tables job, postal_object, product
data_to_insert = {'job', 'postal_object', 'product'}

for table_name in data_to_insert:
    data = pd.read_csv(
        Path(f'data_to_insert/{table_name}.csv'),
        keep_default_na=False
    )
    insert_data(data, table_name)

# Getting an ID using composite keys
job_ids_by_composite_key = get_ids_by_composite_key(
    'job',
    ["job_title", "job_industry_category", "wealth_segment"]
)

postal_object_ids_by_composite_key = get_ids_by_composite_key(
    'postal_object',
    ["postcode", "state"]
)

# Processing and inserting customer data
data_customer = pd.read_csv(
    Path('data_to_insert/customer.csv'),
    keep_default_na=False
)

data_customer['job_id'] = (
    data_customer[['job_title', 'job_industry_category', 'wealth_segment']]
    .apply(
        lambda x: job_ids_by_composite_key[(
            f'{x["job_title"] if x["job_title"] else "_"}_'
            f'{x["job_industry_category"] if x["job_industry_category"] else "_"}_'
            f'{x["wealth_segment"] if x["wealth_segment"] else "_"}'
        )],
        axis=1
    )
)

data_customer['postal_object_id'] = (
    data_customer[['postcode', 'state']]
    .apply(
        lambda x: postal_object_ids_by_composite_key[(
            f'{x["postcode"] if x["postcode"] else "_"}_'
            f'{x["state"] if x["state"] else "_"}'
        )],
        axis=1
    )
)

data_customer = data_customer.drop(
    columns=[
        'job_title', 'job_industry_category', 'wealth_segment',
        'postcode', 'state'
    ]
)
insert_data(data_customer, 'customer')

# Getting product IDs using a composite key
product_ids_by_composite_key = get_ids_by_composite_key(
    'product',
    [
        "product_number", "brand", "product_line", "product_class",
        "product_size"
    ]
)

# Processing and inserting transaction data
data_transactions = pd.read_csv(
    Path('data_to_insert/transaction.csv'),
    keep_default_na=False
)

data_transactions['transaction_date'] = pd.to_datetime(
    data_transactions['transaction_date']
)

data_transactions['product_id'] = (
    data_transactions[
        [
            'product_id', 'brand', 'product_line', 'product_class',
            'product_size'
        ]
    ]
    .apply(
        lambda x: product_ids_by_composite_key[(
            f'{x["product_id"] if x["product_id"] else "_"}_'
            f'{x["brand"] if x["brand"] else "_"}_'
            f'{x["product_line"] if x["product_line"] else "_"}_'
            f'{x["product_class"] if x["product_class"] else "_"}_'
            f'{x["product_size"] if x["product_size"] else "_"}'
        )],
        axis=1
    )
)

data_transactions = data_transactions.drop(
    columns=['brand', 'product_line', 'product_class', 'product_size']
)
insert_data(data_transactions, 'transaction')

# Fixing changes in the database
connection.commit()

# Closing the connection
cursor.close()
connection.close()
