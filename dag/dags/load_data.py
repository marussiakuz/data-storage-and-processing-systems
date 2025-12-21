from datetime import datetime, timedelta
import logging
import pandas as pd
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)

DAGS_DIR = Path(__file__).parent
BASE_DIR = DAGS_DIR.parent
DATA_DIR = BASE_DIR / 'data_to_insert'

TABLES = ['customer', 'product', 'orders', 'order_items']

TABLE_CREATION_SQL = {
    'customer': """
        DROP TABLE IF EXISTS customer;
        CREATE TABLE IF NOT EXISTS customer (
            customer_id BIGINT PRIMARY KEY,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR,
            gender VARCHAR NOT NULL,
            DOB DATE,
            job_title VARCHAR,
            job_industry_category VARCHAR,
            wealth_segment VARCHAR,
            deceased_indicator CHAR(1) NOT NULL,
            owns_car VARCHAR NOT NULL CHECK (owns_car IN ('Yes','No')),
            address TEXT NOT NULL,
            postcode INTEGER NOT NULL,
            state VARCHAR NOT NULL,
            country VARCHAR NOT NULL,
            property_valuation INTEGER NOT NULL
        );
    """,
    'product': """
        DROP TABLE IF EXISTS product;
        CREATE TABLE IF NOT EXISTS product (
            product_id BIGINT PRIMARY KEY,
            brand VARCHAR,
            product_line VARCHAR,
            product_class VARCHAR,
            product_size VARCHAR,
            list_price FLOAT NOT NULL,
            standard_cost FLOAT
        );
    """,
    'orders': """
        DROP TABLE IF EXISTS orders;
        CREATE TABLE IF NOT EXISTS orders (
            order_id BIGINT PRIMARY KEY,
            customer_id BIGINT NOT NULL,
            order_date DATE,
            online_order BOOLEAN,
            order_status VARCHAR NOT NULL
        );
    """,
    'order_items': """
        DROP TABLE IF EXISTS order_items;
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id BIGINT PRIMARY KEY,
            order_id BIGINT NOT NULL,
            product_id BIGINT NOT NULL,
            quantity INTEGER NOT NULL,
            item_list_price_at_sale FLOAT NOT NULL,
            item_standard_cost_at_sale FLOAT
        );
    """
}


def on_failure_callback(context):
    error_message = "ЗАДАЧА СОЗДАНИЯ И ЗАГРУЗКИ ТАБЛИЦ ПРОВАЛЕНА!"
    logger.error(error_message)


def on_success_callback(context):
    success_message = "ЗАДАЧА СОЗДАНИЯ И ЗАГРУЗКИ ТАБЛИЦ ВЫПОЛНЕНА УСПЕШНО!"
    logger.info(success_message)


def check_database_connection():
    """Проверка подключения к базе данных"""
    try:
        postgres_conn_id = 'postgres_default'
        logger.info(f"Проверка подключения к БД с conn_id: {postgres_conn_id}")

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()

        logger.info(f"Подключение успешно! База данных: {db_name[0]}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {str(e)}")
        raise AirflowException(f"Database connection failed: {str(e)}")


def check_tables_exist():
    """
    Проверка существования всех необходимых таблиц
    Возвращает True если все таблицы существуют, False если хотя бы одной нет
    """
    try:
        postgres_conn_id = 'postgres_default'
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        missing_tables = []
        existing_tables = []

        for table_name in TABLES:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (table_name,))

            table_exists = cursor.fetchone()[0]

            if table_exists:
                existing_tables.append(table_name)
                logger.info(f"Таблица '{table_name}' существует")
            else:
                missing_tables.append(table_name)
                logger.warning(f"Таблица '{table_name}' не найдена")

        cursor.close()
        conn.close()

        if missing_tables:
            logger.warning(f"Отсутствуют таблицы: {', '.join(missing_tables)}")
            logger.info(f"Существующие таблицы: {', '.join(existing_tables)}")
            return False
        else:
            logger.info("Все необходимые таблицы существуют!")
            return True

    except Exception as e:
        logger.error(f"Ошибка при проверке существования таблиц: {str(e)}")
        raise AirflowException(f"Failed to check tables existence: {str(e)}")


def create_all_tables_if_needed():
    """
    Проверяет существование таблиц и создает отсутствующие
    """
    try:
        postgres_conn_id = 'postgres_default'

        # Проверяем существование таблиц
        all_tables_exist = check_tables_exist()

        if all_tables_exist:
            logger.info("Все таблицы существуют, предсоздание не требуется")
            return True

        logger.info("Старт создания отсутствующих таблиц...")
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        for table_name, sql in TABLE_CREATION_SQL.items():
            try:
                logger.info(f"Создание таблицы '{table_name}'...")
                cursor.execute(sql)
                logger.info(f"Таблица '{table_name}' успешно создана")
            except Exception as e:
                logger.error(f"Ошибка при создании таблицы '{table_name}': {str(e)}")

        conn.commit()
        cursor.close()
        conn.close()

        final_check = check_tables_exist()
        if not final_check:
            raise AirflowException("Не удалось создать все необходимые таблицы")

        logger.info("Все таблицы успешно созданы!")
        return True

    except Exception as e:
        logger.error(f"Ошибка при создании таблиц: {str(e)}")
        raise AirflowException(f"Failed to create tables: {str(e)}")


def recreate_all_tables():
    """
    Принудительное пересоздание всех таблиц
    """
    try:
        postgres_conn_id = 'postgres_default'
        logger.info("Старт принудительного пересоздания всех таблиц")

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        for table_name, sql in TABLE_CREATION_SQL.items():
            try:
                logger.info(f"Пересоздание таблицы '{table_name}'...")
                cursor.execute(sql)
                logger.info(f"Таблица '{table_name}' успешно пересоздана")
            except Exception as e:
                logger.error(f"Ошибка при пересоздании таблицы '{table_name}': {str(e)}")
                raise

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Все таблицы успешно пересозданы!")
        return True

    except Exception as e:
        logger.error(f"Ошибка при пересоздании таблиц: {str(e)}")
        raise AirflowException(f"Failed to recreate tables: {str(e)}")


def log_table_creation(table_name):
    """Функция для логирования создания таблицы"""
    logger.info(f"Таблица {table_name} успешно создана/проверена")
    return True


def log_all_tables_ready():
    """Логирование готовности всех таблиц"""
    logger.info("Все таблицы готовы для загрузки данных!")
    return True


def load_table_data(table_name, postgres_conn_id='postgres_default'):
    """
    Универсальная функция для загрузки данных в таблицу из CSV файла
    """
    try:
        csv_file = DATA_DIR / f"{table_name}.csv"

        if not csv_file.exists():
            logger.error(f"Файл не найден: {csv_file}")
            raise AirflowException(f"File not found: {csv_file}")

        logger.info(f"Старт загрузки данных в таблицу '{table_name}' из файла: {csv_file}")

        df = pd.read_csv(csv_file)
        logger.info(f"Прочитано {len(df)} строк из файла {table_name}.csv")

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """, (table_name,))

        table_exists = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        if not table_exists:
            logger.error(f"Таблица '{table_name}' не найдена")
            raise AirflowException(f"Table '{table_name}' does not exist")

        logger.info(f"Загрузка данных в таблицу '{table_name}'...")
        df.to_sql(table_name,
                  pg_hook.get_sqlalchemy_engine(),
                  if_exists='append',
                  index=False,
                  method='multi',
                  chunksize=1000)

        logger.info(f"Успешно загружено {len(df)} строк в таблицу '{table_name}'")
        return True

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в таблицу '{table_name}': {str(e)}")
        raise AirflowException(f"Failed to load {table_name} data: {str(e)}")


def log_all_data_loaded():
    """Логирование успешной загрузки всех данных"""
    logger.info("Все данные успешно загружены во все таблицы!")
    return True


default_args = {
    'start_date': datetime(2025, 12, 21),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_callback
}

with DAG(
        dag_id='table_creation_and_load',
        default_args=default_args,
        description='Создание таблиц и загрузка данных',
        schedule_interval='5 17 * * *',
        catchup=False,
        tags=['table', 'customer', 'production', '', 'data_load'],
        max_active_runs=1,
) as dag:
    # 1. Проверка подключения к БД
    check_db_connection_task = PythonOperator(
        task_id='check_database_connection',
        python_callable=check_database_connection
    )

    # 2. Проверка существования таблиц
    check_tables_task = PythonOperator(
        task_id='check_tables_existence',
        python_callable=check_tables_exist
    )

    # 3. Создание таблиц (если нужно)
    create_tables_if_needed_task = PythonOperator(
        task_id='create_tables_if_needed',
        python_callable=create_all_tables_if_needed
    )

    # 4. Принудительное пересоздание таблиц (branching задача)
    recreate_tables_task = PythonOperator(
        task_id='recreate_all_tables',
        python_callable=recreate_all_tables
    )

    # 5. Логирование готовности таблиц
    log_tables_ready_task = PythonOperator(
        task_id='log_all_tables_ready',
        python_callable=log_all_tables_ready
    )

    # 6. Задачи загрузки данных для каждой таблицы
    load_tasks = []
    for table_name in TABLES:
        task = PythonOperator(
            task_id=f'load_{table_name}_data',
            python_callable=load_table_data,
            op_kwargs={'table_name': table_name}
        )
        load_tasks.append(task)

    # 7. Финальное логирование
    log_all_data_loaded_task = PythonOperator(
        task_id='log_all_data_loaded',
        python_callable=log_all_data_loaded
    )

    # Определение зависимостей
    # Основной поток: проверка подключения -> проверка таблиц -> создание при необходимости -> загрузка
    check_db_connection_task >> check_tables_task >> create_tables_if_needed_task

    # Альтернативный поток: принудительное пересоздание (можно активировать через BranchPythonOperator)
    check_db_connection_task >> recreate_tables_task

    # После создания таблиц (любым способом) продолжаем с загрузкой
    create_tables_if_needed_task >> log_tables_ready_task
    recreate_tables_task >> log_tables_ready_task

    # Параллельная загрузка данных
    log_tables_ready_task >> load_tasks

    # Финальное логирование после загрузки всех данных
    load_tasks >> log_all_data_loaded_task