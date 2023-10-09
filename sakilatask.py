from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'sakila_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Menjalankan setiap hari
    catchup=False,  # Tidak mengeksekusi task yang terlewat
)

# Task 1: Membuat tabel fact_sales
create_fact_sales = MySqlOperator(
    task_id='create_fact_sales',
    mysql_conn_id='mysql_ID',
    sql='''
    CREATE TABLE fact_sales (
        sales_key INT AUTO_INCREMENT PRIMARY KEY,
        date_key INT NOT NULL,
        customer_key INT NOT NULL,
        movie_key INT NOT NULL,
        store_key INT NOT NULL,
        sales_amount FLOAT NOT NULL,
        FOREIGN KEY(date_key) REFERENCES dimDate(date_key),
        FOREIGN KEY(customer_key) REFERENCES dimCustomer(customer_key),
        FOREIGN KEY(movie_key) REFERENCES dimMovie(movie_key),
        FOREIGN KEY(store_key) REFERENCES dimStore(store_key)
    );
    ''',
    dag=dag,
)

# Task 2: Membuat tabel dimCustomer
create_dim_customer = MySqlOperator(
    task_id='create_dim_customer',
    mysql_conn_id='mysql_ID',
    sql='''
    CREATE TABLE dimcustomer (
        customer_key INT PRIMARY KEY,
        customer_id SMALLINT NOT NULL,
        first_name VARCHAR(45) NOT NULL,
        last_name VARCHAR(45) NOT NULL,
        email VARCHAR(50),
        address VARCHAR(50) NOT NULL,
        address2 VARCHAR(50),
        district VARCHAR(30) NOT NULL,
        city VARCHAR(50) NOT NULL,
        country VARCHAR(50) NOT NULL,
        postal_code INT,
        phone VARCHAR(20) NOT NULL,
        active SMALLINT NOT NULL,
        create_date TIMESTAMP NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL
    );
    ''',
    dag=dag,
)

# Task 3: Membuat tabel dimDate
create_dim_date = MySqlOperator(
    task_id='create_dim_date',
    mysql_conn_id='mysql_ID',
    sql='''
    CREATE TABLE dimdate (
        date_key INT PRIMARY KEY,
        date DATE NOT NULL,
        year SMALLINT NOT NULL,
        quarter SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        is_weekend BOOLEAN NOT NULL
    );
    ''',
    dag=dag,
)

# Task 4: Membuat tabel dimStore
create_dim_store = MySqlOperator(
    task_id='create_dim_store',
    mysql_conn_id='mysql_ID',
    sql='''
    CREATE TABLE dimstore (
        store_key INT PRIMARY KEY,
        store_id SMALLINT NOT NULL,
        address VARCHAR(50) NOT NULL,
        address2 VARCHAR(50),
        district VARCHAR(30) NOT NULL,
        city VARCHAR(50) NOT NULL,
        country VARCHAR(50) NOT NULL,
        postal_code INT,
        manager_first_name VARCHAR(45) NOT NULL,
        manager_last_name VARCHAR(45) NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL
    );
    ''',
    dag=dag,
)

# Task 5: Membuat tabel dimMovie
create_dim_movie = MySqlOperator(
    task_id='create_dim_movie',
    mysql_conn_id='mysql_ID',
    sql='''
    CREATE TABLE dimmovie (
        movie_key INT PRIMARY KEY,
        film_id INT NOT NULL,
        title VARCHAR(255) NOT NULL,
        description TEXT,
        release_year YEAR,
        language VARCHAR(20) NOT NULL,
        original_language VARCHAR(20),
        rental_duration SMALLINT NOT NULL,
        length SMALLINT NOT NULL,
        rating VARCHAR(5) NOT NULL,
        special_features VARCHAR(60) NOT NULL
    );
    ''',
    dag=dag,
)

# Set dependensi antara tugas-tugas
create_fact_sales >> create_dim_customer >> create_dim_date >> create_dim_store >> create_dim_movie

if __name__ == "__main__":
    dag.cli()
