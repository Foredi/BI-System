import uuid
import psycopg2
from psycopg2 import sql
import datetime as dt
import pandas as pd
import os


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct

# Hoàn thành việc kết nối với Postgres và Qdrant
qdrant_client = QdrantClient(host="qdrant_db", port=6333)
# kết nối với Postgres
conn = psycopg2.connect(
    user="",
    password="",
    host="",
    port="",
    database=""
)
cursor = conn.cursor()
# tôi có 5 bảng trong database postgres của mình là: "Orders", "Customers", "Order_Details", "Products", "Suppliers" và 5 file csv tương ứng
# tên các bảng trong Postgres
tables = ['Customers', 'Orders', 'Suppliers', 'Products', 'Order_Details']
# tên các bảng trong Qdrant
collections = ['Customers', 'Orders', 'Suppliers', 'Products', 'Order_Details']
# cấu hình vector params cho collection bao gồm size = 1536 và distance = cosine
vectorParams = {
    'size': 1536,
    'distance': Distance.COSINE
}

def connect_postgres():
    try:
        conn = psycopg2.connect(
            user="postgres.zcnfjmdqotssgdjktdga",
            password="Data_platform123",
            host="aws-0-ap-southeast-1.pooler.supabase.com",
            port="5432",
            database="postgres"
        )
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

def create_collection_qdrant():
    try:
        # lấy tên tất cả collection hiện có trong Qdrant
        cles = qdrant_client.get_collections()
        names = [c.name for c in cles.collections]

        # cấu hình vector params cho collection bao gồm size = 1536 và distance = cosine
        vectorParams = {
            'size': 1536,
            'distance': Distance.COSINE
        }
        # kiểm tra nếu collection chưa tồn tại thì tạo mới
        # Sử dụng **vectorParams để unpack dict thành các keyword arguments
        for name in collections:
            if name not in names:
                qdrant_client.recreate_collection(
                    collection_name=name,
                    vectors_config=VectorParams(**vectorParams)
                )
        return {
            "status": "success",
            "collections": collections,
            "vectorParams": str(vectorParams)
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
    
def insert_data_postgres():
    try:
        for table in tables:
            df = pd.read_csv(f"/opt/airflow/dags/sample/{table}.csv")
            if not df.empty:
                insert_query = sql.SQL('INSERT INTO platform.{} ({}) VALUES ({}) ON CONFLICT DO NOTHING').format(
                    sql.Identifier(table.lower()),
                    sql.SQL(', ').join([sql.Identifier(str(col).replace(' ', '_').lower()) for col in df.columns]),
                    sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
                )
                
                for row in df.itertuples(index=False):
                    try:
                        cursor.execute(insert_query, tuple(row))
                    except Exception as e:
                        print(f"Error inserting row {row} in table {table}: {e}")
                        conn.rollback()
            else:
                conn.commit()

        return {"status": "success", "tables": tables}
    
    except Exception as e:
        print(f"Transaction failed: {e}")
        conn.rollback()
        return {"status": "error", "message": str(e)}

    
def insert_data_qdrant():
    try:
        for table in tables:
            # lấy dữ liệu từ Postgres
            cursor.execute(f'SELECT * FROM platform.{table}')
            records = cursor.fetchall()
            # insert dữ liệu vào Qdrant
            for record in records:
                point = PointStruct(
                    id=uuid.uuid4().hex,
                    vector=record
                )
                qdrant_client.insert_point(collection_name=table, point=point)
        return {
            "status": "success",
            "tables": tables
        }
    except Exception as e:
        print(f"Transaction failed: {e}")
        conn.rollback()
        return {
            "status": "error",
            "message": str(e)
        }
    
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime.now() - dt.timedelta(minutes=2),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=2),
}

with DAG('pipeline', default_args=default_args, schedule_interval='0 * * * *') as dag:
    create_collection_qdrant = PythonOperator(
        task_id='create_collection_qdrant',
        python_callable=create_collection_qdrant
    )
    
    insert_data_postgres = PythonOperator(
        task_id='insert_data_postgres',
        python_callable=insert_data_postgres
    )
    
    insert_data_qdrant = PythonOperator(
        task_id='insert_data_qdrant',
        python_callable=insert_data_qdrant
    )

    start = BashOperator(task_id='start', bash_command='echo "Pipeline started"')

    end = BashOperator(task_id='end', bash_command='echo "Pipeline finished"')
    
    start >> create_collection_qdrant >> insert_data_postgres >> insert_data_qdrant >> end

    
