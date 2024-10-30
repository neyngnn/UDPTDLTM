import sys
import os
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

# Đặt đường dẫn root
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

from pipelines.flights_pipeline import run_pipeline

# Thiết lập các tham số mặc định cho DAG
default_args = {
    'owner': 'omar-sinno',
    'start_date': datetime(2024, 4, 4, 1, 41)
}

# Danh sách các sân bay để lặp qua
airports = ["HAN", "SGN", "DAD"]

with DAG(dag_id='google_flights_pipeline', default_args=default_args, schedule='@daily') as dag:
    
    for departure_id in airports:
        for arrival_id in airports:
            if departure_id != arrival_id:  # Chỉ tạo task cho các cặp sân bay khác nhau
                task_id = f'google_flights_extraction_{departure_id}_to_{arrival_id}'
                
                op_kwargs = {
                    "engine": "google_flights",
                    "departure_id": departure_id,
                    "arrival_id": arrival_id,
                    "outbound_date": "2024-12-01",
                    "return_date": "2024-12-02",
                    "currency": "VND",
                    "hl": "vi"
                }
                
                # Tạo task PythonOperator cho từng cặp sân bay
                PythonOperator(
                    task_id=task_id,
                    python_callable=run_pipeline,
                    op_kwargs=op_kwargs,
                )
