import os 
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="lesson4_sensor",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # 我们还是切回手动触发，方便观察
    catchup=False,
    tags=["learning", "sensors"]
)

def sensor_pipeline():
    @task.sensor(poke_interval=30, timeout=600, mode="poke")
    @task.sensor(poke_interval=30, timeout=600, mode="poke")
    def wait_for_start_signal():
        # 🔴 修改这里：
        # 不要去 /tmp 找了，那是容器私有的。
        # 我们去 dags 目录下找，那是和你的 VS Code 共享的！
        # 注意：在容器里，dags 的绝对路径是 /usr/local/airflow/dags
        file_path = "/usr/local/airflow/dags/start_signal.txt"
        
        if os.path.exists(file_path):
            print(f"发现了信号文件！路径: {file_path}")
            return True 
        else:
            print(f"文件还没到... 再等 30 秒...")
            return False
    @task
    def run_dbt_job():
        print("✅ 启动信号收到，开始运行 dbt 任务...")
    
    wait_for_start_signal() >> run_dbt_job()

sensor_dag = sensor_pipeline()