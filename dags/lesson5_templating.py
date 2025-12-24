from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="lesson5_jinja_templating",
    start_date=datetime(2025, 12, 1), # 注意：我们设一个过去的时间
    schedule="@daily",                # 每天跑
    catchup=True,                     # 🔴 开启补数！让它把过去的账都算一遍
    tags=["learning", "jinja"]
)
def templating_pipeline():

    # 在 Python 函数里使用 Jinja 变量比较特殊
    # 我们需要通过参数 context 来获取
    # 或者直接使用 Airflow 提供的 **kwargs
    @task
    def print_date(**kwargs):
        # ds = logical date (数据日期)
        # ts = timestamp (具体时间戳)
        logical_date = kwargs['ds']
        
        print(f"-------------------------------------------")
        print(f"📅 我正在处理的数据日期是: {logical_date}")
        print(f"-------------------------------------------")

        timestamp = kwargs['ts']
        print(f"⏰ 具体的时间戳是: {timestamp}")
        print(f"-------------------------------------------")

    # 运行
    print_date()

templating_pipeline()