from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# 1. 路径还是原来的路径
DBT_ROOT_PATH = Path("/usr/local/airflow/dags/dbt/dbt_project/analytics")

# 2. 连接配置 (保持不变)
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={"database": "DBT_DEV", "schema": "PUBLIC"},
    ),
)

# 3. 关键点：高级渲染配置 (RenderConfig)
# 这就是 Senior Engineer 和 Junior 的区别
render_config = RenderConfig(
    # 策略：跑完一个模型，立刻跑它的测试。
    # 如果测试挂了，后面的依赖任务直接停止，止损！
    test_behavior=TestBehavior.AFTER_EACH,
    
    # 视觉优化：虽然我们只有几个模型，但这个配置允许 Airflow 
    # 把 dbt 的 models 文件夹渲染成 UI 上的“任务组” (Task Groups)，看起来更整洁
    select=["path:models"] 
)

# 4. 定义高级 DAG
my_advanced_dag = DbtDag(
    project_config=ProjectConfig(DBT_ROOT_PATH),
    profile_config=profile_config,
    render_config=render_config,  # <--- 把高级配置塞进去
    operator_args={"install_deps": True},
    
    dag_id="dbt_advanced_quality_first", # 改个新名字
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["dbt", "advanced"],
)