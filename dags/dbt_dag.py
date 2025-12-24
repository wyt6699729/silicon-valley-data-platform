import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# 1. 定义路径
DBT_ROOT_PATH = Path("/usr/local/airflow/dags/dbt/dbt_project/analytics")

# 2. 定义连接配置
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default", 
        profile_args={"database": "DBT_DEV", "schema": "PUBLIC"},
    ),
)

# 3. 定义 DAG
my_dbt_dag = DbtDag(
    project_config=ProjectConfig(DBT_ROOT_PATH),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,  # 自动运行 dbt deps
    },
    dag_id="dbt_tpch_dag",
    schedule="@daily",       # <--- 关键修改：用 schedule 替代 schedule_interval
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["dbt", "snowflake"],
)