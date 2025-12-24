import random
from airflow.decorators import dag, task
from datetime import datetime
# 🔴 新增导入：用来跳过任务的特殊异常
from airflow.utils.edgemodifier import Label

@dag(
    dag_id="lesson1_branching",
    start_date=datetime(2025, 1, 1),
    schedule=None, # 我们还是切回手动触发，方便观察
    catchup=False,
    tags=["learning", "branching"]
)
def branching_pipeline():

    # 任务 1: 生成数据
    @task
    def get_sales_amount():
        amount = random.randint(100, 1000)
        print(f"生成的销售额: ${amount}")
        return amount

    # 任务 2: 【关键】分支控制器
    # 注意这里用的是 @task.branch，而不是普通的 @task
    @task.branch
    def choose_path(sales):
        if sales > 500:
            # 返回你要执行的下一个任务的 task_id (函数名)
            return "send_bonus"
        else:
            return "send_warning"

    # 任务 3A: 发奖金 (销售额 > 500 时跑)
    @task
    def send_bonus():
        print("🎉 恭喜！正在发放奖金...")

    # 任务 3B: 发警告 (销售额 <= 500 时跑)
    @task
    def send_warning():
        print("🚨 警告！业绩未达标，发送邮件...")

    # 任务 4: 汇总 (无论上面走哪条路，最后都要执行这步)
    # trigger_rule="none_failed_min_one_success" 意思是：
    # 只要上游有一个任务成功了（忽略被跳过的那个），我就跑。
    @task(trigger_rule="none_failed_min_one_success")
    def final_report():
        print("📋 流程结束，生成日报。")

    # ==========================
    # 编排依赖关系
    # ==========================
    sales = get_sales_amount()
    
    # 分支选择
    branch_result = choose_path(sales)

    # 定义两条路
    # 分支任务 >> [路A, 路B]
    branch_result >> [send_bonus(), send_warning()] >> final_report()

branching_pipeline()