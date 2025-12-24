# 使用官方推荐的 dbt-snowflake 镜像
FROM ghcr.io/dbt-labs/dbt-snowflake:1.7.0

# 设置工作目录
WORKDIR /usr/app/dbt

# 安装 git (dbt deps 需要 git 来拉取包)
USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

# 这里的 USER 保持 root 或者切换回 dbt_user 取决于你的权限需求，
# 学习环境用 root 方便处理文件权限。