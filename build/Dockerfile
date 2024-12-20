# Sử dụng hình ảnh chính thức của Airflow
FROM apache/airflow:2.7.0-python3.9

# Thiết lập môi trường
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
RUN pip install pandas openpyxl snowflake-connector-python cx_Oracle apache-airflow oracledb apache-airflow-providers-oracle

# Cài đặt thư viện Oracle
USER root
RUN apt-get update && apt-get install -y \
    libaio1 \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Sao chép và cài đặt Oracle Instant Client
COPY instantclient-basic-linux.x64-19.19.0.0.0dbru.el9.zip /opt/oracle/
RUN unzip /opt/oracle/instantclient-basic-linux.x64-19.19.0.0.0dbru.el9.zip -d /opt/oracle && \
    ln -s /opt/oracle/instantclient_* /opt/oracle/instantclient && \
    echo "/opt/oracle/instantclient" > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig && \
    rm -rf /opt/oracle/*.zip

# Cài đặt các yêu cầu bổ sung nếu cần
# COPY ../build/requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt
