from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
from datetime import datetime

# Khởi tạo logger
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

# Hàm ghi log và thực hiện tác vụ DB
def log_and_execute_db_task(task_id, sql, **context):
    execution_date = context["execution_date"]
    logger.info(f"Task {task_id} is starting at {execution_date}.")
    logger.info("=================================================================================")
    logger.info("=================================================================================")
    logger.info("=================================================================================")
    
    # Thực thi câu lệnh SQL
    postgres_hook = PostgresHook(postgres_conn_id="postgres-db")
    postgres_hook.run(sql)
    
    logger.info(f"Task {task_id} finished at {execution_date}.")
    logger.info("=================================================================================")
    logger.info("=================================================================================")
    logger.info("=================================================================================")
# Khởi tạo DAG
active_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}
with DAG(
    dag_id="active_user",
    default_args=active_args,
    description="Active new user",
    # schedule_interval='*/5 * * * *',  # Chạy mỗi 5 phút
    schedule_interval=None,  # Chạy thủ công
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:
    # SQL cho từng bước
    sql_get_inactive_users = """
        INSERT INTO ACTIVE_USER_TEMP SELECT ID FROM CUSTOMER WHERE STATUS = '0';
    """
    
    sql_insert_account = """
        INSERT INTO ACCOUNTS (CUSTOMER_ID) SELECT ID FROM ACTIVE_USER_TEMP;
    """
    
    sql_update_customer = """
        UPDATE CUSTOMER SET STATUS = '1' WHERE ID IN (SELECT ID FROM ACTIVE_USER_TEMP);
    """
    
    sql_delete_temp = """
        DELETE FROM ACTIVE_USER_TEMP;
    """
    
    getInactiveUsers = PythonOperator(
        task_id="getInactiveUsers",
        python_callable=log_and_execute_db_task,
        op_args=["getInactiveUsers", sql_get_inactive_users],
        provide_context=True,
    )
    
    insertActiveUsers = PythonOperator(
        task_id="insertActiveUsers",
        python_callable=log_and_execute_db_task,
        op_args=["insertActiveUsers", sql_insert_account],
        provide_context=True,
    )
    
    updateCustomer = PythonOperator(
        task_id="updateCustomer",
        python_callable=log_and_execute_db_task,
        op_args=["updateCustomer", sql_update_customer],
        provide_context=True,
    )
    
    deleteTemp = PythonOperator(
        task_id="deleteTemp",
        python_callable=log_and_execute_db_task,
        op_args=["deleteTemp", sql_delete_temp],
        provide_context=True,
    )
    
    getInactiveUsers >> insertActiveUsers >> updateCustomer >> deleteTemp
    
    
