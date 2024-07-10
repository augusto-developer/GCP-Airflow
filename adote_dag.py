####################################################################################
## AUTOR: Caio Augusto
## DATA: 10/07/2024
## DESCRICAO: DAG QUE REALIZA CARGA DE ARQUIVOS CSV NO BIGQUERY, GERA UMA TABELA FILTRADA E MOVE OS ARQUIVOS FONTES.
####################################################################################

from datetime import datetime, timedelta  
import airflow
from airflow import DAG
from airflow.operators import bash_operator
import airflow.providers.google.cloud.operators.bigquery
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 20),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'schedule_interval': '30 20 * * *'
}

with DAG(    
    dag_id='adote_dag',
    default_args=default_args,
    schedule_interval=None,
    tags=['gcp','adote-doguinhos','big-query'],
) as dag:    

	t1_load_csv_bq = GCSToBigQueryOperator(
    task_id='csv_from_gcs_to_bigquery',
    bucket='dados_manipulados_adote',
    source_objects='v1/arquivos-para-tratar/*.csv',
	field_delimiter=',',
	skip_leading_rows=1,
    destination_project_dataset_table=f"meu_dataset.tb_raw_dogs_airflow", 
    schema_fields=[
		{'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'nome', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'idade', 'type': 'INTEGER', 'mode': 'NULLABLE'},
		{'name': 'raca', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'porte', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sexo', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'vacinado', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
		{'name': 'vermifugado', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
		{'name': 'castrado', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
		{'name': 'convive_com_animais', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
		{'name': 'convive_com_criancas', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

	t2_creating_table_from_query = BigQueryExecuteQueryOperator(
    task_id="querying_and_creating_bq_table",
    sql="""
    SELECT
      *
    FROM
      `adote-seu-bichinho.meu_dataset.tb_raw_dogs_airflow`
	WHERE
	SEXO = 'F' AND 
	CASTRADO = TRUE AND
	CONVIVE_COM_CRIANCAS = TRUE
    """,
    destination_dataset_table=f"adote-seu-bichinho.meu_dataset.tb_filtered_dogs_airflow",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="composer-bigquery",
    use_legacy_sql=False,
)

	t3_move_src_files_to_bkp = GCSToGCSOperator(
    task_id="move_source_file_to_backup",
    source_bucket='dados_manipulados_adote',
    source_object='v1/arquivos-para-tratar/*.csv',
    destination_bucket='dados_manipulados_adote',
    destination_object='v1/backup/',
    move_object=True,
)

	t1_load_csv_bq >> t2_creating_table_from_query >> t3_move_src_files_to_bkp