from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from functions import utils
from functions import transformation_functions as tfunc
from functions import insight_functions as ifunc

def step_extraction(**kwargs):
    print('Extraction step')
    source_files = [
        ('BaseCargos.csv', 'cargos_raw', ';'),
        ('BaseCEP.csv', 'cep_raw', '|'),
        ('BaseClientes.csv', 'clientes_raw',';'),
        ('BaseFuncionarios.csv', 'funcionarios_raw','||'),
        ('BaseNível.csv', 'nivel_raw','%'),
        ('BasePQ.csv', 'pq_raw', ';')
    ]
            
    spark = utils.start_spark_session("Extraction")
    for file, table, sep in source_files:
        print(f"Lendo arquivo: {file} ...")
        df = utils.read_csv(spark, f"/opt/airflow/sources/{file}", sep=sep)
        print("Arquivo lido com sucesso!")
        print(f"Escrevendo tabela {table}...")
        utils.write_table(df, table)
        print("Tabela escrita com sucesso!")
    spark.stop()
    
def step_transformation(**kwargs):
    print('Transformation step')
    tables = [
        'cargos',
        'cep',
        'clientes',
        'funcionarios',
        'nivel',
        'pq'
    ]
    spark = utils.start_spark_session("Transformation")
    for table in tables:
        print(f"Transformando tabela {table}_raw...")
        df = utils.read_table(spark, f"{table}_raw")
        transform_function = getattr(tfunc, f"transform_{table}")
        df = transform_function(df)
        print("Tabela transformada com sucesso!")
        print(f"Escrevendo tabela {table}_stg...")
        utils.write_table(df, f"{table}_stg")
        print("Tabela escrita com sucesso!")
    spark.stop()

def step_landing(**kwargs):
    print('Landing step')
    spark = utils.start_spark_session("Insight")
    
    print("Gerando insights Funcionarios por Nivel...")
    df = ifunc.insight_funcionarios_nivel(spark)
    utils.write_table(df, 'insight_funcionarios_nivel')
    print("Insight gerado com sucesso!")
    
    print("Gerando insights Salario por Nivel...")
    df = ifunc.insight_salario_nivel(spark)
    utils.write_table(df, 'insight_salario_nivel')
    print("Insight gerado com sucesso!")
    
    print("Gerando insights Nivel Responsavel por Clientes...")
    df = ifunc.insight_nivel_cliente(spark)
    utils.write_table(df, 'insight_nivel_cliente')
    print("Insight gerado com sucesso!")
    
    print("Gerando insights Geografico de Clientes...")
    df = ifunc.insight_geografico_clientes(spark)
    utils.write_table(df, 'insight_geografico_clientes')
    print("Insight gerado com sucesso!")
    
    print("Gerando insights Clientes Anuais...")
    df = ifunc.insight_cliente_ano(spark)
    utils.write_table(df, 'insight_cliente_ano')
    print("Insight gerado com sucesso!")
    
    spark.stop()
    
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 1,
}

with DAG('etl_pipeline_dag', default_args=default_args) as dag:
    start = DummyOperator(task_id='start')
    
    step_extraction = PythonOperator(
        task_id='step_extraction',
        python_callable=step_extraction
    )
    step_transformation = PythonOperator(
        task_id='step_transformation',
        python_callable=step_transformation
    )
    step_landing = PythonOperator(
        task_id='step_landing',
        python_callable=step_landing
    )
    end = DummyOperator(task_id='end')

    start >> step_extraction >> step_transformation >> step_landing >> end