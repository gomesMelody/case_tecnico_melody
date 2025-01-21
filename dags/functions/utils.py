from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import pandas as pd

# Load arquivo .env
load_dotenv()

# Acessa variáveis de ambiente
JDBC_PATH = os.getenv("JDBC_PATH")
JDBC_URL = os.getenv("JDBC_URL")

CONNECTION_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER"), 
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": os.getenv("POSTGRES_DRIVER")
}
def return_path():
    print(JDBC_PATH)
    return JDBC_PATH

def standarize_columns(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Padroniza nomes das colunas do DataFrame
    '''
    for col in df.columns:
        new_col = (col.strip().lower()
                   .replace(' ', '_')
                   .replace('ç', 'c')
                   .replace('ã', 'a')
                   .replace('á', 'a')
                   .replace('é', 'e')
                   .replace('í', 'i')
                   .replace('õ', 'o')
                   .replace('ó', 'o')
                   )
        df = df.withColumnRenamed(col, new_col)
    
    return df


def read_csv(spark: SparkSession, file_path: str, sep: str = ',') -> pd.DataFrame:
    '''
    Lê um arquivo CSV e retorna um DataFrame do Spark
    '''
    #Lê o arquivo como texto
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Remove quebras de linha desnecessárias
    if ("BaseNível" in file_path):
        content = content.replace(f'{sep}\n', sep)
    
    if ("BaseClientes" in file_path):
        content = content.replace(f'{sep}{sep}{sep}{sep}{sep}', "")
        content = content.replace(f'{sep}\n', sep)
        content = content.replace(f'{sep}\n\n', sep)
        
    # Remove separadores desnecessários
    if ("BaseFuncionarios" not in file_path): 
        # Replace the specified string
        content = content.replace(f"{sep}{sep}", sep)   
    
    # Cria um RDD com o conteúdo do arquivo
    rdd = spark.sparkContext.parallelize(content.splitlines())

    return spark.read.option("delimiter", sep).csv(rdd, header=True, inferSchema=True)
    
    
def start_spark_session(app_name: str) -> SparkSession:
    '''
    Initializa uma SparkSession com PostgreSQL JDBC driver
    '''
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", JDBC_PATH) \
        .getOrCreate()
        
def read_table(spark: SparkSession, table_name: str):
    '''
    Lê uma tabela do PostgreSQL e retorna um DataFrame do Spark
    '''
    return spark.read.format('jdbc').options(
        url=JDBC_URL,
        dbtable=table_name,
        **CONNECTION_PROPERTIES
    ).load()
    
def write_table(df, table_name: str):
    '''
    Escreve uma tabela no PostgreSQL, sobrescreve se já existir
    '''
    df.write.format('jdbc').options(
        url=JDBC_URL,
        dbtable=table_name,
        **CONNECTION_PROPERTIES
    ).mode('overwrite').save()
    