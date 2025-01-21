from functions import utils
import pyspark.sql.functions as F

def transform_cargos(df):
    '''
    Transforma a tabela cargos para o formato esperado
    '''
    df = utils.standarize_columns(df)
    df = df.withColumn('area', F.regexp_replace('area', '@@@', ''))
    return df

def transform_cep(df):
    '''
    Transforma a tabela cep para o formato esperado
    '''
    df = utils.standarize_columns(df)
    return df

def transform_clientes(df):
    '''
    Transforma a tabela clientes para o formato esperado
    '''
    df = utils.standarize_columns(df)
    df = df.drop("_c7","_c8","_c9")
    return df

def transform_funcionarios(df):
    '''
    Transforma a tabela funcionarios para o formato esperado
    '''
    df = utils.standarize_columns(df)
    return df

def transform_nivel(df):
    '''
    Transforma a tabela nivel para o formato esperado
    '''
    df = utils.standarize_columns(df)
    return df

def transform_pq(df):
    '''
    Transforma a tabela pq para o formato esperado
    '''
    df = utils.standarize_columns(df)
    return df
