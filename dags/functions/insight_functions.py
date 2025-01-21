from functions import utils
from pyspark.sql.functions import col, count, round, to_date, sum
from pyspark.sql.window import Window

def insight_funcionarios_nivel(spark):
    """
    Proposta de insight:
        Quantos funcionários estão em cada nível?
    """

    df_cargos = utils.read_table(spark, 'cargos_stg')
    df_funcionarios = utils.read_table(spark, 'funcionarios_stg')
    
    df_cargos_format = df_cargos.withColumnRenamed("cargo","id_cargo")
    df_merged = (df_funcionarios.join(df_cargos_format, 
                                      df_funcionarios.cargo == df_cargos_format.id_cargo, 
                                      'left'))
    
    # Retira os funcionários que foram demitidos
    df_merged = df_merged.filter(df_merged.data_de_demissao.isNotNull())
   
    df_grouped = (df_merged
                  .groupBy('nivel').count()
                  .orderBy('count', ascending=False)
                  ).withColumnRenamed('count', 'total_funcionarios')
    
    return df_grouped

def insight_salario_nivel(spark):
    """
    Proposta de insight:
        Qual a média de salários por nivel?
    """

    df_funcionarios = utils.read_table(spark, 'funcionarios_stg')
    df_cargos = utils.read_table(spark, 'cargos_stg')
    
    df_cargos_format = df_cargos.withColumnRenamed("cargo","id_cargo")  
    df_merged = (df_funcionarios.join(df_cargos_format, 
                                      df_funcionarios.cargo == df_cargos_format.id_cargo, 
                                      'left'))
    
    # Retira os funcionários que foram demitidos
    df_merged = df_merged.filter(df_merged.data_de_demissao.isNotNull())
    
    df_grouped = (df_merged.groupBy("nivel").agg({"salario_base": "avg"})
                .withColumnRenamed("avg(salario_base)", "media_salario_base")
                .withColumn("media_salario_base", round(col("media_salario_base"), 2))
                .orderBy(col("media_salario_base").desc())
                )
     
    return df_grouped

def insight_geografico_clientes(spark):
    """
    Proposta de insight:
        Qual o alcance geografico da empresa?
    """

    df_clientes = utils.read_table(spark, 'clientes_stg')
    df_cep = utils.read_table(spark, 'cep_stg')
    
    df_cep_format = df_cep.withColumnRenamed('cep','id_cep')
    df_merged = (df_clientes.join(df_cep_format, 
                                  df_clientes['cep'] == df_cep_format['id_cep'], 
                                  'left'))
    
    df_grouped = (df_merged
                  .groupBy('estado').count()
                  .orderBy('count', ascending=False)
                  ).withColumnRenamed('count', 'total_clientes')

    return df_grouped

def insight_nivel_cliente(spark):
    """
    Proposta de insight:
        Qual nível é responsável por mais clientes?
    """

    df_clientes = utils.read_table(spark, 'clientes_stg')
    df_cargo = utils.read_table(spark, 'cargos_stg')
       
    df_merged = (df_clientes.join(df_cargo, 
                                  df_clientes['cargo_responsavel'] == df_cargo['cargo'], 
                                  'left'))
    
    df_grouped = (df_merged
                  .groupBy("nivel").count()
                  .orderBy(col("count").desc())
                  ).withColumnRenamed("count", "total_clientes")
    return df_grouped

def insight_cliente_ano(spark):
    """
    Proposta de insight:
        Quantos clientes a empresa teve ao longo dos anos?
    """
    
    df_clientes = utils.read_table(spark, 'clientes_stg')
    
    df_clientes = (df_clientes
                   .withColumn('valor_contrato_anual', col('valor_contrato_anual').cast('int'))
                   .withColumn('data_inicio_contrato', to_date(col('data_inicio_contrato'), 'dd/MM/yyyy'))
                   .withColumn('ano', col('data_inicio_contrato').substr(1, 4)))
    
    # Utiliza a função Window para contar o total de clientes por ano
    window_spec = Window.partitionBy('ano')
    df_grouped = (df_clientes.withColumn('total_clientes', 
                              count('ano').over(window_spec)).distinct().orderBy('ano'))
    
    df_grouped = df_clientes.groupBy('ano').agg(
        count('ano').alias('total_clientes'),
        sum('valor_contrato_anual').alias('total_valor_contrato_anual')
    ).orderBy('ano')
    
    return df_grouped


