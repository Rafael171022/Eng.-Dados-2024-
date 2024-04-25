# Projeto de Engenharia de dados

## PySpark e Jupyter

- Parte 1: - Instalação do Delta, PySpark e Apache Iceberg
  ```python
    pip install pyspark
    pip instal delta 
  
- Parte 2: - Importar bibliotecas para usar o Pyspark e Delta
  ```python
  from pyspark.sql import SparkSession
  from pyspark.sql.types import StructType, StructField, StringType, FloatType

- Parte 3: - Iniciar uma sessão no pyspark
  ```python
  spark = ( 
    SparkSession
    .builder
    .appName("")
    .getOrCreate() 
  )
