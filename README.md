# Projeto de Engenharia de dados

## PySpark e Jupyter

- Parte 1: - Instalação do Delta, PySpark e Apache Iceberg
- Parte 2: - Instalar pyspark e delta
  ```python
    pip install pyspark
    pip instal delta 
  
- Parte 3: - Importar bibliotecas para usar o Pyspark e Delta
  ```python
  from pyspark.sql import SparkSession
  from pyspark.sql.types import StructType, StructField, StringType, FloatType

- Parte 4: - Iniciar uma sessão no pyspark
  ```python
  spark = ( 
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate() 
  )
