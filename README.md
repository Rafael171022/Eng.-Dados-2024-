# Projeto de Engenharia de dados

## PySpark e Jupyter

### Antes de começar a trabalhar no nosso jupyter-nootbook vamos precisas instância-lo pelo docker da seguinte forma:
### Baixar a imagem oficial do Jupyter-notebook
  - docker pull jupyter/scipy-notebook

### Criar e executar um container com base na imagem, assim poderemos acessar o Jupyter Notebook no navegador usando o endereço http://localhost:8888.
  - docker run -p 8888:8888 my-jupyter-notebook

### Agora que conseguimos rodar o jupyter-notebook precisamos seguir esse passo a passo para criarmos nossa primeira tabela apartir da leitura de um arquivo CSV:
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

- Parte 4: - Importar um arquivo CSV
  ```python
  df = spark.read.csv("work/CarPrice_Assignment (2).csv", header=True, inferSchema=True)

- Parte 5: - Criar uma tabela temporária a partir do DataFrame
  ```python
  df.createOrReplaceTempView("carros")

- Parte 6: - Executar uma consulta SQL na tabela temporária
  ```python
  resultado = spark.sql("SELECT * FROM carros WHERE fueltype = 'gas'")
  resultado.show()

- Parte 7: - Criar a tabela temporária a partir do DataFrame df
  ```python
  df.createOrReplaceTempView("carros")

- Parte 8: - Criar um DataFrame com os dados fictícios
  ```python
      dados_ficticios = [
  Row(car_ID=206, CarName='toyota camry', fueltype='gas', doornumber='four', carbody='sedan', enginelocation='front', price=20000),
  Row(car_ID=207, CarName='honda accord', fueltype='gas', doornumber='two', carbody='convertible', enginelocation='front', price=25000),
  Row(car_ID=208, CarName='ford mustang', fueltype='gas', doornumber='two', carbody='hatchback', enginelocation='rear', price=30000),
  ]
  dados_ficticios_df = spark.createDataFrame(dados_ficticios)

- Parte 9: - Adicionar os dados fictícios à tabela temporária existente
  ```python
  dados_ficticios_df.write.mode("append").insertInto("carros")

- Parte 10: - Verificar se os dados foram adicionados corretamente à tabela temporária
  ```python
  spark.sql("SELECT * FROM carros").show()

### Como fazer um UPDATE
- Parte 1: - Ler os dados da tabela temporária "carros" em um DataFrame
  ```python
  df_carros = spark.sql("SELECT * FROM carros")

- Parte 2: - Atualizar o preço dos carros da marca "Toyota" para um novo preço
  ```python
  df_atualizado = df_carros.withColumn("price", when(df_carros["CarName"].like("%toyota%"), novo_preco_toyota).otherwise(df_carros["price"]))

- Parte 3: - Criar uma nova tabela temporária ou substituir a tabela temporária existente pelo DataFrame atualizado
  ```python
  df_atualizado.createOrReplaceTempView("carros")

- Parte 4: - Verificar se os dados foram atualizados corretamente na tabela temporária
  ```python
  spark.sql("SELECT * FROM carros").show()

### Como fazer um DELETE
- Parte 1: - Ler os dados da tabela temporária "carros" em um DataFrame
  ```python
  df_carros = spark.sql("SELECT * FROM carros")

- Parte 2: - Excluir os carros da marca "Toyota"
  ```python
  df_atualizado = df_carros.filter(~df_carros["CarName"].like("%toyota%"))

- Parte 3: - Criar uma nova tabela temporária ou substituir a tabela temporária existente pelo DataFrame atualizado
  ```python
  df_atualizado.createOrReplaceTempView("carros")

- Parte 4: - Verificar se os dados foram excluídos corretamente na tabela temporária
  ```python
  spark.sql("SELECT * FROM carros").show()

## Delta Iceberg
- Parte 1: # Inicializar uma sessão do Spark
  ```python
  spark = SparkSession.builder \
      .appName("Tabela Iceberg") \
      .config("spark.sql.catalog.default", "org.apache.iceberg.spark.SparkCatalog") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
      .getOrCreate()

- Parte 2: - Ler o arquivo CSV e criar o DataFrame
  ```python
  df = spark.read.csv("work/CarPrice_Assignment (2).csv", header=True, inferSchema=True)

- Parte 3: - Criar os dados fictícios
  ```python
  dados_ficticios = [
    Row(car_ID=206, CarName='toyota camry', fueltype='gas', doornumber='four', carbody='sedan', enginelocation='front', price=20000),
    Row(car_ID=207, CarName='honda accord', fueltype='gas', doornumber='two', carbody='convertible', enginelocation='front', price=25000),
    Row(car_ID=208, CarName='ford mustang', fueltype='gas', doornumber='two', carbody='hatchback', enginelocation='rear', price=30000)
  ]

- Parte 4: - Criar DataFrame com os dados fictícios
  ```python
  dados_ficticios_df = spark.createDataFrame(dados_ficticios)

- Parte 5: - Unir os dados do CSV com os dados fictícios
  ```python
  novo_df = df_csv.union(dados_ficticios_df)

- Parte 6: - Salvar os dados na tabela Iceberg
  ```python
  novo_df.write.format("iceberg") \
    .mode("overwrite") \  # Sobrescrever a tabela existente
    .save("Tabela Iceberg")

### Como fazer um UPDATE na tabela Iceberg

- Parte 1: - Ler os dados da tabela Iceberg para um DataFrame
  ```python
  df_iceberg = spark.read.format("iceberg").load("work/CarPrice_Assignment (2).csv")

- Parte 2: - Fazer as atualizações desejadas no DataFrame
  Suponha que você queira atualizar o preço do carro com car_ID igual a 206 para 22000
  ```python
  df_iceberg = df_iceberg.withColumn("price", 
                                   when(df_iceberg["car_ID"] == 206, 22000)
                                   .otherwise(df_iceberg["price"]))

- Parte 3: - Salvar o DataFrame atualizado de volta na tabela Iceberg
  ```python
  df_iceberg.write.format("iceberg") \
    .mode("overwrite") \  # Sobrescrever os dados existentes
    .save("Tabela Iceberg")


  
