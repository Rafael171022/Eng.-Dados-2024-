# Projeto de Engenharia de dados

## PySpark e Jupyter

### Antes de começar a trabalhar no nosso jupyter-nootbook vamos precisas instância-lo pelo docker da seguinte forma:
### Baixar a imagem oficial do Jupyter-notebook
  - docker pull jupyter/scipy-notebook

### Criar e executar um container com base na imagem, assim poderemos acessar o Jupyter Notebook no navegador usando o endereço http://localhost:8888.
  - docker run -p 8888:8888 my-jupyter-notebook

### Agora que conseguimos rodar o jupyter-notebook precisamos seguir esse passo a passo:
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
