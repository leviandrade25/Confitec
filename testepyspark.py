#imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from langdetect import detect
from translate import Translator
import re

df_netflix = spark.read.parquet("dbfs:/FileStore/originaisNetflix/*.parquet")
#Mostrando o conteudo do df
df_netflix.show(1,truncate=False,vertical=True)


#Transformando os campos "Premiere" e "dt_inclusao" de string para datetime
df_netflix = df_netflix.withColumn("Premiere",to_date(col("Premiere"),"d-MMM-yy"))\
                       .withColumn("dt_inclusao",to_timestamp(col("dt_inclusao")))


#Confirmando transformação
df_netflix.printSchema()
df_netflix.show(1,truncate=False,vertical=True)

#Ordenando os dados por ativos e gênero de forma decrescente, 0 = inativo e 1 = ativo, todps com número 1 devem aparecer primeiro
df_ordered = df_netflix.sort(col("active").desc(),col("genre").desc())
df_ordered.show(1,truncate=False,vertical=True)


#Removendo linhas duplicadas e trocar o resultado das linhas que tiverem a coluna "Seasons" de "TBA" para "a ser anunciado"
df_drop_duplicastes = df_ordered.withColumn("Seasons",when(col("Seasons")=="TBA",lit("a ser anunciado")).otherwise(col("Seasons"))).dropDuplicates()
display(df_drop_duplicastes)
print("total df_ordered ", df_ordered.count())
print("total df_drop_duplicastes", df_drop_duplicastes.count())


#Criando uma coluna nova chamada "Data de Alteração" e dentro dela um timestamp
df_with_new_coluns = df_drop_duplicastes.withColumn("DataDeAlteração",current_timestamp())
df_with_new_coluns.show(truncate=False,vertical=True)


#Trocando os nomes das colunas de ingles para português, exemplo: "Title" para "Título" (com acentuação)
#funcao para traduzir a coluna
def traduzi_colunas(coluna):
    translator = Translator(to_lang='pt', from_land='en')
    traduzido = translator.translate(coluna)
    return traduzido
  
  
  #aplicando
mew_columns = [] 
colunas_originais = []

#Ponto utilizado para retirar manter camel case e colocar um espaço entre eles para não atrapalhar tradução da coluna
colunas_originais = [re.sub(r'(?<!^)(?=[A-Z])', ' ', coluna).strip().title() for coluna in df_with_new_coluns.columns]

#Ponto utilizado para extrair colunas na forma original sem perder o camelCase
colunas_para_rename = [re.sub(r'\s+', '', coluna) for coluna in df_with_new_coluns.columns]

#traduzindo colunas 
for i in colunas_originais:
    if i == "Premiere":
        mew_columns.append("Pré Estréia")
    else:
        mew_columns.append(traduzi_colunas(i))
    

final_df = df_with_new_coluns.select("*")

#Contador para iterar nas colunas novas
colun = 0

#Renomeando colunas
for i in colunas_para_rename:
    final_df = new_df.withColumnRenamed(i,mew_columns[colun])
    colun = colun + 1

    
    
new_df.show(truncate=False,vertical=True)

#Para escrever em um S3 basta trocar "dbfs:/" por "s3://"
#Criar apensa 1 .csv com as seguintes colunas que foram nomeadas anteriormente "Title, Genre, Seasons, Premmiere, Language, Active, Status, dt_inclusao, Data de Alteraçã as colunas devem estar em português com header e separadas por ";"
#para Escrever no s3 deve-se trocar "dbfs:/" por "s3://" e o bucket para write
final_df = final_df.select("Título","Género","Épocas","Pré Estréia","Língua","Ativo","Status","dt_inclusao","Data De Alteração")
final_df.coalesce(1).write.mode("overwrite").option("header","true").option("sep",";").format("csv").save("dbfs:/FileStore/originaisNetflix/csv_out/")



spark.read.option("sep",";").option("header","true").csv("dbfs:/FileStore/originaisNetflix/csv_out/*.csv").show()


    
  
  
  
