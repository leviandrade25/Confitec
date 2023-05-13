# Confitec

Desafio 1

Criar diretórito para receber arquivo parquet
Transformar os campos "Premiere" e "dt_inclusao" de string para datetime
Ordenar os dados por ativos e gênero de forma decrescente, 0 = inativo e 1 = ativo, todps com número 1 devem aparecer primeiro
Remover linhas duplicadas e trocar o resultado das linhas que tiverem a coluna "Seasons" de "TBA" para "a ser anunciado"
Criar uma coluna nova chamada "Data de Alteração" e dentro dela um timestamp
Trocas os nomes das colunas de ingles para português, exemplo: "Title" para "Título" (com acentuação)
Testar e verificar se existe algum erro de processamento do spark e identificar onde pode ter ocorrido o erro
Criar apensa 1 .csv com as seguintes colunas que foram nomeadas anteriormente "Title, Genre, Seasons, Premmiere, Language, Active, Status, dt_inclusao, Data de Alteraçã as colunas devem estar em português com header e separadas por ";"
Inserir esse .csv dentro de um bucket do AWS S3
Subir o codigo no github com o nome TESTEPYSPARK-Confitec
OBS: 
  O script  está na branch testepyspark
  O mesmo foi escrito em ambiente databricks.
  Dependendo de onde seja executado pode requerer a instalação das bibliotecas usadas para a solução.


TESTE-Confitec

Desafio 2 Confitec

Crie um algoritmo de multiplicação de matriz quadrada. O resultado do programa deverá apresentar os valores da Matriz A, Matriz B e o Produto.
Utilize a linguagem que possui maior familiaridade e facilidade;
O programa deverá instanciar uma Matriz A e Matriz B com números aleatórios;
O Output do programa deverá conter os valores da Matriz A, Matriz B e Produto
Subir o código no github com o nome TESTEPYSPARK-Confitec
OBS:
  O script está na branch algoritimo
  O mesmo foi executado em ambiente databricks e pode requerer instalação de biblioteca numpy dependendo de onde seja executado.
