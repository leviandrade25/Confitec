## Python 3.10

1. Consumo de informações _entre_ SEGBR/Mobile e Medallia (ferramenta NPS).
    - Levantamento de CPF's e respectivas propostas ativas dos produtos participantes do app Mobile

2. Seleção de CPF's e respectivas propostas ativas dos produtos participantes do app Mobile (tabela unicamente do Mobile)
    - Ainda não implementado: regras de negócio para quais banners devem ser exibidos para cada CPF.
    - No máximo, geração de parquet.
        - Campos: `cpf_cnpj`, `id_banner_i`, seja `i` dentro do intervalo dos naturais não nulos.
    - Criação de esteira no Jenkins.

3. Leitura do Parquet ou Leitura direto do datalake.* (a ser avaliado durante a construção da arquitetura)
    - Escrita dos dados na coleção `banner_usuario`.
    - `cpfCnpj`, `banners` [id, nome, ordemExibicao, `<campos da banner_dominio>`]** (a ser avaliado durante a modelagem da coleção).
    - buscar as informações na coleção `banner_dominio` (indexador) conforme os `ids de banner` vindos do parquet para _gravar junto_ na `banner_usuario`.*** ( a ser avaliado durante a modelagem da coleção).
    - Criação de esteira no Jenkins.

* Squad ETL não cuida da carga de propostas do Ultron, seria o caso de envolver o time do Ultron para fazer o levantamento.
* item 2 não será desenvolvido por agora.
* pyspark.
