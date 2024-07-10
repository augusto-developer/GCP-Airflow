# Primeiro Projetinho: GCP + BigQuery + Pipelines + ETL + Airflow

Este simples projeto demonstra o uso do Google Cloud Platform (GCP), BigQuery, Pipelines ETL e Apache Airflow. O objetivo é extrair dados de um banco de dados MySQL sobre adoção de cachorros (criado como exemplo), processá-los usando o GCP e o Airflow, e criar uma nova tabela com os dados dos doguinhos que atendem aos seguintes critérios:
- Sexo feminino
- Podem conviver com crianças
- São castrados

## Configurações do GCP

1. **Criar um Bucket**
   - Dentro deste bucket, crie duas pastas: uma para backup e outra (com o nome de sua preferência) para armazenar o CSV raiz.

2. **Armazenar o .csv**
   - Coloque o arquivo .csv na pasta criada para armazenar o CSV raiz.

3. **Criar uma Conta de Serviço**
   - Atribua as seguintes permissões de IAM:
     - Editor de Dados BigQuery
     - Usuário de Jobs BigQuery

4. **Configurar a Conta de Serviço no Airflow**
   - Gere a chave JSON para a conta de serviço e configure-a no Airflow (Connections).

## Próximos Passos

Este é apenas um projeto inicial. O próximo projeto será mais robusto e detalhado.

---
