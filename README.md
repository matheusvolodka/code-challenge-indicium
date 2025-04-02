# Relatório do Code Challenge - Flashlight Indicium
### Matheus Volodka Osorio

## Introdução
O presente relatório descreve o desenvolvimento do code challenge proposto pela Flashlight Indicium. O objetivo do desafio foi extrair dados de duas fontes diferentes (um arquivo CSV e uma base de dados PostgreSQL da Northwind), armazená-los em diretórios organizados por data no ambiente local e carregá-los em um banco de dados PostgreSQL fornecido.

## Ferramentas Utilizadas
- **Python**: gerenciamento do ambiente virtual e organização de diretórios.  
- **Docker e Docker Compose**: provisionamento do PostgreSQL.  
- **Apache Airflow**: orquestração das tarefas de extração, transformação e carga.  
- **Psycopg2**: integração com PostgreSQL.  
- **Meltano**: extração de dados das fontes (CSV e PostgreSQL) e carregamento para armazenamento local e banco de dados.  

As principais fontes de pesquisa utilizadas foram a [documentação do Meltano](https://hub.meltano.com/) e a documentação do Apache Airflow, além de conhecimentos adquiridos previamente por meio de estudos pessoais.

## Desenvolvimento

### Configuração Inicial
O projeto iniciou com a clonação do repositório via `git clone`. Para um melhor aprendizado, optou-se por começar pela ferramenta com menos familiaridade, o Meltano. Assim, foi criada uma instância de ambiente virtual Python (`venv`) e instalados o Meltano e o Psycopg2.  

A primeira etapa consistiu na extração dos dados do arquivo CSV, utilizando o plug-in `tap-csv`, e armazenamento local na pasta `ext_data`, criada automaticamente pelo Meltano.

### Extração de Dados do PostgreSQL
- Iniciado o container do PostgreSQL com `docker-compose`.  
- Testada a conexão com o banco Northwind via `psql`.  
- Adicionados os plug-ins `tap-postgres` (extração do banco) e `target-csv` (salvar os dados extraídos em CSV).  
- Configurado um segundo `target-csv` para separar os arquivos extraídos do PostgreSQL dos CSVs originais.

### Carregamento dos Dados para o PostgreSQL
- Criada nova configuração no Meltano com `tap-csv--2` e `target-postgres`.  
- Arquivos CSV armazenados foram lidos e carregados para a base PostgreSQL fornecida.  
- Após validação do pipeline do Meltano, iniciou-se a orquestração com o Apache Airflow.

### Orquestração com Apache Airflow
- **Problema**: conflito de dependências entre Meltano e Apache Airflow.  
- **Solução**: instalação do Meltano via `pipx` no sistema Ubuntu para isolamento, mantendo o Airflow no ambiente virtual Python.  

Criada uma **DAG no Airflow** para orquestrar:  
1. **Extração de dados**: do CSV e do banco Northwind para armazenamento local.  
2. **Carregamento de dados**: leitura dos arquivos CSV e carga no PostgreSQL.  
3. **Renomeação do diretório**: inclusão da data de extração no nome (ex: `northwind_data_2025-04-01`).  

Esse fluxo garante que a cada execução da DAG um novo diretório seja criado para armazenar os dados sem sobrescrever os anteriores.

## Desafios e Soluções
- **Conflito de dependências entre Meltano e Apache Airflow**  
  - *Solução*: Instalação do Meltano via `pipx`.  
- **Separação dos arquivos CSV extraídos do PostgreSQL dos originais**  
  - *Solução*: Configuração de dois `target-csv` distintos.  
- **Falha na renomeação de diretórios via Meltano**  
  - *Solução*: Implementação de uma função Python com `os` e `datetime`.  
## Conclusão
O desafio foi concluído com sucesso, permitindo a extração de dados de diferentes fontes, armazenamento local estruturado e carregamento eficiente para um banco de dados PostgreSQL. O Apache Airflow garantiu a orquestração automatizada do processo, consolidando um pipeline de ETL funcional.

O aprendizado sobre Meltano e integração de ferramentas foi um dos principais ganhos do projeto, além da experiência na resolução de conflitos de dependências e na automação do fluxo de trabalho.
## Preparação de Ambiente

### Criação do ambiente  
```sh
python3 -m venv airflow_venv
pip install -r venv_requirements.txt
export AIRFLOW_HOME=$(pwd)/airflow
meltano init northwind_meltano


