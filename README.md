# Airflow 

Repositório central para armazenamento, versionamento e organização de todas as **DAGs do Apache Airflow** que utilizo em projetos de **engenharia de dados** e **pipelines de automação**.

---

## Objetivo

Este repositório foi criado para:

- Centralizar todas as DAGs em um único lugar.  
- Manter **versionamento** e histórico de mudanças.  
- Facilitar a execução de DAGs **localmente via Docker**.  
- Servir como base para **boas práticas de desenvolvimento** em Airflow.  

---

## Stack utilizada

- [Apache Airflow 3.x](https://airflow.apache.org/)  
- [Python 3.11+](https://www.python.org/)  
- [Docker & Docker Compose](https://www.docker.com/)  

---

## Setup local

### 1. Clonar o repositório
```bash
git clone https://github.com/seu-usuario/Airflow.git
cd airflow
```

### 2. Criar estrutura inicial

```bash
mkdir -p dags logs plugins config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 3. Subir o ambiente com Docker Compose

```bash
docker compose up airflow-init   # Inicializa metadado + usuário admin
docker compose up -d             # Sobe o ambiente em background
```

> Acesse em: http://localhost:8080 <br>
> Login padrão: airflow / airflow

## Estrutura do projeto

airflow/
│
├── dags/               # Todas as DAGs (.py)
│   ├── exemplo_dag.py
│   └── ...
│
├── plugins/            # Plugins e hooks customizados
├── config/             # Arquivos de configuração adicionais
├── logs/               # Logs gerados pelo Airflow (ignorado no git)
│
├── docker-compose.yaml # Arquivo de orquestração do ambiente
├── requirements.txt    # Dependências adicionais (pip)
├── .env                # Configurações locais (ignorado no git)
└── README.md           # Este arquivo

## Exemplo de DAG

# dags/hello_world.py
```python
import pendulum
from airflow.sdk import dag, task

@dag(
    dag_id="hello_world",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def hello_world():
    @task
    def say_hello():
        print("Hello Airflow!")

    say_hello()

dag_obj = hello_world()
```

## Dependências extras

Para instalar libs adicionais em seus workers/scheduler:

- Edite o requirements.txt
- Reconstrua a imagem customizada:

```bash
docker build -t airflow-custom -f Dockerfile .
docker compose up -d --build
```

## Boas práticas

- Cada DAG deve estar em um arquivo separado dentro de dags/.
- Use tags para organizar seus pipelines.
- Evite colocar credenciais no código → use Connections e Variables no Airflow.
- Nomeie DAGs e tasks de forma clara e consistente.
- Teste DAGs localmente (airflow dags test) antes de commitar.

