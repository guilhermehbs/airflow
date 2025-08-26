import json
import csv
import os
import sqlite3
import logging
import pendulum
from urllib.request import urlopen, Request
from urllib.parse import urlencode

from airflow.sdk import dag, task
from airflow.sdk.decorators import task_group
from airflow.sdk.models.param import Param

TZ = pendulum.timezone('America/Sao_Paulo')
BASE_DIR = '/opt/airflow/data'
RAW_DIR = f'{BASE_DIR}/raw'
PROCESSED_DIR = f'{BASE_DIR}/processed'
DB_PATH = f'{BASE_DIR}/warehouse/example.db'

for d in (BASE_DIR, RAW_DIR, PROCESSED_DIR, os.path.dirname(DB_PATH)):
    os.makedirs(d, exist_ok=True)

LOGGER = logging.getLogger('airflow.task')

@dag(
    dag_id='EXAMPLE_ETL_LOCAL',
    description='DAG de exemplo: ETL local com arquivos + SQLite (stdlib)',
    start_date=pendulum.datetime(2025, 1, 1, tz=TZ),
    schedule='',
    catchup=False,
    max_active_runs=1,
    tags=['example', 'etl', 'template'],
    params={
        'limit': Param(20, type='integer', minimum=1, maximum=100, description='Qtde de registros para extrair'),
        'only_completed': Param(False, type='boolean', description='Filtrar por completed = true'),
    },
    doc_md=
    '''
        ### EXAMPLE_ETL_LOCAL
        Pipeline de exemplo que baixa um conjunto de TODOs públicos, transforma, valida e grava em SQLite.

        **Por que esta DAG é útil como template?**
        - Sem dependências extras (apenas stdlib).
        - Mostra boas práticas de Airflow 3 (TaskFlow, params, logging, TaskGroup, retries).
        - Escreve arquivos e um pequeno 'warehouse' SQLite para inspecionar facilmente.

        **Diretórios principais**
        - `/opt/airflow/data/raw` – dumps JSON brutos por data de execução.
        - `/opt/airflow/data/processed` – CSVs transformados.
        - `/opt/airflow/data/warehouse/example.db` – banco SQLite com a tabela `todos`.

        > Dica: monte `./data:/opt/airflow/data` no seu docker-compose para visualizar os artefatos no host.
    '''
)

def example_etl_local():
    @task(
        retries=2,
        retry_delay=pendulum.duration(minutes=1),
    )
    def extract(limit: int) -> str:
        base_url = 'https://jsonplaceholder.typicode.com/todos'
        q = {'_limit': limit}
        url = f'{base_url}?{urlencode(q)}'
        LOGGER.info('Requesting URL: %s', url)

        req = Request(url, headers={'User-Agent': 'airflow-example/1.0'})
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode('utf-8'))

        ts = pendulum.now(TZ).format('YYYYMMDD_HHmmss')
        raw_path = os.path.join(RAW_DIR, f'todos_{ts}.json')
        with open(raw_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        LOGGER.info('Raw saved: %s (records=%d)', raw_path, len(data))
        return raw_path

    @task_group(group_id='transform')
    def transform_group(raw_file: str, only_completed: bool):
        @task
        def filter_and_flatten(raw_file: str, only_completed: bool) -> list[dict]:
            with open(raw_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            rows: list[dict] = []
            for item in data:
                if only_completed and not item.get('completed', False):
                    continue
                rows.append(
                    {
                        'todo_id': int(item['id']),
                        'user_id': int(item['userId']),
                        'title': str(item['title']).strip(),
                        'completed': bool(item['completed']),
                    }
                )

            LOGGER.info('Filtered rows: %d (only_completed=%s)', len(rows), only_completed)
            return rows

        @task
        def save_csv(rows: list[dict]) -> str:
            ts = pendulum.now(TZ).format('YYYYMMDD_HHmmss')
            out_path = os.path.join(PROCESSED_DIR, f'todos_processed_{ts}.csv')

            if not rows:
                # Ainda salva um CSV vazio com cabeçalho para previsibilidade
                headers = ['todo_id', 'user_id', 'title', 'completed']
                with open(out_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
            else:
                headers = list(rows[0].keys())
                with open(out_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(rows)

            LOGGER.info('Processed CSV saved: %s', out_path)
            return out_path

        processed_csv = save_csv(filter_and_flatten(raw_file, only_completed))
        return processed_csv

    @task
    def quality_check(processed_csv: str) -> dict:
        count = 0
        ids = set()
        with open(processed_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                count += 1
                if row.get('todo_id'):
                    ids.add(row['todo_id'])

        has_rows = count > 0
        unique_ids = len(ids) == count

        LOGGER.info('QC -> rows=%d | has_rows=%s | unique_ids=%s', count, has_rows, unique_ids)

        if not has_rows:
            raise ValueError('Quality check failed: No rows found in processed CSV.')

        return {'rows': count, 'unique_ids': unique_ids}

    @task
    def load_to_sqlite(processed_csv: str, qc: dict) -> str:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute(
            '''
            CREATE TABLE IF NOT EXISTS todos (
                todo_id     INTEGER PRIMARY KEY,
                user_id     INTEGER NOT NULL,
                title       TEXT NOT NULL,
                completed   INTEGER NOT NULL,
                load_ts     TEXT NOT NULL
            )
            '''
        )

        with open(processed_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        load_ts = pendulum.now(TZ).to_iso8601_string()
        for r in rows:
            cur.execute(
                '''
                INSERT INTO todos (todo_id, user_id, title, completed, load_ts)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(todo_id) DO UPDATE SET
                    user_id=excluded.user_id,
                    title=excluded.title,
                    completed=excluded.completed,
                    load_ts=excluded.load_ts
                ''',
                (
                    int(r['todo_id']),
                    int(r['user_id']),
                    r['title'],
                    1 if str(r['completed']).lower() in {'1', 'true', 't', 'yes'} else 0,
                    load_ts,
                ),
            )

        conn.commit()
        conn.close()
        LOGGER.info('Loaded %d rows into SQLite at %s', len(rows), DB_PATH)
        return DB_PATH

    raw_path = extract(limit='{{ params.limit }}')
    processed_path = transform_group(raw_path, only_completed='{{ params.only_completed }}')
    qc_result = quality_check(processed_path)
    load_to_sqlite(processed_path, qc_result)


dag_obj = example_etl_local()
