"""
Выгрузка таблиц проекта из playground_common в локальные parquet.
Запускать на ВМ, где видна managed-база.

    python scripts/export_data.py

Сам ищет таблицы по набору колонок, чтобы не угадывать имена руками.
"""
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(ROOT, '.env')
load_dotenv(env_path)

if 'DB_SOURCE_USER' not in os.environ:
    sys.exit(
        f'не вижу переменные из .env (искал {env_path}).\n'
        'создай файл .env в корне проекта с кредами DB_SOURCE_* и т.д.'
    )

DATA_DIR = os.path.join(ROOT, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

url = (
    f"postgresql+psycopg2://{os.environ['DB_SOURCE_USER']}:{os.environ['DB_SOURCE_PASSWORD']}"
    f"@{os.environ['DB_SOURCE_HOST']}:{os.environ['DB_SOURCE_PORT']}/{os.environ['DB_SOURCE_NAME']}"
)
engine = create_engine(url)

# что хотим найти: имя файла -> набор колонок, которые должны быть в таблице
wanted = {
    'events': {'visitorid', 'event', 'itemid'},
    'item_properties': {'itemid', 'property', 'value'},
    'category_tree': {'categoryid', 'parentid'},
}

# собираем колонки всех таблиц одним запросом
q = """
select table_name, column_name
from information_schema.columns
where table_schema = 'public'
"""
cols = pd.read_sql(text(q), engine)
by_table = cols.groupby('table_name')['column_name'].apply(set).to_dict()
print('таблиц в схеме public:', len(by_table))

for fname, need in wanted.items():
    match = [t for t, c in by_table.items() if need.issubset(c)]
    if not match:
        print(f'[!] не нашел таблицу под {fname} (колонки {need})')
        continue
    table = match[0]
    print(f'{fname} -> таблица {table}, выгружаю...')
    df = pd.read_sql(text(f'select * from {table}'), engine)
    out = os.path.join(DATA_DIR, f'{fname}.parquet')
    df.to_parquet(out, index=False)
    print(f'   {len(df)} строк -> {out}')

print('готово')
