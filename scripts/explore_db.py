"""Печатает все таблицы public и их колонки, чтобы понять реальную схему."""
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, '.env'))

# какую базу смотреть: python explore_db.py source | dest  (по умолчанию dest)
which = sys.argv[1] if len(sys.argv) > 1 else 'dest'
pref = 'DB_SOURCE' if which == 'source' else 'DB_DESTINATION'
print(f'смотрю базу: {os.environ[pref + "_NAME"]} ({which})')

url = (
    f"postgresql+psycopg2://{os.environ[pref + '_USER']}:{os.environ[pref + '_PASSWORD']}"
    f"@{os.environ[pref + '_HOST']}:{os.environ[pref + '_PORT']}/{os.environ[pref + '_NAME']}"
)
engine = create_engine(url)

q = """
select table_name, column_name, data_type
from information_schema.columns
where table_schema = 'public'
order by table_name, ordinal_position
"""
df = pd.read_sql(text(q), engine)
for t, g in df.groupby('table_name'):
    cols = ', '.join(f'{r.column_name}:{r.data_type}' for r in g.itertuples())
    print(f'\n=== {t} ===')
    print(' ', cols)
