import os
import pickle
import logging
from contextlib import asynccontextmanager
from typing import Optional

import boto3
import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

MODELS_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
S3_BUCKET  = os.environ.get('S3_BUCKET_NAME', '')
AWS_KEY    = os.environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SEC    = os.environ.get('AWS_SECRET_ACCESS_KEY', '')


def load_pickle(path: str):
    with open(path, 'rb') as f:
        return pickle.load(f)


def download_from_s3(fname: str):
    """Скачиваем файл из S3 если нет локально."""
    local = os.path.join(MODELS_DIR, fname)
    if os.path.exists(local):
        return local
    log.info('downloading %s from S3', fname)
    s3 = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SEC,
    )
    os.makedirs(MODELS_DIR, exist_ok=True)
    s3.download_file(S3_BUCKET, f'models/{fname}', local)
    return local


# глобальные артефакты
state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info('loading models...')
    for fname in ['als_model.pkl', 'similar_items.pkl', 'mappings.pkl', 'popular_items.pkl']:
        download_from_s3(fname)

    state['als']      = load_pickle(os.path.join(MODELS_DIR, 'als_model.pkl'))
    state['similar']  = load_pickle(os.path.join(MODELS_DIR, 'similar_items.pkl'))
    mappings          = load_pickle(os.path.join(MODELS_DIR, 'mappings.pkl'))
    state['user2idx'] = mappings['user2idx']
    state['item2idx'] = mappings['item2idx']
    state['idx2item'] = mappings['idx2item']
    state['popular']  = load_pickle(os.path.join(MODELS_DIR, 'popular_items.pkl'))

    # строим sparse матрицу нулевого размера — нужна для als.recommend
    import scipy.sparse as sp
    n_users = len(state['user2idx'])
    n_items = len(state['item2idx'])
    state['empty_row'] = sp.csr_matrix((1, n_items), dtype=np.float32)
    state['user_item'] = sp.csr_matrix((n_users, n_items), dtype=np.float32)

    log.info('models loaded')
    yield
    state.clear()


app = FastAPI(title='RetailRocket Recommendations', lifespan=lifespan)


class EventIn(BaseModel):
    visitor_id: int
    item_id: int
    event: str = 'view'


@app.get('/health')
def health():
    return {'status': 'ok', 'models_loaded': bool(state)}


@app.post('/events')
def add_event(body: EventIn):
    """Принимаем событие (для логирования; онлайн-обновление матрицы не реализовано)."""
    log.info('event: visitor=%s item=%s type=%s', body.visitor_id, body.item_id, body.event)
    return {'status': 'ok'}


@app.get('/recommendations/{visitor_id}')
def recommendations(visitor_id: int, k: int = 10):
    """Персональные рекомендации. Если пользователь холодный — топ популярных."""
    als       = state['als']
    user2idx  = state['user2idx']
    idx2item  = state['idx2item']
    popular   = state['popular']
    user_item = state['user_item']

    if visitor_id not in user2idx:
        log.info('cold start for visitor %s', visitor_id)
        recs = [idx2item[i] for i in popular[:k]]
        return {'visitor_id': visitor_id, 'recommendations': recs, 'source': 'top_popular'}

    uidx = user2idx[visitor_id]
    ids, _ = als.recommend(uidx, user_item[uidx], N=k, filter_already_liked_items=True)
    recs = [idx2item[i] for i in ids.tolist()]
    return {'visitor_id': visitor_id, 'recommendations': recs, 'source': 'als'}


@app.get('/similar/{item_id}')
def similar_items(item_id: int, k: int = 10):
    """Похожие товары (i2i)."""
    item2idx = state['item2idx']
    idx2item = state['idx2item']
    similar  = state['similar']

    if item_id not in item2idx:
        raise HTTPException(status_code=404, detail='item not found')

    iidx = item2idx[item_id]
    sim_idx = similar.get(iidx, [])[:k]
    return {'item_id': item_id, 'similar_items': [idx2item[i] for i in sim_idx]}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
