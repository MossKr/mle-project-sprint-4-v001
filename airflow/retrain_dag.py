"""
DAG дообучения ALS-модели рекомендаций RetailRocket.
Запускается еженедельно: забирает данные из S3, переобучает модель,
сохраняет артефакты обратно в S3 и логирует метрики в MLflow.
"""
import os
import pickle
import logging

import numpy as np
import pandas as pd
import scipy.sparse as sp
import boto3
import mlflow
from implicit.als import AlternatingLeastSquares
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# настройки из env (в Airflow задаются через Admin → Variables или .env)
S3_BUCKET  = os.environ.get('S3_BUCKET_NAME', '')
AWS_KEY    = os.environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SEC    = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
DB_USER    = os.environ.get('DB_DESTINATION_USER', '')
DB_PASS    = os.environ.get('DB_DESTINATION_PASSWORD', '')
DB_HOST    = os.environ.get('DB_DESTINATION_HOST', '')
DB_PORT    = os.environ.get('DB_DESTINATION_PORT', '6432')
DB_NAME    = os.environ.get('DB_DESTINATION_NAME', '')

S3_ENDPOINT  = 'https://storage.yandexcloud.net'
DATA_PREFIX  = 'data/'
MODEL_PREFIX = 'models/'
TMP_DIR      = '/tmp/retailrocket_retrain'

ALS_PARAMS = dict(factors=64, iterations=15, regularization=0.01, random_state=42)
WEIGHTS    = {'addtocart': 2, 'transaction': 3}


def get_s3():
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SEC,
    )


@dag(
    dag_id='retailrocket_retrain',
    schedule_interval='@weekly',
    start_date=days_ago(1),
    catchup=False,
    tags=['recsys', 'retailrocket'],
)
def retrain_dag():

    @task
    def download_data():
        """Скачиваем events.csv из S3 в /tmp."""
        os.makedirs(TMP_DIR, exist_ok=True)
        s3 = get_s3()
        local = os.path.join(TMP_DIR, 'events.csv')
        s3.download_file(S3_BUCKET, f'{DATA_PREFIX}events.csv', local)
        log.info('downloaded events.csv')
        return local

    @task
    def train_model(events_path: str):
        """Обучаем ALS, строим i2i, возвращаем путь к артефактам."""
        events = pd.read_csv(events_path)
        events['timestamp'] = pd.to_datetime(events['timestamp'], unit='ms')

        pos = events[events['event'].isin(WEIGHTS)].copy()
        pos['weight'] = pos['event'].map(WEIGHTS)
        pos = pos.sort_values('timestamp')

        user_cnt = pos.groupby('visitorid').size()
        active   = user_cnt[user_cnt >= 2].index
        pos      = pos[pos['visitorid'].isin(active)]

        user_ids  = pos['visitorid'].unique()
        item_ids  = pos['itemid'].unique()
        user2idx  = {u: i for i, u in enumerate(user_ids)}
        item2idx  = {it: i for i, it in enumerate(item_ids)}
        idx2item  = {i: it for it, i in item2idx.items()}

        pos['uidx'] = pos['visitorid'].map(user2idx)
        pos['iidx'] = pos['itemid'].map(item2idx)

        N_USERS = len(user_ids)
        N_ITEMS = len(item_ids)

        # train/test split
        test_idx  = pos.groupby('uidx')['timestamp'].idxmax()
        test      = pos.loc[test_idx]
        train     = pos.drop(index=test_idx)

        matrix = sp.csr_matrix(
            (train['weight'].values.astype(np.float32),
             (train['uidx'].values, train['iidx'].values)),
            shape=(N_USERS, N_ITEMS),
        )

        als = AlternatingLeastSquares(**ALS_PARAMS)
        als.fit(matrix)

        # метрики
        def recall_at_k(actual, recs, k=10):
            return len(set(actual) & set(recs[:k])) / len(set(actual)) if actual else 0.0

        recalls = []
        for uidx, grp in test.groupby('uidx'):
            actual = grp['iidx'].tolist()
            ids, _ = als.recommend(uidx, matrix[uidx], N=10, filter_already_liked_items=True)
            recalls.append(recall_at_k(actual, ids.tolist()))
        recall = float(np.mean(recalls))
        log.info('recall@10 = %.4f', recall)

        # i2i
        norms = np.linalg.norm(als.item_factors, axis=1, keepdims=True)
        norms[norms == 0] = 1
        normed = als.item_factors / norms
        similar_items = {}
        BATCH = 500
        for start in range(0, N_ITEMS, BATCH):
            end = min(start + BATCH, N_ITEMS)
            sims = normed[start:end] @ normed.T
            sims[:, start:end] -= np.eye(end - start, N_ITEMS - start) * 2
            top = np.argsort(-sims, axis=1)[:, :20]
            for i, iidx in enumerate(range(start, end)):
                similar_items[iidx] = top[i].tolist()

        # топ популярных
        pop_list = train.groupby('iidx')['weight'].sum().sort_values(ascending=False).index.tolist()

        # сохраняем артефакты
        artifacts = {
            'als_model.pkl':      als,
            'similar_items.pkl':  similar_items,
            'popular_items.pkl':  pop_list[:200],
            'mappings.pkl':       {'user2idx': user2idx, 'item2idx': item2idx, 'idx2item': idx2item},
        }
        os.makedirs(TMP_DIR, exist_ok=True)
        paths = {}
        for fname, obj in artifacts.items():
            p = os.path.join(TMP_DIR, fname)
            with open(p, 'wb') as f:
                pickle.dump(obj, f)
            paths[fname] = p

        # mlflow
        mlflow.set_tracking_uri(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        mlflow.set_experiment('retailrocket_recsys')
        with mlflow.start_run(run_name='als_retrain'):
            mlflow.log_params(ALS_PARAMS)
            mlflow.log_metric('recall_at_10', recall)
            for fname, p in paths.items():
                mlflow.log_artifact(p)

        return paths

    @task
    def upload_models(paths: dict):
        """Загружаем обновлённые модели в S3."""
        s3 = get_s3()
        for fname, local_path in paths.items():
            s3.upload_file(local_path, S3_BUCKET, f'{MODEL_PREFIX}{fname}')
            log.info('uploaded %s', fname)

    events_path = download_data()
    paths       = train_model(events_path)
    upload_models(paths)


dag_instance = retrain_dag()
