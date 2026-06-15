# RetailRocket - рекомендации товаров

Рекомендательная система для интернет-магазина на основе данных RetailRocket (~2.7M событий).

## Структура репозитория

```
notebooks/
  01_eda.ipynb        - исследование данных
  02_modeling.ipynb   - обучение модели, метрики, сохранение артефактов
service/
  app.py              - FastAPI сервис
  Dockerfile
  requirements.txt
airflow/
  retrain_dag.py      - DAG еженедельного дообучения
scripts/
  start_mlflow.sh     - запуск MLflow tracking server
monitoring/
  metrics.md          - описание метрик мониторинга
requirements.txt
.env                  - секреты (не в репо)
```

## Постановка задачи и метрики

**Цель:** предсказать, какие товары пользователь добавит в корзину.

**Офлайн-метрики:** Recall@10, Precision@10, MAP@10. Оцениваются на hold-out (последнее addtocart/transaction событие каждого пользователя).

**Подход:** ALS (collaborative filtering на implicit-фидбеке) + i2i через косинусное сходство факторов. Холодный старт - топ популярных товаров.

**Данные:** события из events.csv: view (не используется, слишком шумный), addtocart (вес 2), transaction (вес 3).

## Установка

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Данные в папку `data/` (events.csv, item_properties_part1.csv, item_properties_part2.csv, category_tree.csv).

## MLflow

Для локальной разработки - SQLite бекенд. Для продакшена - PostgreSQL + S3.

```bash
bash scripts/start_mlflow.sh
```

Открыть: `http://localhost:5000`

По умолчанию скрипт запускает с SQLite (`mlflow.db`). Для PostgreSQL задайте переменные окружения в `.env`.

## Запуск ноутбуков

```bash
jupyter lab
```

Порядок: `01_eda.ipynb` -> `02_modeling.ipynb`. После `02_modeling.ipynb` модели сохраняются в `models/` и загружаются в S3.

## Сервис

**Локально:**

```bash
cd service
pip install -r requirements.txt
python app.py
```

**Docker:**

```bash
docker build -t recsys-service ./service
docker run -p 8000:8000 --env-file .env recsys-service
```

Эндпоинты:

- `GET /health` - статус сервиса
- `POST /events` - принять событие `{"visitor_id": 123, "item_id": 456, "event": "view"}`
- `GET /recommendations/{visitor_id}?k=10` - персональные рекомендации
- `GET /similar/{item_id}?k=10` - похожие товары

Swagger: `http://localhost:8000/docs`

Поле `source` в ответе `/recommendations`: `als` (персональные) или `top_popular` (холодный старт).

## Airflow (дообучение)

DAG `retailrocket_retrain` запускается еженедельно:

1. Скачивает свежие данные из S3
2. Переобучает ALS
3. Строит i2i
4. Логирует метрики в MLflow
5. Заливает обновленные модели обратно в S3

Запуск Airflow (standalone для разработки):

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow standalone
```

DAG файл: `airflow/retrain_dag.py`. Переменные окружения из `.env` должны быть видны процессам Airflow.
