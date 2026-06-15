# метрики мониторинга

## офлайн-метрики (считаются при обучении, логируются в MLflow)

| метрика | описание |
|---|---|
| `recall_at_10` | доля тестовых товаров в топ-10 рекомендаций |
| `precision_at_10` | точность в топ-10 |
| `map_at_10` | mean average precision@10 |

логируются при каждом запуске DAG в эксперимент `retailrocket_recsys`. если recall упал больше чем на 10% от базового - смотрим руками.

## сервисные метрики (из `app.py`)

| метрика | описание |
|---|---|
| `requests_total` | число запросов к сервису |
| `cold_start_ratio` | доля ответов из top_popular |
| `recommendation_latency_ms` | время ответа на /recommendations |
| `event_ingestion_total` | число принятых событий через POST /events |

можно подключить prometheus-fastapi-instrumentator:

```python
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)
```

## бизнес-метрики (считаются по логам)

- CTR рекомендаций - клики по рекомендованным товарам / показы
- конверсия из рекомендации в addtocart
- доля холодных пользователей

## дрейф данных

каждый запуск DAG логирует в MLflow:
- число строк в events.csv
- соотношение addtocart/transaction (резкий сдвиг - признак проблемы)
