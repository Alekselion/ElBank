# ElBank

Эмулятор электронного банка (pet-проект).

**Стек:** Python 3.15, uv, FastAPI, SQLAlchemy 2 (async) + asyncpg, Alembic, Pydantic v2,
Redis, Kafka (aiokafka), httpx, WireMock, ELK, PostgreSQL, Docker.

## Структура

```
src/elbank/
  main.py          # FastAPI-приложение, lifespan (PG, Redis, Kafka)
  __main__.py      # python -m elbank / консольная команда `elbank`
  deps.py          # FastAPI-зависимости (сессия БД, Redis, Kafka)
  api/routes/      # HTTP-роуты
  core/            # конфиг, логирование (structlog -> Logstash), JWT/пароли
  db/              # Base, engine/session, ORM-модели
  schemas/         # Pydantic-схемы
  services/        # бизнес-логика
  clients/         # клиенты внешних API (httpx, мокаются WireMock)
  kafka/           # продюсеры/консьюмеры
alembic/           # миграции
wiremock/          # маппинги и файлы ответов WireMock
logstash/pipeline/ # пайплайн Logstash
```

## Конфигурация

Все настройки — через переменные окружения / `.env` (см. `.env.example`):

```bash
cp .env.example .env
# Задайте POSTGRES_PASSWORD и JWT_SECRET (не короче 32 символов):
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

В `.env` хосты указаны для запуска приложения на хосте (`localhost`).
В docker-compose они переопределяются на имена сервисов (`postgres`, `redis`, `kafka:9092`, ...).

## Локальный запуск

Требуется [uv](https://docs.astral.sh/uv/). Python 3.15 uv установит сам.

```bash
uv sync                                   # создаёт .venv и ставит зависимости (вкл. dev)

# инфраструктура в Docker, приложение — на хосте
docker compose up -d postgres redis kafka wiremock elasticsearch logstash kibana

uv run alembic upgrade head               # миграции
uv run uvicorn elbank.main:app --reload   # http://localhost:8000
# или: uv run elbank  (host/port/reload берутся из .env)
```

Kafka с хоста доступна на `localhost:9094` (внутри сети compose — `kafka:9092`).

## Запуск через docker-compose

```bash
docker compose up -d --build
docker compose ps
```

Сервис `migrate` выполняет `alembic upgrade head` и завершается, после чего стартует `app`.

| Сервис        | URL                          |
|---------------|------------------------------|
| API / Swagger | http://localhost:8000/docs   |
| WireMock      | http://localhost:8080        |
| Elasticsearch | http://localhost:9200        |
| Kibana        | http://localhost:5601        |
| PostgreSQL    | localhost:5432               |
| Redis         | localhost:6379               |
| Kafka         | localhost:9094               |

Остановка: `docker compose down` (с удалением данных: `docker compose down -v`).

## Проверка

```bash
curl -s localhost:8000/health   # liveness: {"status":"ok","version":"0.1.0"}
curl -s localhost:8000/ready    # readiness: PG + Redis + Kafka; 503, если что-то недоступно
curl -s localhost:8080/ping     # WireMock
```

Логи приложения уходят в Logstash → Elasticsearch, индекс `elbank-YYYY.MM.dd`.
В Kibana создайте data view с шаблоном `elbank-*`.

## Миграции

```bash
uv run alembic revision --autogenerate -m "describe change"
uv run alembic upgrade head
uv run alembic downgrade -1
```

Новые модели импортируйте в `src/elbank/db/models/__init__.py`, иначе autogenerate их не увидит.

## Качество кода

```bash
uv run ruff format .
uv run ruff check .
uv run mypy
```
