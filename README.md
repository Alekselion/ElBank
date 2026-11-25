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

В коде настройки сгруппированы по секциям (`settings.postgres.host`, `settings.jwt.secret`, ...),
имена переменных окружения при этом плоские (`POSTGRES_HOST`, `JWT_SECRET`, ...).

Host-порты контейнеров: `APP_PORT`, `POSTGRES_PORT`, `REDIS_HOST_PORT`, `KAFKA_HOST_PORT`,
`KAFKA_UI_HOST_PORT`, `WIREMOCK_HOST_PORT` — если стандартный порт занят, поменяйте его в `.env`
(не забудьте про соответствующий URL: `REDIS_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `WIREMOCK_BASE_URL`).

## Локальный запуск

Требуется [uv](https://docs.astral.sh/uv/). Python 3.15 uv установит сам.

```bash
uv sync                                   # создаёт .venv и ставит зависимости (вкл. dev)

# инфраструктура в Docker, приложение — на хосте
docker compose up -d postgres redis kafka kafka-ui wiremock elasticsearch logstash kibana kibana-setup

uv run alembic upgrade head               # миграции
uv run uvicorn elbank.main:app --reload   # http://localhost:8000
# или: uv run elbank  (host/port/reload берутся из .env)
```

Kafka с хоста доступна на `localhost:29092` (внутри сети compose — `kafka:9092`).

## Запуск через docker-compose

```bash
docker compose up -d --build
docker compose ps
```

Сервис `migrate` выполняет `alembic upgrade head` и завершается, после чего стартует `elbank-app`.
Сервис `kibana-setup` создаёт в Kibana data view `elbank-logs-*` и завершается.

Порты по умолчанию (переопределяются в `.env`):

| Сервис        | URL                          |
|---------------|------------------------------|
| API / Swagger | http://localhost:8000/docs   |
| Kafka UI      | http://localhost:8080        |
| WireMock      | http://localhost:8081        |
| Elasticsearch | http://localhost:9200        |
| Kibana        | http://localhost:5601        |
| Logstash TCP  | localhost:5000               |
| PostgreSQL    | localhost:5432               |
| Redis         | localhost:6379               |
| Kafka         | localhost:29092              |

Остановка: `docker compose down` (с удалением данных: `docker compose down -v`).

## Проверка

```bash
curl -s localhost:8000/health   # liveness: {"status":"ok","version":"0.1.0"}
curl -s localhost:8000/ready    # readiness: PG + Redis + Kafka; 503, если что-то недоступно
curl -s localhost:8081/ping     # WireMock
```

## Аутентификация

Эндпоинты — `/api/v1/auth/*` (см. Swagger). Ошибки: `{"detail": "...", "code": "..."}`.

- **Токены.** Access — JWT на 15 мин, refresh — JWT на 30 дней; в БД хранится только sha256 refresh-токена.
  `/refresh` ротирует пару; повторное использование уже отозванного refresh считается кражей
  и отзывает все сессии пользователя. `/logout` и `/reset-password` отзывают все refresh-токены
  и делают недействительными ранее выданные access-токены (метка в Redis).
- **OTP.** `/send-otp` → SMS через WireMock (`POST /sms/send`) → `/verify-otp` возвращает
  одноразовый `otp_token` (JWT, 5 мин, `otp_verified: true`), привязанный к телефону и `purpose`.
  Цели: `login` (вход при включённой 2FA), `reset`, `two_factor` (вкл./выкл. 2FA);
  коды `phone_change` выдаёт только `/users/me/change-phone/request`.
  Код живёт 300 с, сгорает после 5 неверных попыток; повторная отправка — не чаще раза в 60 с.
  Для незарегистрированного номера ответ тот же, но SMS не отправляется.
- **2FA.** При `is_2fa_enabled` `/login` требует `otp_token` с `purpose=login`, иначе 401 `otp_required`.
- **Rate limit.** Не более 5 попыток входа на телефон за 15 мин → 429 с `Retry-After`;
  счётчик блокировок — Redis-ключ `metrics:user.login_blocked`, событие в логах `user.login_blocked`.
- **Kafka.** Формат `{event_id, event_type, occurred_at, correlation_id, payload}`; топик = `event_type`.
  Топики `user.registered`, `user.deleted`, `profile.updated`, `notification.send`, `kyc.requested`,
  `kyc.decided` создаёт сервис `kafka-init`.

Код из SMS при локальной разработке виден в журнале WireMock: `GET localhost:8081/__admin/requests`.
Тестовые номера WireMock: `+79990000000` — SMS-шлюз отвечает 503, `+79990000001` — таймаут.

## Профиль

`/api/v1/users/me`: `GET`, `PATCH` (full_name, email, address; `null` очищает full_name/address),
`DELETE` (soft delete: `status=deleted`, все сессии отзываются, событие `user.deleted`).

- `POST /me/change-password` — проверяет старый пароль, отзывает все сессии и возвращает новую пару токенов.
- `POST /me/change-phone/request` `{new_phone}` → коды на старый и новый номер;
  `POST /me/change-phone/confirm` `{old_code, new_code}` — оба кода проверяются атомарно.
- `GET/PATCH /me/notification-settings` — каналы `email`, `sms`, `push`, `in_app` (JSONB, по умолчанию включены).

Изменения профиля публикуют `profile.updated` с изменёнными полями.

## KYC

`POST /api/v1/kyc/requests` (multipart: `passport_series`, `passport_number`, `birth_date`, `file` — JPEG
до 5 МБ). Скан сохраняется в `{KYC_UPLOADS_DIR}/{request_id}.jpg` (в compose — volume `kyc_uploads`
в `/app/uploads`), затем паспорт проверяется в Госуслугах (`POST /gosuslugi/verify-passport`,
таймаут 5 с, 2 повтора при таймауте/сетевой ошибке/5xx):

| Ответ Госуслуг              | Статус заявки                     | Событие       |
|-----------------------------|-----------------------------------|---------------|
| `valid: true`               | `approved`, ФИО пишется в профиль | `kyc.decided` |
| `valid: false`              | `rejected` + `reject_reason`      | `kyc.decided` |
| недоступен / мусор в ответе | `manual_review`                   | —             |

`kyc.requested` публикуется сразу после создания заявки. Одновременно может быть только одна
открытая заявка (`pending`/`manual_review`); после `approved` новые не принимаются.

Бэк-офис (роль `manager` или `admin`): `GET /api/v1/admin/kyc/requests?status=`,
`GET .../{id}/file` (скан), `POST .../{id}/approve`, `POST .../{id}/reject` `{reason}`.

Тестовые данные WireMock: серия `0000` или номер `000000` — паспорт недействителен,
серия `9999` — Госуслуги отвечают 503 (заявка уходит в `manual_review`), остальное — валиден.

## Логи

Все логи — JSON, по одному объекту на строку. Обязательные поля:
`timestamp`, `level`, `logger`, `message`, `correlation_id`, `service`, `env`.

- `correlation_id` берётся из заголовка `X-Correlation-Id` (или `X-Request-ID`), иначе генерируется;
  возвращается в ответе в `X-Correlation-Id`, передаётся во внешние HTTP-вызовы (WireMock)
  и в Kafka (поле envelope и заголовок записи `correlation_id`). Вне HTTP-запроса — `null`.
- Логи пишутся в stdout и асинхронно отправляются в Logstash (TCP 5000, `json_lines`) →
  Elasticsearch, индекс `elbank-logs-YYYY.MM.dd`. В Kibana: Discover → data view «ElBank logs».
- Если Logstash недоступен, приложение работает как обычно: события ждут в памяти
  и досылаются после восстановления (старше 5 минут — отбрасываются).
- `LOG_JSON=false` включает человекочитаемый вывод в консоль для локальной разработки.

## Миграции

```bash
uv run alembic revision --autogenerate -m "describe change"
uv run alembic upgrade head
uv run alembic downgrade -1
```

Новые модели импортируйте в `src/elbank/db/models/__init__.py`, иначе autogenerate их не увидит.

В контейнере: `docker compose exec elbank-app uv run alembic upgrade head`
(при `docker compose up` это делает сервис `migrate`).

Соглашения по моделям:
- `UUIDMixin` (`id`, генерируется и в Python, и `gen_random_uuid()` в БД),
  `TimestampMixin` (`created_at`, `updated_at`), `CreatedAtMixin` — для append-only таблиц.
- Enum'ы хранятся как `VARCHAR` + `CHECK` (`str_enum`), не нативные PG-типы.
- Деньги — `Numeric(18, 2)` (`Decimal`), ставки — `Numeric(7, 4)` в процентах.
- Связи объявлены с `lazy="raise"`: загружайте их явно (`selectinload`), неявного I/O нет.
- `get_session` не коммитит сам: транзакцию завершает сервисный слой (`await session.commit()`).

## Качество кода

```bash
uv run ruff format .
uv run ruff check .
uv run mypy
```
