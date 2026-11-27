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
  workers/         # фоновые процессы (Kafka-консьюмеры), напр. генерация выписок
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

Сервис `migrate` выполняет `alembic upgrade head` и завершается, после чего стартуют `elbank-app`,
`statement-worker` (консьюмер `statement.requested`) и `transfers-worker` (завершение переводов,
разбор таймаутов внешних переводов и платежей).
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

## Счета

`/api/v1/accounts` — открыть счёт может только пользователь с одобренным KYC
(иначе `403`, `code: KYC_REQUIRED`). Тип и валюта: `checking`/`savings` — только `RUB`,
`foreign` — `USD`/`EUR`/`CNY`. Все денежные суммы в JSON — строки (`"1500.00"`).

| Метод и путь                          | Назначение                                                  |
|---------------------------------------|-------------------------------------------------------------|
| `POST /`                              | открыть счёт `{type, currency}` → `account.opened`          |
| `GET /`                               | свои счета (кэш Redis `accounts:{user_id}`, 30 с)            |
| `GET /{id}`                           | карточка счёта (чужой — `404`)                              |
| `GET /{id}/requisites`                | реквизиты: номер, БИК, корсчёт, ИНН/КПП банка, SWIFT, IBAN   |
| `DELETE /{id}`                        | закрыть: `active`, баланс 0, нет незавершённых операций → `account.closed` |
| `POST /{id}/block`, `/unblock`        | `active` ⇄ `frozen`, только `manager`/`admin`               |
| `GET`/`PUT /{id}/limits`              | `daily_limit`/`monthly_limit` (по умолчанию из `ACCOUNTS_DEFAULT_*`) |
| `GET /{id}/statement?from=&to=`       | заказать выписку CSV → `202` + `job_id`                     |
| `GET /statements/{job_id}`            | статус задачи (`pending`/`processing`/`ready`/`failed`) и `download_url` |
| `GET /statements/{job_id}/file`       | скачать CSV                                                 |

Номер счёта (20 цифр) строится по правилам Банка России: балансовый счёт (`40817` текущий,
`42301` до востребования), код валюты (`810` для рубля), контрольный ключ по БИК банка,
код подразделения (`BANK_BRANCH_CODE`) и 7 случайных цифр. Корсчёт выводится из БИК тем же
алгоритмом, IBAN — псевдо (`RU` + mod-97 + БИК + номер счёта). Реквизиты банка — `BANK_*`.

Кэш списка сбрасывается после открытия, закрытия, блокировки и разблокировки счёта. Недоступность
Redis не ломает `GET /` — ответ берётся из БД.

Выписка: задача хранится в Redis (`statement_job:{job_id}`, TTL `STATEMENTS_JOB_TTL_SECONDS`),
публикуется `statement.requested`. Воркер `elbank-statements-worker` (сервис `statement-worker`)
пишет `{STATEMENTS_DIR}/{job_id}.csv` (в compose — `/app/uploads/statements`, общий volume с
`elbank-app`) и раз в час удаляет файлы старше TTL. Период — целые дни UTC, не больше
`STATEMENTS_MAX_PERIOD_DAYS`, `to` не в будущем. Если Kafka недоступна, задача не создаётся (`503`).

Локально воркеры запускаются отдельно: `uv run elbank-statements-worker`, `uv run elbank-transfers-worker`.

## Переводы

`/api/v1/transfers`:

| Метод и путь                  | Назначение                                                        |
|-------------------------------|-------------------------------------------------------------------|
| `POST /internal`              | на счёт этого банка: `{from_account_id, to_account_id, amount, currency, description, idempotency_key}` |
| `POST /external`              | в другой банк: `{from_account_id, bic, account_number, recipient_name, amount, description, idempotency_key}` |
| `POST /sbp`                   | по телефону (СБП): `{from_account_id, phone, amount, description}` + заголовок `Idempotency-Key` |
| `POST /convert`               | обмен валют между своими счетами: `{from_account_id, to_account_id, amount, idempotency_key}` |
| `GET /rates?base=RUB&quote=USD` | курс ЦБ (кэш Redis `rates:{base}:{quote}`, 60 с)                |
| `GET /{operation_id}`         | статус перевода любого типа (`type`: internal/external/sbp/conversion) |
| `POST /{operation_id}/cancel` | отмена внутреннего `pending`-перевода в окне отмены → `transfer.reversed` |

### Внутренние переводы

Жизненный цикл:

1. `POST /internal` в одной транзакции блокирует оба счёта (`SELECT ... FOR UPDATE` в порядке id),
   проверяет статусы, баланс (`amount + комиссия`) и лимиты, списывает `amount + комиссия`, зачисляет
   `amount` и создаёт три операции в статусе `pending`: `transfer_out` (это и есть `operation_id`),
   `transfer_in` и `fee`. Комиссия — 0,5 %, минимум 10, максимум 5000 (`TRANSFERS_FEE_*`).
2. `TRANSFERS_CANCEL_WINDOW_SECONDS` (60 с) перевод можно отменить: создаются обратные операции
   (`reversed_operation_id`), деньги возвращаются, исходные операции становятся `cancelled`.
   Если получатель уже потратил деньги, будет `409`.
3. Затем `elbank-transfers-worker` переводит операции `pending → processing → completed` и публикует
   `transfer.completed`.

Если не прошла бизнес-проверка, перевод сохраняется со `status=failed` и `error_code`
(`ACCOUNT_NOT_ACTIVE`, `RECIPIENT_ACCOUNT_NOT_ACTIVE`, `INSUFFICIENT_FUNDS`, `DAILY_LIMIT_EXCEEDED`,
`MONTHLY_LIMIT_EXCEEDED`), публикуется `transfer.failed`. Ответ — `201`, повторить можно только с
новым ключом. Лимиты считаются по исходящим `transfer_out`/`payment` за текущие сутки/месяц UTC;
комиссия и отменённые переводы в лимит не входят.

Идемпотентность: `idempotency_key` уникален для пары (пользователь, ключ). Повтор с тем же ключом
возвращает сохранённую операцию с кодом `200`; тот же ключ с другими параметрами — `422
IDEMPOTENCY_KEY_REUSED`. Пока запрос обрабатывается, в Redis висит `op_lock:{user_id}:{key}`
(TTL 30 с). Параллельный запрос ждёт до `TRANSFERS_LOCK_WAIT_SECONDS`, а затем получает результат
или `409 OPERATION_IN_PROGRESS`. Если Redis недоступен, дубли всё равно отсекает уникальный индекс в БД.

Все смены статусов операций проходят через `services/operation_service.py` (state machine:
`created → pending → processing → completed`, `failed`/`cancelled` — из незавершённых статусов,
`processing → pending` — внешний шлюз не ответил). Идемпотентность общая для всех каналов
(`services/idempotency.py`).

### Внешние переводы и СБП

Только с рублёвых счетов, без комиссии, с проверкой статуса, баланса и лимитов. Номер счёта получателя
проверяется по контрольному ключу его БИК; БИК нашего банка отклоняется (для него есть `/internal`).

1. Деньги списываются, операция (`external_transfer` / `sbp_transfer`) переходит в `processing`, коммит.
2. Вызов шлюза (`clients/interbank.py`, `clients/sbp.py`): таймаут 10 с, заголовок
   `Idempotency-Key: <operation_id>`. Повтор (до 2 раз) — только при ошибке соединения и 502/503/504,
   после таймаута чтения повтора нет: исход неизвестен.
3. По ответу:

| Ответ шлюза         | Операция                                              | Событие              |
|---------------------|-------------------------------------------------------|----------------------|
| `accepted`          | `completed`, `external_id`                            | `transfer.completed` |
| `rejected`          | `failed`, `EXTERNAL_REJECTED`, деньги возвращаются    | `transfer.failed`    |
| ошибка / 4xx / 5xx  | `failed`, `EXTERNAL_UNAVAILABLE`, деньги возвращаются | `transfer.failed`    |
| таймаут             | `pending`, `EXTERNAL_TIMEOUT`                         | — (см. ниже)         |

Неподтверждённый перевод `transfers-worker` через `TRANSFERS_EXTERNAL_TIMEOUT_FAIL_AFTER_SECONDS` (5 с)
переводит в `failed`, возвращает деньги и публикует `transfer.failed`. Так же он закрывает переводы,
зависшие в `processing` дольше `TRANSFERS_EXTERNAL_STALE_AFTER_SECONDS` (процесс API упал во время вызова).
Воркер, а не `asyncio.create_task` в API, выбран потому, что задача не теряется при рестарте.

### Конвертация

Курс — кросс-курс ЦБ из `/rates` (8 знаков), комиссия 1 % (`TRANSFERS_CONVERSION_FEE_PERCENT`) в валюте
списания. Зачисляется `amount * rate` с округлением вниз до копеек. Всё в одной транзакции:
`conversion_out` + `conversion_in` + `fee` сразу `completed`, событие `transfer.completed` с
`type=conversion`. Лимиты не применяются.

### Тестовые данные WireMock

| Сценарий                    | Данные                                                       |
|-----------------------------|--------------------------------------------------------------|
| межбанк: принят             | БИК `044525225`, счёт `40817810800010000001`                 |
| межбанк: отклонён           | счёт получателя оканчивается на `9999999`, например `40817810600019999999` |
| СБП: принят                 | любой телефон `+7XXXXXXXXXX`                                 |
| СБП: таймаут (15 с)         | `+79990000002`                                               |
| курсы ЦБ                    | `GET /cbr/rates`: USD, EUR, CNY и др., дата — сегодня        |

## Оплата услуг

`/api/v1/payments`:

| Метод и путь                    | Назначение                                                          |
|---------------------------------|---------------------------------------------------------------------|
| `POST /`                        | `{account_id, provider, amount, payload, idempotency_key}`          |
| `GET /{id}`                     | статус платежа (`created → sent → completed / failed`)              |
| `GET /providers`                | справочник провайдеров: лимиты суммы и JSON-схема `payload`         |
| `GET/POST /templates`, `GET/PUT/DELETE /templates/{id}` | шаблоны: `{name, provider, payload, amount, recipient_account, description}` |

Провайдеры и `payload`:

| provider       | Шлюз (WireMock)               | payload                                        |
|----------------|-------------------------------|------------------------------------------------|
| `utility`      | `/providers/utility/pay`      | `personal_account`, `period` (YYYY-MM), `service` |
| `mobile`       | `/providers/mobile/pay`       | `phone` (+7XXXXXXXXXX)                         |
| `internet`     | `/providers/internet/pay`     | `contract_number`                              |
| `tax`          | `/providers/fns/pay`          | `inn` (12 цифр), `uin` (20/25 цифр)            |
| `traffic_fine` | `/providers/gibdd/pay`        | `uin` (начинается с 188)                       |
| `qr`           | `/providers/qr/pay`           | `qr` — строка ГОСТ Р 56042 (`ST0001…`)         |
| `receipt`      | `/providers/receipt/pay`      | `receipt_number`                               |

Платёж — это операция `payment` (списание; её статус и `error_code` главные) плюс `PaymentOrder`
(провайдер, payload, ответ провайдера с квитанцией в `provider_response`). Только с рублёвых счетов,
без комиссии, с проверкой статуса, баланса, лимитов и диапазона суммы провайдера. Схема как у внешних
переводов: списание и `processing` → вызов провайдера → результат:

| Ответ провайдера          | Платёж / операция                                  | Событие             |
|---------------------------|----------------------------------------------------|---------------------|
| 2xx `accepted`            | `completed`, `external_id` = `transaction_id`      | `payment.processed` |
| 4xx `error`               | `failed`, `EXTERNAL_REJECTED`, деньги возвращаются | `payment.failed`    |
| 5xx / мусор               | `failed`, `EXTERNAL_UNAVAILABLE`, возврат          | `payment.failed`    |
| таймаут                   | `sent` / `pending`, `EXTERNAL_TIMEOUT`             | через ~5 с воркер: `failed`, возврат, `payment.failed` |

Ретраи (tenacity): 5xx, таймаут и ошибка соединения — 2 повтора с экспоненциальной задержкой
(`PROVIDERS_RETRIES`, таймаут попытки `PROVIDERS_TIMEOUT_SECONDS`), 4xx — без повторов. Каждый запрос
несёт `Idempotency-Key: <payment id>`, поэтому повтор после таймаута не спишет деньги дважды.

Тестовые данные WireMock: остальные значения — успех.

| provider       | Отказ                                  | Таймаут (8 с)                        |
|----------------|----------------------------------------|--------------------------------------|
| `utility`      | `personal_account=000000` (404)        | `personal_account=999999`            |
| `mobile`       | `+79990000003`                         | `+79990000004`                       |
| `internet`     | `INET-000000`                          | `INET-999999`                        |
| `tax`          | `uin=00000000000000000000`             | `uin=99999999999999999999`           |
| `traffic_fine` | `uin=18800000000000000000` (404)       | `uin=18899999999999999999`           |
| `qr`           | `PersonalAcc=00000000000000000000`     | `PersonalAcc=99999999999999999999`   |
| `receipt`      | `0000000000`                           | `9999999999`                         |

Ответы лежат в `wiremock/__files/providers/` (шаблоны Handlebars: квитанция собирается из запроса).

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
