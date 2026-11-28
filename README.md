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
# Задайте POSTGRES_PASSWORD, JWT_SECRET и CARDS_PAN_HASH_KEY (не короче 32 символов):
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
`statement-worker` (консьюмер `statement.requested`), `exports-worker` (консьюмер `export.requested`),
`operations-worker` (консьюмер `operation.completed`: кэш аналитики и бюджеты), `notifications-worker`
(консьюмер `notification.send`), `reconciliation-worker` (консьюмер `reconciliation.requested`),
`deposit-worker` и `transfers-worker` (завершение переводов, разбор таймаутов внешних переводов
и платежей). Сервис `kibana-setup` создаёт в Kibana data views `elbank-logs-*` и `elbank-audit-*`
и завершается.

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
Тестовые номера WireMock: `+79990000000` — SMS-шлюз отвечает 503, `+79990000001` — таймаут,
`+79990000011` — SMS-шлюз отвечает 500, `+79990000012` — таймаут push-шлюза.

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

Локально воркеры запускаются отдельно: `uv run elbank-statements-worker`, `uv run elbank-transfers-worker`,
`uv run elbank-deposits-worker`, `uv run elbank-exports-worker`, `uv run elbank-operations-worker`,
`uv run elbank-notifications-worker`.

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
   `transfer_in` и `fee`. Комиссия — по тарифу `internal_transfer` (по умолчанию 0,5 %, минимум 10,
   максимум 5000 — `TRANSFERS_FEE_*`; меняется через `PUT /api/v1/admin/tariffs`).
2. `TRANSFERS_CANCEL_WINDOW_SECONDS` (60 с) перевод можно отменить: создаются обратные операции
   (`reversed_operation_id`), деньги возвращаются, исходные операции становятся `cancelled`.
   Если получатель уже потратил деньги, будет `409`.
3. Затем `elbank-transfers-worker` переводит операции `pending → processing → completed` и публикует
   `transfer.completed`.

Если не прошла бизнес-проверка, перевод сохраняется со `status=failed` и `error_code`
(`ACCOUNT_NOT_ACTIVE`, `RECIPIENT_ACCOUNT_NOT_ACTIVE`, `INSUFFICIENT_FUNDS`, `DAILY_LIMIT_EXCEEDED`,
`MONTHLY_LIMIT_EXCEEDED`, `FRAUD_SUSPECTED` — сработало правило антифрода), публикуется `transfer.failed`. Ответ — `201`, повторить можно только с
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

Курс — из `/rates` (8 знаков): ручной курс администратора, если задан, иначе кросс-курс ЦБ.
Комиссия — по тарифу `conversion` (по умолчанию 1 %, `TRANSFERS_CONVERSION_FEE_PERCENT`) в валюте
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

## Карты

`/api/v1/cards` — карты к своим активным счетам (не больше `CARDS_MAX_PER_ACCOUNT` активных и
заблокированных на счёт).

| Метод и путь                     | Назначение                                                        |
|----------------------------------|-------------------------------------------------------------------|
| `POST /`                         | виртуальная карта `{account_id, type: "virtual"}` → `card.issued` |
| `POST /plastic`                  | пластиковая `{account_id, address: {postal_code, city, street, house, apartment}}` → `card.issued`; нужно `full_name` в профиле |
| `GET /`                          | свои карты (все статусы)                                          |
| `GET /{id}`                      | детали: лимиты, MCC-ограничения, адрес доставки                    |
| `POST /{id}/block`               | `active → blocked`, тело необязательно: `{reason: lost/stolen/compromised/other}` → `card.blocked` |
| `POST /{id}/unblock`             | `blocked → active` (счёт активен, срок не истёк) → `card.unblocked` |
| `POST /{id}/reissue`             | новая карта того же типа с теми же лимитами/MCC/адресом, старая — `reissued` → `card.issued` |
| `GET`/`PUT /{id}/limits`         | `{daily, monthly, per_transaction}` в валюте счёта, `null` — без лимита; `per_transaction ≤ daily ≤ monthly` |
| `PUT /{id}/mcc-restrictions`     | `{allow: [...], deny: [...]}` — 4-значные MCC; пустой `allow` — разрешено всё, кроме `deny` |

PAN приходит только от процессинга и в БД не попадает: хранятся маска (`220070******1234`) и
`pan_hash` = HMAC-SHA256(`CARDS_PAN_HASH_KEY`, PAN). По хэшу карта адресуется в процессинге:
`POST /card-processor/{pan_hash}/block|unblock`. Выпуск — `POST /card-processor/issue` с
`Idempotency-Key: <card id>`; при перевыпуске передаётся `replaces_pan_hash` старой карты.
Лимиты и MCC-ограничения хранятся в JSONB (`cards.limits`, `cards.mcc_restrictions`),
начальные лимиты — `CARDS_DEFAULT_*`.

Изменение статуса сначала уходит в процессинг и сохраняется только после его ответа.
Ретраи: 5xx, таймаут, ошибка соединения — `CARD_PROCESSOR_RETRIES` повторов; отказ процессинга
(4xx) → `422 CARD_PROCESSOR_REJECTED`, недоступность → `503`.

Тестовые данные WireMock (телефон владельца карты):

| Сценарий                         | Данные                     |
|----------------------------------|----------------------------|
| выпуск: успех                    | любой другой телефон       |
| выпуск: отказ `ISSUE_DECLINED`   | `+79990000005` → `422`     |
| выпуск: процессинг недоступен    | `+79990000006` → `503`     |

## Кредиты

`/api/v1/credits`. Деньги выдаются на рублёвый счёт клиента и списываются с него же (`account_id` в заявке).

| Метод и путь              | Назначение                                                              |
|---------------------------|-------------------------------------------------------------------------|
| `POST /applications`      | `{account_id, amount, term_months, purpose}` → скоринг → `approved` (`credit.approved`, SMS-код) или `rejected` (`credit.rejected`) |
| `GET /`, `GET /{id}`      | свои кредиты и заявки                                                   |
| `GET /{id}/schedule`      | аннуитетный график, `left_to_pay`                                       |
| `POST /{id}/sign/otp`     | отправить код подписания повторно (первый уходит при одобрении)          |
| `POST /{id}/sign`         | `{otp_code}` → `active`, сумма зачисляется на счёт → `credit.signed`     |
| `POST /{id}/repay`        | `{amount, early}` → при полном погашении `closed` → `credit.closed`      |

Скоринг — `POST {WM}/bki/score` (ретраи 5xx/таймаута, `Idempotency-Key` = id заявки). Одобрение:
`decision=approve` и `score >= CREDITS_MIN_SCORE`. Ставка — `CREDITS_GOOD_SCORE_RATE` при
`score >= CREDITS_GOOD_SCORE`, иначе `CREDITS_BASE_RATE`. Бюро недоступно → `503`, заявка не сохраняется.

График: платёж `P·r / (1 − (1 + r)^−n)`, `r` — месячная ставка; проценты месяца — остаток долга × `r`,
последний платёж добирает остаток округлений. При подписании даты пересчитываются от дня подписания.

Погашение:
- `early=false` — сумма гасит платежи по порядку, в каждом сначала проценты, потом долг; можно частично;
  не больше `left_to_pay`.
- `early=true` — сумма уменьшает основной долг (не больше `principal_outstanding`), оставшиеся платежи
  пересчитываются на те же даты с меньшим платежом; проценты за текущий период не начисляются.
  Если платёж оплачен частично, сначала нужно доплатить его обычным погашением (`409`).

Статус `overdue` пока никто не выставляет — нужен отдельный воркер просрочек.

## Вклады

`/api/v1/deposits`:

| Метод и путь          | Назначение                                                             |
|-----------------------|------------------------------------------------------------------------|
| `POST /`              | `{account_id, amount, term_months, rate}` — списание со счёта → `deposit.opened` |
| `GET /`               | свои вклады, `interest_to_date` — проценты на сегодня                  |
| `GET /{id}`           | детали + `accruals` (периоды начисления)                               |
| `POST /{id}/top-up`   | `{amount}` — пополнение со счёта вклада (до даты окончания)            |
| `POST /{id}/close`    | закрытие: тело и проценты на счёт → `deposit.closed`                   |

Ставку выбирает клиент в пределах `(0, DEPOSITS_MAX_RATE]`. Проценты простые, ежедневные: за каждый
день из `[start_date, end_date)` — `сумма × ставка / 100 / дней в году` (365 или 366). Начисленное
лежит во вкладе (`accrued_interest` до `accrued_until`, не включая), перед пополнением и закрытием
проценты доначисляются, так что каждый период в `accruals` — с одной суммой вклада. При закрытии до
`end_date` (`early_closed`) проценты пересчитываются по `DEPOSITS_EARLY_CLOSE_RATE`. На счёт уходят
две проводки: `deposit_close` (тело) и `interest` (проценты).

Периодическое начисление: `POST /api/v1/admin/deposits/accrue` (`manager`/`admin`, тело необязательно:
`{as_of}`, по умолчанию сегодня) публикует `deposit.interest.tick`, воркер `elbank-deposits-worker`
(сервис `deposit-worker`) начисляет проценты всех активных вкладов за дни до `as_of`. В проде тик
публикует cron. Повтор тика ничего не меняет; вклады берутся `FOR UPDATE SKIP LOCKED`.

## Инвестиции (упрощённо)

`/api/v1/investments`:

| Метод и путь      | Назначение                                                                  |
|-------------------|-----------------------------------------------------------------------------|
| `GET /stocks`     | котировки `GET {WM}/broker/quotes`                                          |
| `POST /orders`    | `{account_id, ticker, quantity, side: buy/sell}` → `POST {WM}/broker/orders` → `investment.order.created` |
| `GET /portfolio`  | позиции по средней цене покупки и по рынку, прибыль                         |

Заявка уходит брокеру лимитной: цена — текущая котировка, покупка не исполняется дороже, продажа —
дешевле. До отправки проверяются деньги на счёте (покупка) или бумаги в позиции (продажа). Счёт и
позиция заблокированы, пока брокер отвечает; заявка, проводка (`investment_buy`/`investment_sell`) и
позиция сохраняются вместе. Отказ брокера (4xx) сохраняется как заявка `rejected` без движения денег,
недоступность брокера → `503`, ничего не сохраняется.

### Тестовые данные WireMock (кредиты, инвестиции)

| Сценарий                                    | Данные                                   |
|---------------------------------------------|------------------------------------------|
| скоринг: одобрено, score 720 (базовая ставка) | любой другой телефон клиента           |
| скоринг: одобрено, score 810 (льготная ставка) | `+79990000010`                        |
| скоринг: отказ бюро, score 480              | `+79990000007`                           |
| скоринг: approve, но score 580 < порога     | `+79990000008` → `rejected`              |
| скоринг: бюро недоступно                    | `+79990000009` → `503`                   |
| брокер: исполнено по лимитной цене          | любая заявка до 10 000 бумаг             |
| брокер: отказ `INSUFFICIENT_LIQUIDITY`      | `quantity` > 10 000                      |

Котировки: SBER, GAZP, LKOH, YDEX, GMKN, MTSS, ROSN, VTBR (`wiremock/__files/broker/quotes.json`).

## История операций

`/api/v1/operations` — операции по всем счетам клиента, в любом статусе (включая компенсации):

| Метод и путь                       | Назначение                                                        |
|------------------------------------|-------------------------------------------------------------------|
| `GET /`                            | фильтры `from`, `to`, `account_id`, `type` или `type[]` (повторяемые), `status`, `amount_min`, `amount_max`; `page`, `size` (20, максимум 100); сортировка `created_at DESC` → `{items, total, page, size}` |
| `GET /{id}`                        | детали: провайдер платежа, `error_code`, `parent_operation_id`, `reversed_operation_id`, ... |
| `GET /export?format=csv\|pdf&from=&to=` | заказать экспорт → `202` + `job_id`                         |
| `GET /exports/{job_id}`            | статус задачи и `download_url`                                    |
| `GET /exports/{job_id}/file`       | скачать CSV/PDF                                                   |

Периоды — целые дни UTC. Экспорт устроен как выписка: задача в Redis (`export_job:{job_id}`, TTL
`EXPORTS_JOB_TTL_SECONDS`), событие `export.requested`, воркер `elbank-exports-worker` (сервис
`exports-worker`) пишет `{EXPORTS_DIR}/{job_id}.{csv|pdf}` (в compose — `/app/uploads/exports`) и раз в
час удаляет файлы старше TTL. PDF строится в памяти: больше `EXPORTS_PDF_MAX_ROWS` операций — задача
`failed` с подсказкой выбрать CSV. Для кириллицы нужен TTF-шрифт `EXPORTS_PDF_FONT_PATH` (в образе —
`fonts-dejavu-core`).

## Аналитика и бюджеты

Учитываются только проведённые деньги: операции `completed`, кроме компенсаций (отменённый перевод и
его компенсации взаимно исключаются). Суммы всегда в разрезе валют.

Категория расхода (`SpendingCategory`) — по типу операции, для платежей — по провайдеру:
`utility` → `utilities`, `mobile`, `internet`, `tax` → `taxes`, `traffic_fine` → `fines`,
`qr` → `qr_payments`, `receipt` → `bills`; переводы (внутренние, межбанковские, СБП) → `transfers`,
`fee` → `fees`, погашение кредита → `loans`, открытие/пополнение вклада → `savings`, покупка бумаг →
`investments`. Конвертации и переводы между своими счетами — не расход и не доход (их комиссии —
расход).

`/api/v1/analytics`:

| Метод и путь                          | Назначение                                                  |
|---------------------------------------|-------------------------------------------------------------|
| `GET /categories?from=&to=`           | расходы по категориям: сумма, число операций, доля в валюте |
| `GET /monthly?year=`                  | доходы/расходы/сальдо по 12 месяцам для каждой валюты       |
| `GET /balance-dynamics?from=&to=[&account_id=]` | по дням: приход, расход, сальдо, нарастающий итог (внутренние движения учитываются) |

Кэш: Redis-хэш `analytics:{user_id}`, поле на отчёт с параметрами, TTL поля
`ANALYTICS_CACHE_TTL_SECONDS` (300 с, `HEXPIRE`). Хэш удаляет `operations-worker` по
`operation.completed` любой операции на счетах клиента.

`/api/v1/budgets`: `POST /` `{category, monthly_limit, currency=RUB}`, `GET /`, `GET /{id}`,
`PATCH /{id}` `{monthly_limit}`, `DELETE /{id}`, `GET /status` — потрачено/лимит/остаток/процент и
уровень `ok`/`warn`/`danger` за текущий месяц UTC. Один бюджет на категорию и валюту.

Событие `operation.completed` публикуется для каждой операции, перешедшей в `completed`, после коммита
транзакции (хук сессии в `elbank.services.operation_events`). `operations-worker` пересчитывает бюджет
категории операции и шлёт `notification.send` с `level: warn` (от `BUDGETS_WARN_PERCENT`, 80 %) или
`danger` (от 100 %) — каждый уровень один раз за месяц (маркер `budget_alert:{budget_id}:{YYYY-MM}:{level}`
в Redis; смена лимита сбрасывает маркеры текущего месяца).

## Поддержка

`/api/v1/support/tickets` (клиент видит только свои тикеты — чужие отвечают `404`; manager и admin — все):

| Метод и путь                          | Назначение                                                    |
|---------------------------------------|---------------------------------------------------------------|
| `POST /`                              | `multipart/form-data`: `subject`, `category`, `priority` (`normal`), `body`, `attachments` (повторяемый) → тикет + первое сообщение, `ticket.created` |
| `GET /?status=&limit=&offset=`        | тикеты, свежая активность первой                              |
| `GET /{id}`                           | тикет с сообщениями и вложениями                              |
| `GET /{id}/attachments/{attachment_id}` | скачать вложение                                            |
| `POST /{id}/messages`                 | `{body}` → `ticket.updated`                                   |
| `POST /{id}/close`                    | закрыть: автор или manager/admin                              |
| `POST /{id}/rate`                     | `{score 1..5, comment}` — CSAT, один раз, только автор, тикет `resolved`/`closed` |
| `POST /{id}/status`                   | `{status}` — только manager/admin                             |

Категории: `accounts`, `cards`, `transfers`, `payments`, `credits`, `deposits`, `investments`,
`technical`, `other`. Вложения: до `SUPPORT_MAX_ATTACHMENTS` файлов по `SUPPORT_MAX_ATTACHMENT_BYTES`,
тип определяется по содержимому (JPEG, PNG, PDF; иначе `415`), имя файла очищается от пути и
спецсимволов. Файлы — `{SUPPORT_UPLOADS_DIR}/{attachment_id}`.

Статусы: `open → in_progress | closed`, `in_progress → waiting_client | resolved | closed`,
`waiting_client → in_progress | resolved | closed`, `resolved → in_progress | closed`; `closed` —
конечный. Взятие `open` в работу (сменой статуса или первым ответом сотрудника) назначает тикет на
этого менеджера (`assignee_id`). Ответ клиента в `waiting_client`/`resolved` возвращает тикет в
`in_progress`. Ответ сотрудника дополнительно отправляет клиенту `notification.send`
(`template: support.reply`).

`ticket.updated`: `{ticket_id, user_id, status, old_status, assignee_id, kind}` + `message` (`kind:
message`) или `rating` (`kind: rated`); `kind: status` — смена статуса и закрытие.

### WebSocket

`/ws/tickets/{ticket_id}?token=<access token>` — живые события `ticket.updated` этого тикета (JSON
как выше + `event_id`, `occurred_at`). Историю загружайте через `GET /{id}`; сообщения клиента в сокет
игнорируются (годятся как ping). Коды закрытия: `4401` — нет/неверный/отозванный токен или токен
истёк во время сессии, `4403` — пользователь заблокирован, `4404` — тикета нет или он чужой, `1013` —
клиент не успевает читать (переподключиться и перезагрузить тикет).

Fan-out: каждый процесс API держит один Kafka-консьюмер `ticket.updated` без consumer group (каждый
процесс видит все события, с последнего offset) и раскладывает события по `asyncio.Queue`
подписанных сокетов (`SUPPORT_WS_QUEUE_SIZE`). Без Kafka приложение работает, консьюмер
переподключается в фоне.

### FAQ

`GET /api/v1/support/faq[?category=]` — опубликованные статьи `FaqArticle`, без авторизации. Весь
список кэшируется в Redis (`faq:articles`, `SUPPORT_FAQ_CACHE_TTL_SECONDS` = 10 мин). Стартовые
статьи добавляет миграция `7c2d4e8a9f13`.

## Уведомления

Любой сервис публикует `notification.send` `{user_id, channel: sms|email|push|in_app, template, ...поля
шаблона}`. Воркер `elbank-notifications-worker` (сервис `notifications-worker`):

1. сохраняет `Notification` (`delivery_status: pending`) с `dedup_key = {event_id}:{user_id}`
   (уникальный индекс, `INSERT ... ON CONFLICT DO NOTHING`): повтор события не создаёт вторую запись;
2. не отправляет, а только сохраняет (`skipped` + `delivery_note`), если канал выключен в
   `users.notification_settings` (`channel_disabled`), канал `in_app` (`in_app`) или у пользователя нет
   e-mail (`no_recipient`). Событие с `provider_message_id` (OTP — SMS уже отправлено) записывается как
   `sent`. События без `user_id` (OTP при регистрации) не сохраняются;
3. иначе отправляет: `sms` → `POST {WM}/sms/send`, `email` → `/email/send`, `push` → `/push/send`
   (текст — по шаблону `template`). 5xx, таймаут и ошибка соединения повторяются
   `NOTIFICATIONS_RETRIES` (3) раза с экспоненциальной паузой, 4xx — нет; после этого `failed` и лог
   уровня `error`. Запросы несут `Idempotency-Key: <dedup_key>`: запись, оставшаяся `pending` после
   падения воркера, при повторе события отправляется снова без дубля у шлюза.

`/api/v1/notifications`:

| Метод и путь              | Назначение                                                         |
|---------------------------|--------------------------------------------------------------------|
| `GET /?unread=&limit=&offset=` | уведомления, новые первыми; `unread` — число непрочитанных    |
| `POST /{id}/read`         | отметить прочитанным (повтор ничего не меняет)                     |
| `POST /read-all`          | отметить все → `{updated}`                                         |
| `GET /settings`, `PATCH /settings` | каналы `email`, `sms`, `push`, `in_app` (то же, что `/users/me/notification-settings`) |

WireMock: `sms_ok.json`, `sms_fail.json` (500 для `+79990000011`), `email_ok.json`, `push_ok.json`,
`push_timeout.json` (задержка 10 с для `+79990000012`; push-шлюз находит устройства по телефону).

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
- Записи аудита (`core/audit.py`, поле `log_type=audit`) Logstash пишет в отдельный индекс
  `elbank-audit-YYYY.MM.dd` с шаблоном `logstash/templates/elbank-audit.json` (`action`, `actor_id`,
  `user_id`, `entity`, `entity_id` — keyword, `details` — flattened). Data view «ElBank audit».

## Админ-панель

Все эндпоинты — `/api/v1/admin/*`. Первого администратора назначает CLI (через API роль меняет
только admin):

```bash
uv run elbank-set-role --phone +79991234567 --role admin
# в compose: docker compose run --rm elbank-app elbank-set-role --phone +79991234567 --role admin
```

| Эндпоинт                                                  | Роли                                  |
|-----------------------------------------------------------|---------------------------------------|
| `GET /users?search=&status=&role=&page=&size=`            | manager, admin, auditor               |
| `POST /users/{id}/block` (`{reason}` необяз.), `/unblock` | manager (только клиентов), admin      |
| `POST /users/{id}/role` `{role}`                          | admin (свою роль менять нельзя)       |
| `GET /tariffs`, `GET /rates`, `GET /fraud-rules`          | manager, admin, auditor               |
| `PUT /tariffs`, `PUT`/`DELETE /rates/{pair}`              | admin                                 |
| `POST /fraud-rules`, `PUT`/`DELETE /fraud-rules/{id}`     | admin                                 |
| `GET /audit?user_id=&action=&from=&to=&page=&size=`       | auditor                               |
| `POST /reconciliation/run`, `GET /reconciliation/{run_id}`| admin, auditor                        |

- **Блокировка.** `status=blocked`, отзываются все refresh- и access-токены (пользователь выходит
  отовсюду), публикуется `user.blocked`; разблокировка — `user.unblocked`. Себя блокировать нельзя.
- **Тарифы.** Таблица `tariffs` (`code` → jsonb `{percent, min, max}`), коды `internal_transfer` и
  `conversion`. Нет строки — действует значение из env (`is_default: true`). Тариф читается при
  создании операции, поэтому изменение касается только новых операций.
  `PUT /tariffs` `{"tariffs": {"internal_transfer": {"percent": "0.3", "min": "5.00", "max": "3000.00"}, "conversion": null}}`
  — меняются только перечисленные; `null` возвращает значение по умолчанию.
- **Курсы.** `PUT /rates/USD-RUB` `{"rate": "92.5"}` сохраняет ручной курс в `rate_overrides`;
  обратная пара без своего override считается как `1 / rate`. Ключи Redis `rates:USD:RUB` и
  `rates:RUB:USD` удаляются сразу, поэтому `/transfers/rates` и конвертации видят новый курс со
  следующего запроса. `DELETE /rates/{pair}` возвращает курс ЦБ. `GET /rates` — действующие курсы всех
  пар (`source: cbr | override`) и список override.
- **Аудит.** Читается из Elasticsearch (`elbank-audit-*`), не из БД. `user_id` ищет и по исполнителю,
  и по субъекту; `action` — точное значение или префикс со `*` (`admin.user.*`); `from`/`to` — ISO 8601
  с часовым поясом. Пишутся: `auth.login`, `auth.login_failed`, `admin.user.block|unblock|role_change`,
  `admin.tariffs.update`, `admin.rate.override|override_removed`, `admin.fraud_rule.create|update|delete`,
  `admin.reconciliation.run`, `reconciliation.completed`, `fraud.operation_blocked`, `cli.user.role_change`.
  Elasticsearch недоступен — `503`. Аудит доставляется тем же асинхронным каналом, что и логи: при
  недоступном Logstash дольше 5 минут записи теряются.
- **Сверка.** `POST /reconciliation/run` `{"date_from": "...Z", "date_to": "...Z"}` (до
  `RECONCILIATION_MAX_PERIOD_DAYS`) агрегирует операции периода (кол-во и сумма по
  тип/статус/валюта), публикует `reconciliation.requested` и отвечает `202` с `run_id` и `report_url`.
  `reconciliation-worker` в одной транзакции `REPEATABLE READ` перепроверяет: пары `transfer_out`/`transfer_in`,
  `conversion_out`/`conversion_in` (сумма = `amount * rate` с округлением вниз), статусы комиссий,
  компенсации отменённых проводок, согласованность платёжных поручений и операций, зависшие операции
  (`RECONCILIATION_STUCK_AFTER_SECONDS`), отрицательные балансы, а также расхождение итогов с моментом
  запроса (информационно). Отчёт — `/app/uploads/recon/{run_id}.json`, `status: ok | mismatches`;
  `GET /reconciliation/{run_id}` отдаёт его (`404`, пока не готов).
- **Антифрод.** Правило — выражение над исходящей операцией; если оно истинно, операция сохраняется
  `failed` с `FRAUD_SUSPECTED`, в аудит пишется `fraud.operation_blocked`. Проверяются внутренние,
  межбанковские и СБП-переводы и платежи (не конвертации). Переменные: `amount`, `fee`, `balance`,
  `currency`, `operation`, `hour`, `weekday` (UTC), `account_age_days`, `ops_last_hour`, `spent_today`.
  Синтаксис: литералы, `and/or/not`, сравнения, `in`, `+ - * /`; вызовы, атрибуты и индексы
  запрещены (разбор через `ast`, без `eval`). Выражение проверяется при сохранении (`422`).
  Пример: `amount > 300000 and hour < 6`, `operation == "sbp_transfer" and ops_last_hour >= 5`.

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
