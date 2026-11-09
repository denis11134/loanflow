# LoanFlow

Учебный кредитный конвейер для зарплатных клиентов российского банка. Портфолио системного аналитика.

[Бэклог и план](https://github.com/users/denis11134/projects/1) · [Результаты закрепления и единое ревью](docs/verification.md)

## Фактическое состояние

В репозитории есть бизнес-документы, статусная модель v3, требования UC-01–UC-07,
логическая и физическая модели, два OpenAPI-контракта, DDL PostgreSQL, SQL-практикум
из пяти заданий, браузерное демо и сервисы учебного стенда. LoanFlow и учебная АБС
работают на FastAPI и PostgreSQL. Фоновые задачи выполняет отдельный воркер.

Статусная модель, требования 1.6 и логическая модель помечены согласованными **для учебного стенда**.
BRD v4 остаётся черновиком на согласовании, глоссарий v4 — на ревью. Для текущей итерации
приняты таблица состояний v3, HTML-прототип, OpenAPI в главе 2.3 и пять проверенных SQL-заданий.
Открытые авторские и технические задачи перечислены в [отчёте](docs/verification.md).

## Где лежат артефакты

| Каталог / файл | Содержимое |
|---|---|
| [docs/](docs/index.md) | Сайт MkDocs: бизнес-контекст, процессы, требования, данные, API, интеграции |
| [api/loanflow.openapi.yaml](api/loanflow.openapi.yaml) | 29 операций МП и рабочих мест |
| [api/abs.openapi.yaml](api/abs.openapi.yaml) | 3 операции учебной АБС |
| [sql/001_initial_schema.sql](sql/001_initial_schema.sql) | Первоначальная схема из 20 таблиц |
| [sql/001_initial_schema_test.sql](sql/001_initial_schema_test.sql) | Штатная проверка ограничений, 35 ожидаемых отклонений |
| [sql/practicum/](sql/practicum/README.md) | Синтетические фикстуры, пять существующих решений и описание проверки |
| [docs/08-prototypes/](docs/08-prototypes/index.md) | HTML-демо, его границы и инструкция |
| [services/](services/) | FastAPI, воркер, учебная АБС, сервисные миграции и синтетический набор |
| [tests/](tests/) | OpenAPI, MkDocs, PostgreSQL, браузер и интеграционные HTTP-тесты |
| [.github/workflows/checks.yml](.github/workflows/checks.yml) | CI технической части и отдельная блокирующая проверка контрактов |

Новые Markdown-страницы включаются в `nav` файла `mkdocs.yml`.

## Запуск стенда

Нужны Docker с Compose и Python 3.12 для короткого HTTP-прогона. Выполните в корне репозитория.
На чистом стенде Compose применяет исходную DDL и следующие миграции в порядке номеров.
Ключи ниже создаются один раз и сохраняются между перезапусками.

```bash
mkdir -p .cache/loanflow
if [ ! -e .env ]; then
  (umask 077; python3.12 - <<'PYENV' > .env
import os
from secrets import token_hex, token_urlsafe
print("LOANFLOW_SECRET=" + token_hex(32))
print("LOANFLOW_ABS_TOKEN=" + token_urlsafe(32))
print("LOANFLOW_UID=" + str(os.getuid()))
print("LOANFLOW_GID=" + str(os.getgid()))
PYENV
  )
fi
docker compose build
docker compose up -d --wait postgres
docker compose run --rm migrate
docker compose run --rm seed
docker compose up -d api abs worker
```

Сервис принимает запросы на `127.0.0.1:8000`. Seed создаёт учебные токены всех шести ролей
в `.cache/loanflow/tokens.json` с правами `0600`. Команда seed требует свободного пути
для файла токенов. При повторном запуске Compose используйте уже созданный файл.

```bash
python3.12 scripts/demo_service.py .cache/loanflow/tokens.json
```

Прогон создаёт черновик на 50 000 ₽, подаёт его дважды, дожидается оценки и сверяет
SHA-256 ИУ с PDF, полученным через HTTP. Для повторной демонстрации сначала отмените
заявку из `APPROVED` с помощью её идентификатора, выведенного скриптом.

Для запуска без Compose установите пакет `fonts-dejavu-core` и задайте `LOANFLOW_DSN` для чистого PostgreSQL 18,
`LOANFLOW_SECRET`, `LOANFLOW_ABS_TOKEN` и `LOANFLOW_ABS_URL=http://127.0.0.1:8001`.
Из корня репозитория выполните миграции и загрузку данных. Запустите три процесса
в отдельных терминалах с теми же переменными.

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r services/requirements.txt
.venv/bin/python -m services.loanflow.seed migrate
.venv/bin/python -m services.loanflow.seed seed --tokens .cache/loanflow/tokens.json
.venv/bin/uvicorn services.loanflow.api:app --port 8000 --no-access-log
.venv/bin/uvicorn services.loanflow.abs:app --port 8001 --no-access-log
.venv/bin/python -m services.loanflow.worker
```

Набор [services/scenarios.json](services/scenarios.json) задаёт входы ПДН, параметры DE,
предел ПСК, квоты и доверенные контексты. Загрузчик принимает другой набор через
`--scenarios`. В поле `faults` можно задать последовательность исходов для каждого шага,
например `{"AML": ["TIMEOUT", "TIMEOUT", "TIMEOUT"]}`. Служебные таблицы `training_abs.control`
и `training_abs.call` управляют поведением и считают HTTP-вызовы учебной АБС.
По умолчанию АБС оставляет принятое распоряжение в `PENDING`. Проведение и
закрытие в обоих порядках показаны в [HTTP-тестах АБС](tests/test_service_abs.py).

## Установка и проверка

Целевая среда CI: Ubuntu 24.04, Python 3.12, PostgreSQL 18, Chromium из Playwright.
Прямые зависимости закреплены в `requirements-checks.txt`, транзитивные — в `requirements-checks.lock.txt`.
Нужны серверные утилиты PostgreSQL (`initdb`, `pg_ctl`, `createdb`, `psql`, `pg_config`),
а не только клиент. Запускайте проверки обычным пользователем, не root.

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-checks.txt
PLAYWRIGHT_BROWSERS_PATH="$PWD/.cache/ms-playwright" .venv/bin/python -m playwright install --with-deps chromium
.venv/bin/python scripts/check.py
```

Для нескольких установленных PostgreSQL можно задать `PG_BINDIR`, например
`PG_BINDIR=/usr/lib/postgresql/18/bin`. Тесты запускают собственный временный кластер
без TCP, с отдельным Unix-сокетом, игнорируют `PGHOST`/`PGDATABASE` и не используют
существующую пользовательскую БД. Остановка предусмотрена и при ошибке. Фикстуры,
срезы и часы демо фиксированы; текущая дата не влияет на результат.

52 контрпримера API-01–API-06 входят в обычный прогон. Исправленные схемы должны отклонять
эти тела запросов и ответов. Для запуска отдельных частей используйте команды ниже:

```bash
.venv/bin/python scripts/check.py --technical
.venv/bin/python -m pytest -q tests/test_contracts.py --require-ready
.venv/bin/python -m pytest -q tests/test_database.py
.venv/bin/python -m pytest -q -m service
.venv/bin/python -m pytest -q -s tests/test_prototype.py
```

Общий запуск пишет JUnit в `test-results/checks.xml`. В CI задача контрактов блокирует
готовность, а технический прогон проверяет также HTTP-сервисы. Для HTTP-тестов
FastAPI и учебная АБС слушают временные порты на loopback. Проверки сверяют ответы
с OpenAPI и читают строки из PostgreSQL через отдельные подключения.

На Ubuntu 26.04 для Playwright 1.58 можно использовать сборку Ubuntu 24.04.

```bash
PLAYWRIGHT_HOST_PLATFORM_OVERRIDE=ubuntu24.04-x64 PLAYWRIGHT_BROWSERS_PATH="$PWD/.cache/ms-playwright" .venv/bin/python -m playwright install chromium
```

CI использует Ubuntu 24.04 без переопределения. В ограниченной песочнице запуск PostgreSQL и Chromium
может требовать разрешения на создание локальных сокетов и процессов.

## Документация и демо

Для одного сайта достаточно `.venv/bin/python -m pip install -r requirements-docs.txt`.

```bash
.venv/bin/python -m mkdocs serve
.venv/bin/python -m mkdocs build --strict
```

Документация: <http://127.0.0.1:8000>. Демо доступно через раздел «1.7. Прототипы» или
при открытии [HTML-файла](docs/08-prototypes/loanflow-demo.html) в браузере.
