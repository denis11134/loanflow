# LoanFlow

Учебный кредитный конвейер для зарплатных клиентов российского банка. Портфолио системного аналитика.

[Бэклог и план](https://github.com/users/denis11134/projects/1) · [Результаты закрепления и единое ревью](docs/verification.md)

## Фактическое состояние

В репозитории есть бизнес-документы, статусная модель v3, требования UC-01–UC-07,
логическая и физическая модели, два OpenAPI-контракта, DDL PostgreSQL, SQL-практикум
из пяти заданий и браузерное демо. Сервисов FastAPI, рабочей интеграции АБС и Kafka пока нет.
Браузерное демо не доказывает транзакции, авторизацию или восстановление после сбоя.

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
| [tests/](tests/) | Структура/примеры OpenAPI, MkDocs/ссылки, PostgreSQL, браузер |
| [.github/workflows/checks.yml](.github/workflows/checks.yml) | CI технической части и отдельная блокирующая проверка контрактов |

Новые Markdown-страницы включаются в `nav` файла `mkdocs.yml`.

## Установка и проверка

Целевая среда CI: Ubuntu 24.04, Python 3.12, PostgreSQL 18, Chromium из Playwright.
Прямые зависимости закреплены в `requirements-checks.txt`, транзитивные — в `requirements-checks.lock.txt`.
Нужны серверные утилиты PostgreSQL (`initdb`, `pg_ctl`, `createdb`, `psql`, `pg_config`),
а не только клиент. Запускайте проверки обычным пользователем, не root.

```bash
python3 -m venv .venv
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
.venv/bin/python -m pytest -q -s tests/test_prototype.py
```

Общий запуск пишет JUnit в `test-results/checks.xml`. В CI задача контрактов блокирует
готовность, а технические проверки запускаются независимо. Без серверов проверяются
только статические контракты, ограничения БД, SQL и демо; список будущих сервисных проверок
находится в [отчёте](docs/verification.md).

На машине этого прохода установлен Python 3.14 и Ubuntu 26.04. Для Playwright 1.58 здесь
понадобилась сборка Ubuntu 24.04:

```bash
PLAYWRIGHT_HOST_PLATFORM_OVERRIDE=ubuntu24.04-x64 PLAYWRIGHT_BROWSERS_PATH="$PWD/.cache/ms-playwright" .venv/bin/python -m playwright install chromium
```

Это локальная совместимость, проверенная запуском браузера; CI использует поддерживаемую
Ubuntu 24.04 без переопределения. В ограниченной песочнице запуск PostgreSQL и Chromium
может требовать разрешения на создание локальных сокетов и процессов.

## Документация и демо

Для одного сайта достаточно `.venv/bin/python -m pip install -r requirements-docs.txt`.

```bash
.venv/bin/python -m mkdocs serve
.venv/bin/python -m mkdocs build --strict
```

Документация: <http://127.0.0.1:8000>. Демо доступно через раздел «1.7. Прототипы» или
при открытии [HTML-файла](docs/08-prototypes/loanflow-demo.html) в браузере.
