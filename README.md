# LoanFlow

Учебный кредитный конвейер для зарплатных клиентов российского банка. Портфолио системного аналитика.

[Бэклог и план](https://github.com/users/denis11134/projects/1) · [Результаты закрепления и единое ревью](docs/verification.md)

## Фактическое состояние

В репозитории есть бизнес-документы, статусная модель v3, требования UC-01–UC-07,
логическая и физическая модели, OpenAPI-контракты, DDL PostgreSQL, SQL-практикум
из пяти заданий, браузерное демо и сервисы учебного стенда. LoanFlow, АБС и учебные
проверки КБКИ, паспорта и антифрода работают на FastAPI и PostgreSQL. Фоновые задачи выполняет отдельный воркер.

BRD v4, глоссарий v4, статусная модель, требования 1.6 и логическая модель
приняты **для согласованного учебного стенда**. Также приняты таблица состояний v3,
HTML-прототип, OpenAPI в главе 2.3 и пять проверенных SQL-заданий.
Открытые вопросы реального банковского сценария ведутся в глоссарии и Action Tracker.
Открытые авторские и технические задачи перечислены в [отчёте](docs/verification.md).

## Где лежат артефакты

| Каталог / файл | Содержимое |
|---|---|
| [docs/](docs/index.md) | Сайт MkDocs: бизнес-контекст, процессы, требования, данные, API, интеграции |
| [api/loanflow.openapi.yaml](api/loanflow.openapi.yaml) | 29 операций МП и рабочих мест |
| [api/abs.openapi.yaml](api/abs.openapi.yaml) | 3 операции учебной АБС |
| [api/checks.openapi.yaml](api/checks.openapi.yaml) | REST КБКИ и антифрода |
| [api/passport.xsd](api/passport.xsd) | Элементы операции SOAP 1.1 для проверки паспорта |
| [sql/001_initial_schema.sql](sql/001_initial_schema.sql) | Первоначальная схема из 20 таблиц |
| [sql/001_initial_schema_test.sql](sql/001_initial_schema_test.sql) | Штатная проверка ограничений, 35 ожидаемых отклонений |
| [sql/practicum/](sql/practicum/README.md) | Синтетические фикстуры, пять существующих решений и описание проверки |
| [docs/08-prototypes/](docs/08-prototypes/index.md) | HTML-демо, его границы и инструкция |
| [services/](services/) | FastAPI, воркер, учебная АБС, сервисные миграции и синтетический набор |
| [diagrams/loanflow-assessment.bpmn](diagrams/loanflow-assessment.bpmn) | Исполняемый каскад оценки Camunda и схема BPMN DI |
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
print("LOANFLOW_ASSESSMENT_SECRET=" + token_hex(32))
print("LOANFLOW_ASSESSMENT_TOKEN=" + token_urlsafe(32))
print("LOANFLOW_UID=" + str(os.getuid()))
print("LOANFLOW_GID=" + str(os.getgid()))
PYENV
  )
fi
docker compose build
docker compose up -d --wait postgres assessment-postgres kafka
docker compose run --rm migrate
docker compose run --rm assessment-migrate
docker compose run --rm seed
docker compose run --rm kafka-init
docker compose up -d api abs checks worker assessment
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

Для запуска без Compose нужны PostgreSQL 18, Kafka 4.1.1, Java 21 и
`fonts-dejavu-core`. Создайте две отдельные базы с разными учётными данными — ядро и оценку.
Каждому процессу передаётся `LOANFLOW_DSN` его базы. Сервис оценки получает собственный
`LOANFLOW_SECRET`, служебный токен `LOANFLOW_ASSESSMENT_TOKEN`,
`LOANFLOW_CORE_URL=http://127.0.0.1:8000`, `LOANFLOW_CHECKS_URL=http://127.0.0.1:8002`
и `LOANFLOW_KAFKA_BOOTSTRAP=127.0.0.1:9092`.
У ядра тот же служебный токен для проверки чтения статуса и адрес Kafka,
а также прежние настройки АБС.
На PostgreSQL стенда установите `log_min_messages=fatal`,
`log_min_error_statement=panic`, `log_parameter_max_length=0` и
`log_parameter_max_length_on_error=0`, как в `compose.yaml`.

После установки зависимостей выполните миграцию и загрузку ядра с его окружением.
В отдельном окружении базы оценки выполните её миграцию.

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r services/requirements.txt
.venv/bin/python -m services.loanflow.seed migrate
.venv/bin/python -m services.loanflow.seed seed --tokens .cache/loanflow/tokens.json
.venv/bin/python -m services.loanflow.events topics
# С LOANFLOW_DSN базы оценки
.venv/bin/python -m services.loanflow.events migrate
```

В отдельных терминалах запустите API, АБС, проверки и процесс ядра с окружением ядра,
процесс оценки — с окружением базы оценки.

```bash
.venv/bin/uvicorn services.loanflow.api:app --port 8000 --no-access-log
.venv/bin/uvicorn services.loanflow.abs:app --port 8001 --no-access-log
.venv/bin/uvicorn services.loanflow.check_stubs:app --port 8002 --no-access-log
.venv/bin/python -m services.loanflow.events core
.venv/bin/python -m services.loanflow.events assessment
```

### Обновление до 5.3

На прежней версии остановите приём новых подач и дождитесь завершения всех заявок
в `SCORING`. Старая очередь оценки должна опустеть перед переключением транспорта.
Заявки в `UNDERWRITING` сохраняют рассчитанные условия в ядре и завершаются прежней
ручной командой. Предложения, резервы, доказательства подписи, договоры, outbox АБС
и записи сверки продолжают использовать данные ядра. Сделайте резервную копию учебной БД,
остановите прежние API и воркер, затем примените миграции и запустите новый Compose.
Для существующего `.env` добавьте независимо сгенерированные
`LOANFLOW_ASSESSMENT_SECRET` и `LOANFLOW_ASSESSMENT_TOKEN` командой Python из примера выше.
Загрузчик seed повторно не требуется.

Миграция 010 добавляет учёт распределённых оценок и сохранённый признак предупреждения ПДН.
Для прежних записей признак вычисляется по прежним полям оценки.
Процесс ядра проверяет отсутствие старых заданий оценки в `SCORING` перед запуском.
Два топика создаются идемпотентной командой `events topics`.

### Оркестрация оценки 5.4

Camunda Run 8.10.9 для Linux управляет последовательностью и ожиданиями оценки.
PostgreSQL сервиса оценки хранит входы, расчёты, журнал шагов и результат; ядро
продолжает применять решение и выполнять таймеры подписания и выдачу через АБС.
Исходник процесса, интерфейс воркера и порядок обновления описаны в
[главе 5.4](docs/07-architecture/5.4-assessment-orchestration.md). Перед обновлением
завершите все заявки в `SCORING`; повторный seed и сброс Kafka offsets не требуются.

По умолчанию `LOANFLOW_ASSESSMENT_ENGINE=local`. Для режима `camunda` после подготовки
PostgreSQL, Kafka, миграций и seed замените команду запуска прикладных сервисов:

```bash
docker compose -f compose.yaml -f compose.camunda.yaml up -d --build api abs checks worker assessment camunda
```

Overlay сохраняет данные движка в `camunda_data` и включает режим Camunda только
для `assessment`. Для следующих команд Compose указывайте оба файла.

Для запуска без Docker подготовьте Camunda Run и скопируйте локальную конфигурацию:

```bash
.venv/bin/python scripts/prepare_camunda.py --cache-dir .cache/camunda-8.10.9
export CAMUNDA_HOME="$PWD/.cache/camunda-8.10.9/c8run-8.10.9"
cp services/camunda/application.yaml "$CAMUNDA_HOME/loanflow.yaml"
(cd "$CAMUNDA_HOME" && ./c8run start --no-browser --disable-connectors --config loanflow.yaml)
```

Загрузчик проверяет закреплённые размер и SHA-256 архива и печатает абсолютный путь
`CAMUNDA_HOME`. Дистрибутив включает Java. После готовности
`http://127.0.0.1:8080/v2/topology` в отдельном терминале с окружением **базы оценки**:

```bash
LOANFLOW_ASSESSMENT_ENGINE=camunda \
LOANFLOW_CAMUNDA_URL=http://127.0.0.1:8080 \
.venv/bin/python -m services.loanflow.events assessment
```

Воркер автоматически разворачивает BPMN при создании новой оценки. Остальные
процессы используют прежние команды 5.3, свои базы и существующие секреты.
Движок останавливается командой `(cd "$CAMUNDA_HOME" && ./c8run stop)`.

Приёмочный прогон использует настоящий движок и Kafka:

```bash
CAMUNDA_HOME="$PWD/.cache/camunda-8.10.9/c8run-8.10.9" \
KAFKA_HOME=/absolute/path/kafka_2.13-4.1.1 \
JAVA_HOME=/absolute/path/jdk-21 \
PG_BINDIR=/usr/lib/postgresql/18/bin \
.venv/bin/python -m pytest -q tests/test_camunda.py --require-camunda --require-kafka
```

Нативный запуск проверяется на настоящих Camunda, Kafka и PostgreSQL отдельным
обязательным заданием CI. Оно также проверяет конфигурацию Compose, сборку образа
LoanFlow и наличие в нём AsyncAPI и BPMN.

### Проверка реального Kafka без Docker

Установите `requirements-checks.txt`, распакуйте официальный Kafka 4.1.1 и задайте
`JAVA_HOME` для Java 21. Команда запускает временный брокер, две отдельные базы,
HTTP-сервисы и приёмочные процессы, затем останавливает их.

```bash
KAFKA_HOME=/absolute/path/kafka_2.13-4.1.1 \
PG_BINDIR=/usr/lib/postgresql/18/bin \
.venv/bin/python -m pytest -q tests/test_kafka.py --require-kafka
```

В CI реальный обмен проходит отдельным обязательным заданием на Python 3.12.
Прежние локальные сервисные регрессии и Postman используют встроенный обработчик оценки
при отсутствии `LOANFLOW_KAFKA_BOOTSTRAP`. Для распределённого запуска эта переменная обязательна.
Параметры учебных сбоев оценки доступны в `training_assessment_fault` базы оценки
по заявке, этапу и номеру попытки. Они используются приёмочными сценариями с синтетическими данными.

Набор [services/scenarios.json](services/scenarios.json) задаёт доход, текущие платежи,
результаты внешних проверок, квоты и доверенные контексты. График, ПДН, рекомендация DE и учебная
ПСК рассчитываются по [главе 3.3](docs/03-requirements/3.3-calculation-tasks.md). Загрузчик принимает другой набор через
`--scenarios`. В поле `faults` можно задать последовательность исходов для каждого шага,
например `{"AML": ["TIMEOUT", "TIMEOUT", "TIMEOUT"]}`. Служебные таблицы `training_abs.control`
и `training_abs.call` управляют поведением и считают HTTP-вызовы учебной АБС.
По умолчанию АБС оставляет принятое распоряжение в `PENDING`. Проведение и
закрытие в обоих порядках показаны в [HTTP-тестах АБС](tests/test_service_abs.py).

Заглушки проверок читают учебные ответы из сценария клиента по UUID. По умолчанию
паспорт даёт `VALID`, антифрод даёт `CLEAR`, самозапрета нет. Для других веток задайте
`passport_verdict: INVALID`, `fraud_verdict: SUSPECTED` или `self_ban: true` в сценарии.
Запросы и порядок вызовов описаны в [главе 4.2](docs/06-integrations/4.2-field-mappings.md).

При обновлении до 3.3 завершите оценку и ручное рассмотрение старой версией. Перед миграцией
остановите приём новых подач и проверьте, что в `SCORING` и `UNDERWRITING` нет заявок.
Миграция 007 проверяет это условие. Сохранённые Предложения и экземпляры ИУ используются при последующих чтениях.

При обновлении уже работающего стенда с 3.3 остановите API и воркер на время миграции 008.
Заявки с незавершённой оценкой, уже прошедшие AML, продолжат оценку с проверки паспорта.

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
