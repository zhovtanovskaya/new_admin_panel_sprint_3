# Заключительное задание первого модуля

Ваша задача в этом уроке — загрузить данные в Elasticsearch из PostgreSQL. Подробности задания в папке `etl`.

# Настройка окружения

Прежде чем запускать команды, установите значение переменным окружения в своем файле `.env`. Создайте пустой `.env` через копирование:

```
cp .env.example .env                 # Окружение для доступа к контейнерам с хоста.
cp .env.docker.example .env.docker   # Окружение для контейнеров Docker Compose.
```

Затем впишите в него свои параметры подключения к базам данных.

# Docker Compose

```
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

#  Создать образ в GitHub Container Registry

Предварительно понадобится создать ключ и залогиниться в GitHub Container Registry [по инструкции](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#pushing-container-images).  После чего можно будет создать образ и выгрузить его командами:

```
cd 01_etl/
docker build -t ghcr.io/zhovtanovskaya/new_admin_panel_sprint_3_etl:v0.1 .
docker push ghcr.io/zhovtanovskaya/new_admin_panel_sprint_3_etl:v0.1
```

# Запустить ETL

Запустить перенос данных из PostgreSQL в ElasticSearch на хосте:

```
cd 01_etl/
python ./load_data.py
```

# Unit-тесты

Пример запуска unit-тестов:

```
cd 01_etl/
python -m transform.unit_tests.db_objects_tests
```

# База данных

В проекте используется база данных, созданная ранее командами:

```
docker run -d \
    --name postgres \
    --publish 5432:5432 \
    --volume $HOME/postgresql/data:/var/lib/postgresql/data \
    --env POSTGRES_PASSWORD=123qwe \
    --env POSTGRES_USER=app \
    --env POSTGRES_DB=movies_database \
    postgres:13
cd simple_project/
psql -h 127.0.0.1 -U $POSTGRES_USER -d $POSTGRES_DB -f database/create_schema.ddl
./app/manage.py migrate
```
