# Заключительное задание первого модуля

Ваша задача в этом уроке — загрузить данные в Elasticsearch из PostgreSQL. Подробности задания в папке `etl`.

# Настройка окружения

Прежде чем запускать команды, установите значение переменным окружения в своем файле `.env`. Создайте пустой `.env` через
копирование:

```
cp .env.example .env
```

Затем впишите в него свои параметры подключения к базам данных.

# Команды

Запустить перенос данных из PostgreSQL в ElasticSearch:

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

# Elastic Search

```
docker run \
    -p 9200:9200 
    -e "discovery.type=single-node" \
    docker.io/elastic/elasticsearch:7.7.0
```
