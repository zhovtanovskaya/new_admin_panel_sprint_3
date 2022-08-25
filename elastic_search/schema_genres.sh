# "refresh_interval": "1s" — при сохранении данных обновляет индекс раз в секунду.
# Блок "analysis" — в нём задаются все настройки для полнотекстового поиска: фильтры 
# и анализаторы. Незаменимая вещь для задачи поиска по тексту.
# В каждой схеме данных указано "dynamic": "strict" — это позволяет защититься от
# невалидных данных.
# Поля actors и writers используют вложенную схему данных — это помогает валидировать
# вложенные json-объекты.
# Также присутствуют поля actors_names и writers_names, которые упрощают запросы на 
# поиск. Понадобится в следующих модулях.
# Поле title содержит внутри себя ещё одно поле — title.raw. Оно нужно, чтобы у
# Elasticsearch была возможность делать сортировку, так как он не умеет сортировать
# данные по типу text.
curl -XPUT http://127.0.0.1:9200/genres -H 'Content-Type: application/json' -d'
{
  "settings": {
    "refresh_interval": "1s",
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  "_english_"
        },
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        },
        "english_possessive_stemmer": {
          "type": "stemmer",
          "language": "possessive_english"
        },
        "russian_stop": {
          "type":       "stop",
          "stopwords":  "_russian_"
        },
        "russian_stemmer": {
          "type": "stemmer",
          "language": "russian"
        }
      },
      "analyzer": {
        "ru_en": {
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "english_stop",
            "english_stemmer",
            "english_possessive_stemmer",
            "russian_stop",
            "russian_stemmer"
          ]
        }
      }
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "id": {
        "type": "keyword"
      },
      "name": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "description": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "film_ids": {
        "type": "keyword"
      }
    }
  }
}'
