import os

from dotenv import load_dotenv

load_dotenv()


POSTGRES_DB = {
    'dbname': os.getenv('POSTGRES_NAME'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
}

POSTGRES_TEST_DB = {
    'dbname': os.getenv('POSTGRES_TEST_NAME'),
    'user': os.getenv('POSTGRES_TEST_USER'),
    'password': os.getenv('POSTGRES_TEST_PASSWORD'),
    'host': os.getenv('POSTGRES_TEST_HOST'),
    'port': os.getenv('POSTGRES_TEST_PORT'),
}
