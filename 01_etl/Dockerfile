FROM python:3.10

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.pip requirements.pip

RUN  pip install --upgrade pip \
     && pip install -r requirements.pip

COPY . .

ENTRYPOINT ["python", "load_data.py"]
