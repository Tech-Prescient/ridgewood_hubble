FROM python:3.8-slim-buster

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN mkdir -p /opt/app || true

WORKDIR /opt/app

COPY requirements.txt /opt/app
RUN pip install --no-cache-dir -r /opt/app/requirements.txt

COPY . /opt/app

RUN chmod +x /opt/app/start.sh

ENTRYPOINT ["/bin/sh"]
