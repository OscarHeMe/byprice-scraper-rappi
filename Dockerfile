# Dockerfile for celery worker
FROM byprice/base-scraper:python3.6

RUN apt-get install bc

# App & Logging
ENV APP_NAME='byprice-scraper-rappi'
ENV LOG_LEVEL='INFO'
ENV LOG_HOST='localhost'
ENV LOG_PORT=10000

# Streamer
ENV STREAMER='rabbitmq'
ENV STREAMER_HOST='rabbitmq.byprice.local'
ENV STREAMER_QUEUE='routing'
ENV STREAMER_EXCHANGE='data'
ENV STREAMER_EXCHANGE_TYPE='direct'

# Services
ENV SRV_GEOLOCATION='gate.byprice.com/bpgeolocation'

# Celery and
ENV C_FORCE_ROOT="true"
ENV CELERY_BROKER='rabbitmq.byprice.local'

# Retailer
ENV RETAILER_KEY="rappi"
MAINTAINER Byprice Dev Team

# Ubuntu default encoding
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY ./ /byprice-scraper-rappi/
RUN mkdir /byprice-scraper-rappi/logs

# Install server stuff
RUN pip install \
    virtualenv

# Change workdir
WORKDIR /byprice-scraper-rappi

RUN chmod +x run.sh

RUN virtualenv env && env/bin/pip install -r requirements.txt

VOLUME /var/log/byprice-scrapers
