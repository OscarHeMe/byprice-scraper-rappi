# Dockerfile for celery worker
FROM byprice/base-scraper:python3.6

RUN apt-get install bc

# Ubuntu default encoding
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# App , environment & Logging
ENV APP_NAME='byprice-scraper-rappi-master'
ENV RETAILER_KEY='rappi'
ENV ENV='PROD'
ENV REGION='DEFAULT'
ENV LOG_LEVEL='INFO'
ENV LOG_PORT=27971
ENV LOG_HOST='0.0.0.0'
ENV STORES=600

# Streamer
ENV STREAMER='rabbitmq'
ENV STREAMER_HOST='10.10.0.4'
ENV STREAMER_PORT=5672
ENV STREAMER_QUEUE='routing'
ENV STREAMER_ROUTING_KEY='routing'
ENV STREAMER_EXCHANGE='data'
ENV STREAMER_EXCHANGE_TYPE='direct'
ENV STREAMER_VIRTUAL_HOST='mx'
ENV STREAMER_USER='mx_pubsub'

# Celery
ENV C_FORCE_ROOT="true"
ENV CELERY_BROKER='10.10.0.4'
ENV CELERY_PORT=5672
ENV CELERY_VIRTUAL_HOST='mx_scraper'
ENV CELERY_USER='mx_celery'

# Queues
ENV QUEUE_ROUTING='routing'
ENV QUEUE_CATALOGUE='catalogue'
ENV QUEUE_GEOPRICE='geoprice'

# Services
ENV SMONITOR=smonitor
ENV SRV_GEOLOCATION='geolocation.api.byprice.com'

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
