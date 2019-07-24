#!/bin/bash

. env/bin/activate

export CELERY_QUEUE="rappi_""$SCRAPER_TYPE"
if [ "$ENV" = "DEV" ]
then
   echo "DEV environment"
   export CELERY_QUEUE="$CELERY_QUEUE""_dev"
fi

celery -A worker worker -c 4 --loglevel=INFO -Q $CELERY_QUEUE -n rappi-$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)

