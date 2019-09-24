#!/bin/bash
# todo pass parameters
# todo log forward - correct way to start and monitor
echo "[$(date)][RAPPI]: Started."

echo "Activating virtual environment"
source env/bin/activate

export CONCURRENCY=1
export WORKER=1
export CELERY_QUEUE="$RETAILER_KEY"_"$SCRAPER_TYPE"

echo "[$(date)][RAPPI]: worker=$WORKER | concurrency=$CONCURRENCY | queue=$CELERY_QUEUE"

echo "[$(date)][RAPPI]: Initiating master."
python master.py
sleep 10s

echo "[$(date)][RAPPI]: Starting 1 worker in foreground mode."
celery worker -c $CONCURRENCY -n rappi-worker1-%h --pool=eventlet --loglevel=INFO -Q $CELERY_QUEUE -A worker
