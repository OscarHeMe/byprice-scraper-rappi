#!/bin/bash

count=0
sleep 600
max_cpu=$(ps aux | grep $CELERY_QUEUE | grep celery | awk '{print $3}')
is_alive=1

while (( $is_alive ))
do
  current_cpu=$(ps aux | grep $CELERY_QUEUE | grep celery | awk '{print $3}')

  res=$(echo $current_cpu'>'$max_cpu | bc -l)
  if (( $res ))
  then
    max_cpu=$current_cpu
  fi

  res=$(echo $current_cpu'<'$(echo 0.30 \* $max_cpu | bc ) | bc -l)
  if (( $res ))
  then
    ((count+=1))
    echo "Idle for $(( 5*count )) minutes"
  else
    count=0
  fi

  if (( count>5 ))
  then
    echo "Shutting down"
    # wait a little bit more before actually pulling the plug
    sleep 10
    kill -9 $(ps aux | grep $CELERY_QUEUE | grep celery | awk '{print $2}')
    is_alive=0
  fi

  echo "Max CPU: "$max_cpu", Current CPU: "$current_cpu
  sleep 300

done

