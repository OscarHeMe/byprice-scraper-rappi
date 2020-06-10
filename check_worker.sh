#!/bin/bash

count=0
sleep 900
max_load=$(uptime | sed -e 's/.*load average: //g' | awk 'BEGIN{FS=", "} {print $3}')
is_alive=1

while (( $is_alive ))
do
  load_5_min=$(uptime | sed -e 's/.*load average: //g' | awk 'BEGIN{FS=", "} {print $1}')
  load_15_min=$(uptime | sed -e 's/.*load average: //g' | awk 'BEGIN{FS=", "} {print $3}')

  res=$(echo $load_15_min'>'$max_load | bc -l)
  if (( $res ))
  then
    max_load=$load_15_min
  fi

  res=$(echo $load_5_min'<'$(echo 0.30 \* $load_15_min | bc ) | bc -l)
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
    kill -9 $(ps aux | grep $CELERY_QUEUE | awk '{print $2}')
    kill -9 $(ps aux | grep celery | awk '{print $2}')
    is_alive=0
  fi

  echo "Max load: "$max_load", Load last 15 min: "$load_15_min", Load last 5 min: "$load_5_min
  sleep 300

done

