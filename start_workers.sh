#! /bin/bash

workerlist="workers"

if [ ! -f "$workerlist" ]; then
    echo "Error: File '$workerlist' not found."
    exit 1
fi

for line in $(cat $workerlist);
do
    worker="$(echo $line | awk -F: '{print $1}')"
    ip="$(echo $line | awk -F: '{print $2}')"

    ssh "$ip" "\$DA_MR_HOME/run_worker.sh $worker -b"
done