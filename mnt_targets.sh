#! /bin/bash

if [ $# -ne 1 ];then
    echo "Usage: $0 <worker_name>"
    exit 1
fi

worker_name="$1"
workerlist="$DA_MR_HOME/workers"

for line in $(cat $workerlist); do
    worker="$(echo $line | awk -F: '{print $1}')"
    ip="$(echo $line | awk -F: '{print $2}')"
    sudo iscsiadm --mode node -T iqn.2022-05.$ip:$worker --logout
    sudo iscsiadm --mode discovery --type sendtargets --portal $ip &&\
    sudo iscsiadm --mode node -T iqn.2022-05.$ip:$worker --login
done

for worker in $(cat $workerlist | awk -F: '{print $1}'); do
    dev=$(lsscsi | awk "/${worker}_shared/ {print \$NF}")
    dir=$DA_MR_HOME/mnt/$worker

    mkdir -p $dir

    if [[ "$worker_name" == "$worker" ]]; then
        sudo mount $dev $dir
    else
        sudo mount -oro $dev $dir
    fi
done