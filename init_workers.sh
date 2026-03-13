#! /bin/bash

workerlist="workers"
workers_n_splits="workers_n_splits"

if [ ! -f "$workerlist" ]; then
    echo "Error: File '$workerlist' not found."
    exit 1
fi

# create worker_n_splits file in current dir
if [ ! -f "$workers_n_splits" ]; then
    for worker in $(cat $workerlist | awk -F: '{print $1}');
    do
        echo "${worker}:0" >> $workers_n_splits
    done
fi

# Check an IP is a local IP
is_local_ip() {
    local test_ip=$1
    local local_ips=$(ip -4 addr show | grep 'inet ' | awk '{print $2}' | cut -d/ -f1)

    for ip in $local_ips; do
        if [ "$ip" == "$test_ip" ]; then
            return 0
        fi
    done
    return 1
}

# create dirs in each worker
for line in $(cat $workerlist);
do
    worker="$(echo $line | awk -F: '{print $1}')"
    worker_ip="$(echo $line | awk -F: '{print $2}')"
    # copy the 'workers' file
    if ! is_local_ip "$worker_ip"; then
        remote_dir=$(ssh $worker_ip "echo \$DIRECT_ACCESS_MR_HOME")
        sftp $worker_ip <<EOF
put "$workerlist" "$remote_dir"
bye
EOF
    fi
    # echo $worker_ip
    ssh "$worker_ip" "mkdir -p \$DIRECT_ACCESS_MR_HOME/data/inputs; mkdir \$DIRECT_ACCESS_MR_HOME/data/outputs;\
    mkdir \$DIRECT_ACCESS_MR_HOME/mapred_bin; \$DIRECT_ACCESS_MR_HOME/mnt_targets.sh $worker"
    # ssh "$worker_ip" 'mkdir -p $DIRECT_ACCESS_MR_HOME/data/inputs; mkdir $DIRECT_ACCESS_MR_HOME/data/outputs;\
    # mkdir $DIRECT_ACCESS_MR_HOME/mnt'
    # for l in $(cat $workerlist);
    # do
    #     w="$(echo $l | awk -F: '{print $1}')"
    #     ssh "$worker_ip" "mkdir \$DIRECT_ACCESS_MR_HOME/mnt/$w"
    # done
done