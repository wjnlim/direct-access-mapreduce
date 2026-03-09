#! /bin/bash

if [ $# -lt 1 ];then
    echo "Usage: $0 <worker_name> -b"
    echo "Option:"
    echo " -b) run worker in the background"
    exit 1
fi

opts=$(getopt -o=b --name "$0" -- "$@") || exit 1
eval set -- "$opts"

workerlist="$DA_MR_HOME/workers"
# parse options
while true; do
    case "$1" in
        -b)
            background="true"
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Unexpected option: $1"
            exit 1
            ;;
    esac
done

worker_name="$1"
# worker_exec="$2"
# exec_name=$(basename "$worker_exec")
# if [[ "$exec_name" != "worker" ]]; then
#     echo "Error: the excutable is not 'worker'"
#     exit 1
# fi

worker_ip=$(cat $workerlist | grep "$worker_name" | awk -F: '{print $2}')
worker_dev=$(lsscsi | awk "/${worker_name}_shared/ {print \$NF}")

# cmd="$worker_exec $worker_name $worker_ip mapred_bin sharedfiles mnt $worker_dev"
# cmd="$DA_MR_HOME/bin/worker $worker_name $worker_ip \
# $DA_MR_HOME/mapred_bin $DA_MR_HOME/sharedfiles $DA_MR_HOME/mnt $worker_dev"
cmd="$DA_MR_HOME/bin/worker $worker_name $worker_ip $worker_dev"
if [[ "$background" == "true" ]]; then
    echo "executing in the background: $cmd"
    $cmd &>/dev/null &
else
    echo "executing $cmd"
    $cmd
fi