#! /bin/bash

if [ $# -ne 1 ];then
    echo "Usage: $0 <input_metainfo_file>"
    exit 1
fi

metainfo_file="$1"
workerlist="workers"
workers_n_splits="workers_n_splits"
#Check file
if [[ "$metainfo_file" != *".meta" ]]; then
    echo "Error: File '$metainfo_file' is not metadata file."
    exit 1
fi
if [ ! -f "$metainfo_file" ]; then
    echo "Error: File '$metainfo_file' not found."
    exit 1
fi

metafile_name="$(basename $metainfo_file)"
metafile_name="${metafile_name%.meta}"

dir="\$DIRECT_ACCESS_MR_HOME/data/inputs/${metafile_name}"
# for line in $(sed 's!/[^/]*:!:!g' "$metainfo_file" | sort | uniq -c | awk '{print $2 ":" $1}');
for line in $(awk -F: '{print $2}' "$metainfo_file" | sort | uniq -c | awk '{print $2 ":" $1}');
do
    worker="$(echo $line | awk -F: '{print $1}')"
    nsplits="$(echo $line | awk -F: '{print $2}')"
    ip="$(awk -F: "/^$worker/ {print \$2}" $workerlist)"
    # echo "$worker $dir $nsplits $ip"
    ssh $ip "rm -r $dir"
    # Update the worker_n_split file
    old_val="$(awk -F: "/^$worker/ {print \$2}" $workers_n_splits)"
    new_val="$(( old_val - nsplits ))"
    # echo "updating "$worker"'s "$old_val" to "$new_val""
    sed -i "s/${worker}:.*/${worker}:$new_val/g" $workers_n_splits
done

rm $metainfo_file

echo "$metainfo_file and its splits are deleted"
echo "  $workers_n_splits:"
cat $workers_n_splits