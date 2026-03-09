#! /bin/bash

if [ $# -ne 1 ];then
    echo "Usage: $0 <output_metainfo_file>"
    exit 1
fi

metainfo_file="$1"
workerlist="workers"
# workers_n_splits="workers_n_splits"
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

dir="\$DA_MR_HOME/data/outputs/${metafile_name}"

# for line in $(sed 's!/[^/]*:!:!g' "$metainfo_file" | sort | uniq);
for line in $(cat $workerlist);
do
    # dir="$(echo $line | awk -F: '{print $1}')"
    # worker="$(echo $line | awk -F: '{print $2}')"

    # ip="$(awk -F: "/^$worker/ {print \$2}" $workerlist)"
    worker="$(echo $line | awk -F: '{print $1}')"
    ip="$(echo $line | awk -F: '{print $2}')"

    ssh $ip "rm -r $dir"
done

rm $metainfo_file