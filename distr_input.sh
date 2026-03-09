#! /bin/bash

if [ $# -lt 2 ]; then
	echo "Usage: $0 <filepath> <chunk_size> -o <metadata file name> -w \"worker0 worker1 ...\""
    echo "Option:"
    echo " -w) a list of workers across which the file splits are distributed"
	exit 1
fi

opts=$(getopt -o=w:o: --name "$0" -- "$@") || exit 1
eval set -- "$opts"

workerlist="workers"
workers_n_splits="workers_n_splits"
worker_arr=()
metadata_file=""
# parse options
while true; do
    case "$1" in
        -w)
            # split workers by space
            IFS=' ' read -a worker_arr <<< "$2"
            shift 2
            ;;
        -o)
            metadata_file="$2.meta"
            shift 2
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

#positional args
filepath="$1"
chunk_size="$2"

filename="$(basename $1)"

if [[ -z "$metadata_file" ]]; then
    metadata_file="${filepath}.meta"
fi

metafile_name="$(basename $metadata_file)"
metafile_name="${metafile_name%.meta}"
# Check if the file exists
if [ ! -f "$filepath" ]; then
  echo "Error: File '$filepath' not found."
  exit 1
fi

# Check if already has metainfo file; 
if [ -f "$metadata_file" ]; then
  echo "Error: File '$metadata_file' already exists."
  exit 1
fi

# If worker array is empty
if [ ${#worker_arr[@]} -eq 0 ]; then
    # sort workers to distribute splits to the worker which has min number of splits first
    worker_arr=($(sort -k2n -t: $workers_n_splits | awk -F: '{print $1}'))
fi

# echo "worker arr: ${worker_arr[@]}, metafile: $metadata_file, mfilename: $metafile_name"

# filename="$(basename $1)"
split_tmp_dir="${metafile_name}_split_tmp"
mkdir -p "$split_tmp_dir"

prefix="${split_tmp_dir}/${filename}_part_"

#split
split -d -C "$chunk_size" "$filepath" "$prefix"
if [ $? -ne 0 ]; then
    echo "command 'split' failed"
    rm -r "$split_tmp_dir"
fi

# assign splits to the workers
n_workers=${#worker_arr[@]}
i=0
for split in "${prefix}"*;
do
    worker="${worker_arr[i]}"
    echo "$(basename $split):$worker" >> "$metadata_file"
    # echo "'$split' created"
    i=$(((i+1) % $n_workers))
done

# send splits to each worker
remote_dir="\$DA_MR_HOME/data/inputs/${metafile_name}"
for worker in "${worker_arr[@]}";
do
    worker_ip="$(awk -F: "/^$worker/ {print \$2}" $workerlist)"

    dir=$(ssh $worker_ip "mkdir -p $remote_dir; echo $remote_dir")
    sftp $worker_ip <<EOF
$(awk -F: "\$2~/$worker/ {print \"put\", \"${split_tmp_dir}\" \"/\" \$1, \"$dir\"}" "$metadata_file")
bye
EOF
    # insert dir infront of filename
    sed -i "s!.*:${worker}!${dir}/&!g" $metadata_file
done

# get a list of pairs (worker_name:n_sent_split) from the input metainfo file
workers_sent_splits=$(awk -F: '{print $2}' "$metadata_file" | sort | uniq -c |
                        awk '{print $2":"$1}')

# Update the worker_n_split file
for pair in $workers_sent_splits;
do
    # echo "$pair"
    worker="$(echo $pair | cut -f1 -d: )"
    val="$(echo $pair | cut -f2 -d: )"
    # echo "worker = "$worker", val = "$val""
    old_val="$(awk -F: "/^$worker/ {print \$2}" $workers_n_splits)"
    new_val="$(( val + old_val ))"
    # echo "updating "$worker"'s "$old_val" to "$new_val""
    sed -i "s/${worker}:.*/${worker}:$new_val/g" $workers_n_splits
done

# remove tmp dir
rm -r "$split_tmp_dir"

echo ""
echo "$filepath is distributed"
echo "  $metadata_file:"
cat $metadata_file
echo "  $workers_n_splits:"
cat $workers_n_splits