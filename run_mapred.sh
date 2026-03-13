#! /bin/bash

if [[ $# -ne 3 ]]; then
    echo -e "Usage: $0 <mapred_exec_path> <input metadata file> <output metadata file>"
    exit 1;
fi

mrexec_path="$1"
# master_exec_path="$2"
input_metadata_file="$2"
output_metadata_file="$3"
worker_list="$DIRECT_ACCESS_MR_HOME/workers"
mrexec_name="$(basename $1)"


if [ ! -f "$mrexec_path" ]; then
    echo "Error: mapred executable '$mrexec_path' not found."
    exit 1
fi

# copy the mapreduce executable to workers
# remote_dir="\$DIRECT_ACCESS_MR_HOME/mapred_bin"
for worker_ip in $(cat $worker_list | awk -F: '{print $2}');
do
    # dir=$(ssh $worker_ip "mkdir -p $remote_dir; echo $remote_dir")
    remote_dir=$(ssh $worker_ip "echo \$DIRECT_ACCESS_MR_HOME/mapred_bin")
    remote_path="$remote_dir/$mrexec_name"

    sftp $worker_ip <<EOF
put $mrexec_path $remote_path
bye
EOF
done

# run mastar
# $master_exec_path $worker_list $mrexec_name $input_metadata_file $output_metadata_file
bin/master $worker_list $mrexec_name $input_metadata_file $output_metadata_file