#! /bin/bash

if [ $# -ne 1 ]; then
	echo "Usage: $0 <shared_dev>"
	exit 1
fi

shared_dev="$1"
tmp_dir="$DIRECT_ACCESS_MR_HOME/tmpdir"
mkdir -p $tmp_dir && sudo mount $shared_dev $tmp_dir

shfile_list="sharedfiles"

for entry in $(cat $shfile_list)
do
    shf_name="$(echo $entry | cut -d':' -f1)"
	size=$(echo $entry | cut -d':' -f2-)

	dd if=/dev/zero of="$tmp_dir/$shf_name" bs="$size" count=1
done

echo "syncing..."
sync

sudo umount $tmp_dir
rmdir $tmp_dir