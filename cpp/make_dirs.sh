#!/bin/sh

dir="../matrices"
op_path="../alv_matrices"

for file in "$dir"/*; do
	name=${file%.*}
	ext=${file##*.}
	if [ $ext != "gz" ]; then
		name=${name%_*}
		#dir_name=${dir_name##*/}
		#mkdir -p $op_path/$dir_name
		#cp $file $op_path/$dir_name
	fi
	dir_name=${name##*/}
        mkdir -p $op_path/$dir_name
        cp $file $op_path/$dir_name
done