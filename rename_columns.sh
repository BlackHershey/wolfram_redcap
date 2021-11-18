#!/bin/bash

in_csv=${1}
out_csv=`echo ${in_csv} | sed s/"\.csv"/"_renamed\.csv"/`

rename_mapping=${2}
dos2unix ${rename_mapping}

num_lines=`cat ${rename_mapping} | grep .... | wc -l`

old_header=`head -1 ${in_csv}`
new_header=${old_header}

for i in $(seq 1 1 ${num_lines})
do
    old_name=`head -${i} ${rename_mapping} | tail -1 | cut -d"," -f1`
    new_name=`head -${i} ${rename_mapping} | tail -1 | cut -d"," -f2`

    new_header=`echo ${new_header} | sed s/${old_name}/${new_name}/`
done

cat ${in_csv} | sed s/${old_header}/${new_header}/ > ${out_csv}

exit 0
