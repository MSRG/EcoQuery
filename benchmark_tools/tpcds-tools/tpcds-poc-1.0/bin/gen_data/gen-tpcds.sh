#!/bin/bash

# usage for the script
usage()
{
    echo -e "$(basename "$0") \033[49;32;1m data_size data_dir \033[0m"
    echo -e '\033[1m[USAGE]\033[0m'
    echo -e '    Gens tpcds data at the specified size under the specified data directory.'
    echo -e '\033[1m[OPTIONS]\033[0m'
    echo -e '    \033[49;32;1m-h\033[0m : print this help.'
    echo -e '    \033[49;32;1mdata_size\033[0m : 1 for 1GB, 2 for 2GB, ...'
    echo -e '    \033[49;32;1mdata_dir\033[0m : data directory to store all the generated table data,'\
            'it will be created if not exist'
    echo -e '\033[1m[RETURN]\033[0m'
}

if [[ $# -lt 2 ]] || [[ "$1" == "-h" ]]; then
    usage
    exit 1
fi

_GEN_FILE_DIR_=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

. "${_GEN_FILE_DIR_}"/../common_info.sh

dbgen_path=${PROJECT_ROOT}/thirdparty/tpcds-dbgen
# max chunk size of a file, unit: GB
FILE_CHUNK_SIZE=5


################################################################################
# data size, unit: GB
size=$1
# data_dir for generated data
data_dir=$2


if [[ -z "${size}" ]]; then
    size=1
else
    size=$((size + 0))
fi

# refine the data_dir to be a absolute path
if [[ -z "${data_dir}" ]];then
    data_dir=$(pwd)
elif [[ ! "${data_dir:0:1}" == "/" ]];then
    data_dir="$(pwd)/${data_dir}"
fi
mkdir -p "${data_dir}"

echo "[INFO] gen ${size}GB data under ${data_dir}"


################################################################################
echo "[INFO] generate data..." >&2
cd "${dbgen_path}" || exit

for i in $(seq 1 ${size})
do
    ./dsdgen -scale ${size} -PARALLEL ${size} -CHILD $i -TERMINATE N -FORCE Y -dir ${data_dir}
done

cd "${data_dir}" || exit
rm dbgen_version*.dat || echo "no dbgen_version file"

# replce || with \N
for file in $(ls *.dat)
do
    file_path=${data_dir}/${file}
    echo "[INFO] sed file:${file_path}"
    sed -i 's/^|/\\N|/g;s/|||/|\\N|\\N|/g;s/||/|\\N|/g;s/||/|\\N|/g;s/||/|\\N|/g;s/|$/|\\N/g' ${file_path}
done

du -sh *.dat
echo "[INFO] Data generation completed."
