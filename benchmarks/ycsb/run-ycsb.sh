#!/bin/bash
# $1 - executable
# $2 - number of threads
# $3 - runtime
# $4 - workload
# $5 - (hot) table size
# $6 - additional parameters, e.g., -ycsb_cold_table_size

if [[ $# -lt 5 ]]; then
    echo "Too few arguments. "
    echo "Usage $0 <executable> <threads> <runtime> <workload> <hot table size> <other parameters>"
    exit
fi

LOGDIR=/dev/shm/$USER/corobase-log
mkdir -p $LOGDIR
trap "rm -f $LOGDIR/*" EXIT

exe=$1; shift
threads=$1; shift
runtime=$1; shift
workload=$1; shift
hot_size=$1; shift

if [ -z ${logbuf_mb+x} ]; then
  logbuf_mb=8
  echo "logbuf_mb is unset, using $logbuf_mb";
else
  echo "logbuf_mb is set to $logbuf_mb";
fi

options="$exe -verbose=1 -threads=$threads -seconds=$runtime \
  -log_data_dir=$LOGDIR -log_buffer_mb=$logbuf_mb -parallel_loading=1 \
  -ycsb_hot_table_size=$hot_size -ycsb_workload=$workload"

echo $options "$@"

$options "$@"
