#!/bin/bash

trap 'kill $(jobs -p)' EXIT

UC=/var/run/secrets/user_credentials
prefect_user_token=$(cat $UC/prefectuser)
prefect_runner_token=$(cat $UC/prefectrunner)

# Start local Dask cluster
CG=/sys/fs/cgroup
quota=$(cat $CG/cpu/cpu.cfs_quota_us)
period=$(cat $CG/cpu/cpu.cfs_period_us)
membytes=$(cat $CG/memory/memory.stat | sed -nE 's@^hierarchical_memory_limit *@@p')
nproc=$(( 2 * $quota / $period ))
memmb=$(( $membytes / 1024 / 1024 / $nproc ))
echo "Starting dask scheduler with $nproc workers, ${memmb}MB RAM per worker"
dask-scheduler --dashboard-address :8086 &
dask-worker localhost:8786 --nprocs $nproc --memory-limit ${memmb}MB --nthreads 1 &

# Tell Prefect to use local Dask cluster
export PREFECT__ENGINE__EXECUTOR__DEFAULT_CLASS=prefect.engine.executors.DaskExecutor
export PREFECT__ENGINE__EXECUTOR__DASK__ADDRESS='tcp://127.0.0.1:8786'

# Build flows
prefect auth login -t $prefect_user_token
python flows.py

# Start prefect agent
prefect agent start -t $prefect_runner_token

