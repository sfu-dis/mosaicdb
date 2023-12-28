## The Art of Latency Hiding in Modern Database Engines (VLDB '24)

MosaicDB is a research storage engine that systematically hides various types of latency in modern memory-optimized database systems.

See details in our [VLDB 2024 paper](https://www.vldb.org/pvldb/vol17/p577-huang.pdf) below. If you use our work, please cite:

```
The Art of Latency Hiding in Modern Database Engines.
Kaisong Huang, Tianzheng Wang, Qingqing Zhou and Qingzhong Meng.
PVLDB 17(3) (VLDB 2024)
```

### Environment configurations
Step 1: Software dependencies
* cmake
* python2
* gcc-10
* libnuma
* libibverbs
* libgflags
* libgoogle-glog
* liburing

Example for Ubuntu
```
$ sudo apt-get install cmake gcc-10 g++-10 libc++-dev libc++abi-dev libnuma-dev libibverbs-dev libgflags-dev libgoogle-glog-dev liburing-dev
```

Step 2: Make sure you have enough huge pages

MosaicDB uses `mmap` with `MAP_HUGETLB` (available after Linux 2.6.32) to allocate huge pages. Almost all memory allocations come from the space carved out here. Assuming the default huge page size is 2MB, the command below will allocate 2x MB of memory:
```
$ sudo sh -c 'echo [x pages] > /proc/sys/vm/nr_hugepages'
```

Step 3: Set mlock limits. Add the following to `/etc/security/limits.conf` (replace "[user]" with your username):
```
[user] soft memlock unlimited
[user] hard memlock unlimited
```
Step 4: Re-login to apply the changes

--------
### Compile
We do not allow building in the source directory:

```
$ mkdir build
$ cd build
$ cmake ../ -DCMAKE_BUILD_TYPE=[Debug|Release|RelWithDebInfo]
$ make
```
--------
### Run
Required options:
* -log_data_dir=[path] (path to the directory where the log files will be stored)
* -node_memory_gb=[#] (the amount of memory in GB that the node has)
* -seconds=[#]
* -threads=[#]

MosaicDB-specific options:
* -ycsb_read_tx_type=[hybrid-coro|nested-coro|flat-coro] (choose according to the executable)
* -coro_scheduler=2
* -coro_batch_size=[#] (8, by default)
* -coro_cold_queue_size=[#] (16, by default)
* -coro_check_cold_tx_interval=[#] (8, by default)

Pipeline-specific options:
* -ycsb_read_tx_type=[hybrid-coro|nested-coro|flat-coro] (choose according to the executable)
* -coro_scheduler=1
* -coro_batch_size=[#] (16, by default)
* -coro_cold_tx_threshold=[#] (8, by default)

Batch-specific options:
* -ycsb_read_tx_type=[hybrid-coro|nested-coro|flat-coro] (choose according to the executable)
* -coro_scheduler=0
* -coro_batch_size=[#] (16, by default)
* -coro_cold_tx_threshold=[#] (8, by default)

Sequential-specific options:
* -ycsb_read_tx_type=sequential

For more options, please refer to `sm-config.h`.

Example for MosaicDB:
```
$ ./ycsb_SI_hybrid_coro \
-log_data_dir=/mnt/nvme0n1/mosaicdb-log \
-node_memory_gb=50 \
-ycsb_workload=C -ycsb_read_tx_type=hybrid-coro \
-ycsb_hot_table_size=300000000 \
-ycsb_cold_table_size=3000000 \
-ycsb_ops_per_tx=10 \
-ycsb_cold_ops_per_tx=2 \
-ycsb_ops_per_hot_tx=10 \
-ycsb_hot_tx_percent=0.9 \
-coro_scheduler=2 \
-coro_batch_size=8 \
-coro_cold_queue_size=16 \
-coro_check_cold_tx_interval=8 \
-seconds=10 \
-threads=10 
```

Example for sequential:
```
$ ./ycsb_SI_sequential \
-log_data_dir=/mnt/nvme0n1/mosaicdb-log \
-node_memory_gb=50 \
-ycsb_workload=C \
-ycsb_read_tx_type=sequential \
-ycsb_hot_table_size=300000000 \
-ycsb_cold_table_size=3000000 \
-ycsb_ops_per_tx=10 \
-ycsb_cold_ops_per_tx=2 \
-ycsb_ops_per_hot_tx=10 \
-ycsb_hot_tx_percent=0.9 \
-seconds=10 \
-threads=10
```
