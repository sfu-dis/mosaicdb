## MosaicDB: Latency-Optimized Main-Memory Database Engine

#### Software dependencies
* cmake
* python2
* [clang; libcxx; libcxxabi](https://github.com/llvm/llvm-project)
* libnuma
* libibverbs
* libgflags
* libgoogle-glog
* liburing

Ubuntu
```
apt-get install -y cmake gcc-10 g++-10 clang-8 libc++-8-dev libc++abi-8-dev
apt-get install -y libnuma-dev libibverbs-dev libgflags-dev libgoogle-glog-dev liburing-dev
```

#### Environment configurations
Make sure you have enough huge pages.

* CoroBase uses `mmap` with `MAP_HUGETLB` (available after Linux 2.6.32) to allocate huge pages. Almost all memory allocations come from the space carved out here. Assuming the default huge page size is 2MB, the command below will allocate 2x MB of memory:
```
sudo sh -c 'echo [x pages] > /proc/sys/vm/nr_hugepages'
```
This limits the maximum for --node-memory-gb to 10 for a 4-socket machine (see below).

* `mlock` limits. Add the following to `/etc/security/limits.conf` (replace "[user]" with your login):
```
[user] soft memlock unlimited
[user] hard memlock unlimited
```
*Re-login to apply.*

--------
#### Build it
We do not allow building in the source directory. Suppose we build in a separate directory:

```
$ mkdir build
$ cd build
$ cmake ../ -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo]
$ make -jN
```

Currently the code can compile under GCC-10/Clang-8 or above. E.g., to use GCC-10, issue the following `cmake` command instead:
```
$ CC=gcc-10 CXX=g++-10 cmake ../ -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo]
```

After `make` there will be 4 executables under `build`:

`benchmarks/ycsb/ycsb_SI_sequential_coro` that runs CoroBase (sequential mode), which is equivalent of ERMIA;

`benchmarks/ycsb/ycsb_SI_simple_coro` that runs CoroBase (optimized 2-level coroutine-to-transaction design) with snapshot isolation (not serializable);

`benchmarks/ycsb/ycsb_SI_hybrid_coro` that runs CoroBase (optimized 2-level coroutine-to-transaction design with loading cold tuples nested) with snapshot isolation (not serializable);

`benchmarks/ycsb/ycsb_SI_nested_coro` that runs CoroBase (fully-nested coroutine-to-transaction design) with snapshot isolation (not serializable);

#### Run example

