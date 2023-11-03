## The Art of Latency Hiding in Modern Database Engines (A.K.A. MosaicDB, VLDB '24)
### [Nov 3, 2023] Code and README refactoring in progress, stay tuned.

--------
#### Software dependencies
* cmake
* python2
* gcc-10 or above
* libnuma
* libibverbs
* libgflags
* libgoogle-glog
* liburing

Ubuntu
```
apt-get install -y cmake gcc-10 g++-10 libc++-8-dev libc++abi-8-dev libnuma-dev libibverbs-dev libgflags-dev libgoogle-glog-dev liburing-dev
```

--------
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
#### Build it (WIP)
We do not allow building in the source directory. Suppose we build in a separate directory:

```
$ mkdir build
$ cd build
$ cmake ../ -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo]
$ make
```

The executables will be built under `build/benchmarks`:

`ycsb/ycsb_SI_sequential_coro`: ERMIA;

`ycsb/ycsb_SI_simple_coro`: CoroBase;

`ycsb/ycsb_SI_hybrid_coro`: MosaicDB (selective coroutine);

`ycsb/ycsb_SI_nested_coro`: MosaicDB (nested coroutine);

`ycsb/ycsb_SI_flat_coro`: MosaicDB (flat coroutine);

--------
#### Run example (WIP)
