#include <gtest/gtest.h>

#include "../../dbcore/dlog.h"
#include "../../macros.h"

void insert_one() {
  ermia::dlog::tls_log tlog;
  tlog.initialize("./", 0, 0, 1);

  ermia::dlog::log_block zero_lb;
  zero_lb.csn = 100;
  tlog.insert(&zero_lb);
  tlog.uninitialize();

  int fd = open("tlog-00000000-00000000", O_RDWR, 0644);
  ermia::dlog::log_block read_lb;
  read_lb.csn = 999;
  read_lb.payload_size = 888;

  int ret = read(fd, &read_lb, sizeof(read_lb));
  ALWAYS_ASSERT(ret == sizeof(read_lb));
  ALWAYS_ASSERT(read_lb.csn == 100);
  ALWAYS_ASSERT(read_lb.payload_size == 0);
  close(fd);
}

void insert_many() {
  ermia::dlog::tls_log tlog;
  tlog.initialize("./", 0, 0, 1);
  uint64_t log_size = 0;

  for (uint32_t i = 0; i < 10000; ++i) {
    ermia::dlog::log_block *lb = (ermia::dlog::log_block *)malloc(sizeof(ermia::dlog::log_block) + 2 * i);
    lb->csn = i;
    lb->payload_size = i * 2;
    tlog.insert(lb);
    log_size += lb->total_size();
    printf("Record payload: %u, total: %u\n", lb->payload_size, lb->total_size());
    free(lb);
  }
  tlog.uninitialize();
  printf("total size = %lu\n", log_size);

/*
  int fd = open("tlog-00000000-00000000", O_RDWR, 0644);
  ermia::dlog::log_block read_lb;
  read_lb.csn = 999;
  read_lb.payload_size = 888;

  int ret = read(fd, &read_lb, sizeof(read_lb));
  ALWAYS_ASSERT(ret == sizeof(read_lb));
  ALWAYS_ASSERT(read_lb.csn == 100);
  ALWAYS_ASSERT(read_lb.payload_size == 0);
  close(fd);
  */
}


int main(int argc, char **argv) {
  insert_one();
  insert_many();
}

