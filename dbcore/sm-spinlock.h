#pragma once

#include <atomic>
#include <cpuid.h>
#include <immintrin.h>
#include <x86intrin.h>
#include "sm-coroutine.h"

#define MIN_DELAY_MICROSEC 1L

#define DELAY(n)                       \
  do {                                 \
    volatile int x = 0;                \
    for (int i = 0; i < (n); ++i) x++; \
  } while (0)

constexpr int kExpBackoffBase = 4000;
constexpr int kExpBackoffLimit = 32000;
constexpr int kExpBackoffMultiplier = 2;

namespace ermia {
  void microdelay(long microsec);

  class TATAS {
    std::atomic<uint64_t> lock_;
    uint64_t attempt;
    uint64_t success;
   public:
    TATAS() {
      std::atomic_init(&lock_, 0ull);
      attempt = 0;
      success = 0;
    }
    void lock();
    void unlock();
    uint64_t get_attempt();
    uint64_t get_success();
  };

  class Spinlock {
   private:
    std::atomic_flag flag;

   public:
    Spinlock() : flag(ATOMIC_FLAG_INIT) {}

    void lock() {
        while (flag.test_and_set(std::memory_order_acquire)) {
            // busy spin until the flag is cleared
        }
    }

    void unlock() {
        flag.clear(std::memory_order_release);
    }
};
}
