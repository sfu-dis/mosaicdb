#include "sm-spinlock.h"

namespace ermia {
  void microdelay(long microsec) {
    if (microsec > 0) {
      struct timeval delay;
      delay.tv_sec = microsec / 1000000L;
      delay.tv_usec = microsec % 1000000L;
      (void) select(0, NULL, NULL, NULL, &delay);
    }
  }

  PROMISE(void) nanodelay(long nanosec) {
    if (nanosec > 0) {
      auto start = std::chrono::steady_clock::now();
      SUSPEND;
      while (std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count() < nanosec) {
        SUSPEND;
      }
    }
  }

  void TATAS::lock() {
    int cas_failure = 0;
    uint64_t seed = (uintptr_t)(&cas_failure);
    auto next_u32 = [&]() {
      seed = seed * 0xD04C3175 + 0x53DA9022;
      return (seed >> 32) ^ (seed & 0xFFFFFFFF);
    };
    next_u32();
    int maxDelay = kExpBackoffBase;
retry:
#ifdef TATAS_STATS
    __atomic_fetch_add(&attempt, 1, __ATOMIC_ACQ_REL);
#endif
    auto locked = lock_.load(std::memory_order_acquire);
    if (locked) {
      int delay = next_u32() % maxDelay;
      maxDelay = std::min(maxDelay * kExpBackoffMultiplier, kExpBackoffLimit);
      DELAY(delay);
      goto retry;
    }

    if (!lock_.compare_exchange_strong(locked, 1ul)) {
      cas_failure++;
      int delay = next_u32() % maxDelay;
      maxDelay = std::min(maxDelay * kExpBackoffMultiplier, kExpBackoffLimit);
      DELAY(delay);
      goto retry;
    }

#ifdef TATAS_STATS
    ++success;
#endif
  }

  PROMISE(void) TATAS::coro_lock() {
    int cas_failure = 0;
    uint64_t seed = (uintptr_t)(&cas_failure);
    auto next_u32 = [&]() {
      seed = seed * 0xD04C3175 + 0x53DA9022;
      return (seed >> 32) ^ (seed & 0xFFFFFFFF);
    };
    next_u32();
    int maxDelay = kExpBackoffBase;
retry:
    auto locked = lock_.load(std::memory_order_acquire);
    if (locked) {
      int delay = next_u32() % maxDelay;
      maxDelay = std::min(maxDelay * kExpBackoffMultiplier, kExpBackoffLimit);
      AWAIT ermia::nanodelay(maxDelay);
      goto retry;
    }

    if (!lock_.compare_exchange_strong(locked, 1ul)) {
// TODO(khuang): delay is probably not needed in this phase.
      cas_failure++;
      int delay = next_u32() % maxDelay;
      maxDelay = std::min(maxDelay * kExpBackoffMultiplier, kExpBackoffLimit);
      AWAIT ermia::nanodelay(maxDelay);
      goto retry;
    }
  }

  void TATAS::unlock() {
    lock_.store(0, std::memory_order_release);
  }

  uint64_t TATAS::get_attempt() {
    return attempt;
  }

  uint64_t TATAS::get_success() {
    return success;
  }
}
