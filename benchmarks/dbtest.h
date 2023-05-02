#pragma once

#include <functional>
#include "bench.h"

void bench_main(int argc, char **argv, std::function<void(ermia::Engine *)> test_fn);
