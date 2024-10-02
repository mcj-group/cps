# cps
This repo contains implementations for different schedulers.

# Build & Run
```
# after cloning the repo
mkdir cps/build
cd cps/build/
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j

# in the build folder, run tests
./tests/all_test

# run a single test case
./tests/all_test --gtest_filter=MQTest.Less
```

# Directories
### include/
This folder contains the queues that can be included as a library into other repos through git submodule. 
* `MultiQueue.h`: Multi-Queue with `std::priority_queue` as underlying structure
* `MultiBucketQueue.h`: Multi Bucket Queue with bucket queues as underlying structure
* `BucketStructs.h`: Contains implementation of buckets and the bucket queue, used by MultiBucketQueue

The `MultiQueue` and the `MultiBucketQueue` both use push & pop batching (batch sizes = 1 by default), prefetching (off by default), and stickiness (1 by default).

Refer to the unit tests in the `cps/tests/` folder on details of how these schedulers are used.

### tests/
This folder contains basic unit tests for the queues in `cps/include/`.
