# CPS
This repo contains implementations for different schedulers.

# Build & Run
```
# after cloning the repo
mkdir CPS/build
cd CPS/build/
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

The `MultiQueue` and the `MultiBucketQueue` both use push & pop batching by default. If task-batching is not wanted, set the batch
sizes to 1 during initialization.

Refer to the unit tests in the `CPS/tests/` folder on details of how these schedulers are used.

### tests/
This folder contains basic unit tests for the queues in `CPS/include/`.
