#include <gtest/gtest.h>
#include <iostream>
#include <cstdlib>
#include <thread>
#include <vector>
#include "BucketStructs.h"
#include "MultiBucketQueue.h"

using PQElement = std::tuple<uint32_t,int>;
using BucketID = mbq::BucketID;

const uint64_t queueNum = 2;
const uint64_t threadNum = 1;
const uint64_t bucketNum = 10;
const uint64_t shift = 1;

// Set the push & pop buffer size to 1.
// This ensures that the MQ pushes and pops like a normal queue
const uint64_t batchSizePop = 1;
const uint64_t batchSizePush = 1;

// Set stickiness to 1 to pick a random queue on each access
const uint64_t stickiness = 1;

const auto emptyPrefetchLambda = [] (uint32_t v) -> void {};

TEST(MBQTest, Increasing) {

    // lambda for getting the shifted priority of a task
    auto getBucketID = [&] (uint32_t v) -> BucketID {
        return (v >> shift);
    };

    // disable prefetching on the last argument
    using MBQ = mbq::MultiBucketQueue<
        decltype(getBucketID),
        decltype(emptyPrefetchLambda),
        std::greater<BucketID>, // we only need to compare the bucket ID
        uint64_t, uint64_t, false
    >;
    
    MBQ mbq(getBucketID, emptyPrefetchLambda, queueNum, threadNum, shift, 
            bucketNum, batchSizePop, batchSizePush, mbq::increasing, stickiness);

    // Hack to assign each thread a [0..threadNum-1] thread ID without
    // an explicit thread pool management.
    // Need to call this at the beginning of the thread task.
    mbq.initTID();

    for (int i = 0; i < 100; i++) {
        mbq.push(i, i);
    }

    // note: relaxed and bucketing --> pop based on min BucketID
    // of each queue, unlike heaps that look at the top element
    for (int i = 0; i < 100; i++) {
        auto item = mbq.pop().get();
        uint64_t prio = std::get<0>(item); 
        uint64_t key = std::get<1>(item); 
        std::cout << i << ": popped (" << prio << ", " << key << ")\n";
    }
    // empty
    EXPECT_FALSE(mbq.pop());
    mbq.stat();
}


TEST(MBQTest, Decreasing) {

    // lambda for getting the shifted priority of a task
    auto getBucketID = [&] (uint32_t v) -> BucketID {
        return (v >> shift);
    };

    // disable prefetching on the last argument
    using MBQ = mbq::MultiBucketQueue<
        decltype(getBucketID),
        decltype(emptyPrefetchLambda),
        std::less<BucketID>, // we only need to compare the bucket ID
        uint64_t, uint64_t, false
    >;
    
    // For decreasing order, need to first set a maximum bucket ID
    // so that the bucket queues start at maxBkt and proceed down.
    uint64_t maxBkt = 100 >> shift;
    MBQ mbq(getBucketID, emptyPrefetchLambda, queueNum, threadNum, shift, 
            bucketNum, batchSizePop, batchSizePush, mbq::decreasing, stickiness, maxBkt);

    // Hack to assign each thread a [0..threadNum-1] thread ID without
    // an explicit thread pool management.
    // Need to call this at the beginning of the thread task.
    mbq.initTID();

    for (int i = 100; i >= 0; i--) {
        mbq.push(i, i);
    }

    // note: relaxed and bucketing --> pop based on min BucketID
    // of each queue, unlike heaps that look at the top element
    for (int i = 100; i >= 0; i--) {
        auto item = mbq.pop().get();
        uint64_t prio = std::get<0>(item); 
        uint64_t key = std::get<1>(item); 
        std::cout << i << ": popped (" << prio << ", " << key << ")\n";
    }
    // empty
    EXPECT_FALSE(mbq.pop());
    mbq.stat();
}
