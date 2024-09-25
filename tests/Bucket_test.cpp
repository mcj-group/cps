#include <gtest/gtest.h>
#include <iostream>
#include <cstdlib>
#include <thread>
#include <vector>
#include "BucketStructs.h"

uint64_t keys[10] = {5, 7, 1, 3, 8, 6, 2, 9, 4, 10};

TEST(BucketTest, Pop) {
    mbq::Bucket<uint32_t> bkt;

    // normal push & pops

    for (int i = 0; i < 128; i++) {
        EXPECT_TRUE(bkt.push(i));
    }

    EXPECT_EQ(bkt.size(), 128);

    for (int i = 0; i < 128; i++) {
        EXPECT_EQ(i, bkt.pop());
    }

    // head == tail
    EXPECT_EQ(UINT32_MAX, bkt.pop());
    EXPECT_EQ(bkt.size(), 0);
}

TEST(BucketTest, Upsize) {
    mbq::Bucket<uint32_t> bkt;

    // should already have 128 capacity

    // push above limit
    for (int i = 0; i < 10; i++) {
        EXPECT_TRUE(bkt.push(keys[i]));
    }

    for (int i = 10; i < 200; i++) {
        EXPECT_TRUE(bkt.push(i));
    }

    EXPECT_EQ(bkt.size(), 200);
    EXPECT_EQ(bkt.lgCapacity, 8);

    // pop everything
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(keys[i], bkt.pop());
    }

    for (int i = 10; i < 200; i++) {
        EXPECT_EQ(i, bkt.pop());
    }

    // empty
    EXPECT_EQ(UINT32_MAX, bkt.pop());
    EXPECT_EQ(bkt.size(), 0);
}

// Test the bucket's ring-buffer attribute
TEST(BucketTest, Wrap) {
    mbq::Bucket<uint32_t> bkt;

    // should already have 128 capacity

    // normal push & pops
    for (int i = 0; i < 127; i++) {
        EXPECT_TRUE(bkt.push(i));
    }

    for (int i = 0; i < 20; i++) {
        EXPECT_EQ(i, bkt.pop());
    }

    // should now have room for 20 keys
    for (int i = 0; i < 20; i++) {
        EXPECT_TRUE(bkt.push(i+20));
    }

    // at capactiy
    EXPECT_EQ(bkt.size(), 127);

    // pop everything
    for (int i = 20; i < 127; i++) {
        EXPECT_EQ(i, bkt.pop());
    }

    for (int i = 0; i < 20; i++) {
        EXPECT_EQ(i+20, bkt.pop());
    }

    // empty
    EXPECT_EQ(UINT32_MAX, bkt.pop());
    EXPECT_EQ(bkt.lgCapacity, 7);
}

// Test the bucket queue's APIs
TEST(BucketQueueTest, Pop) {
    std::vector<uint64_t> dists(100);
    for (int i = 0; i < 100; i++) {
        dists[i] = i;
    }

    uint64_t shift = 1;
    auto func = [&] (uint64_t v) -> const uint64_t {
        auto d = dists[v];
        return (d >> shift);
    };

    // 100 elements with delta of 2 --> 50 buckets
    // allow 20 total buckets for now
    uint64_t batchSizePop = 1;
    mbq::BucketQueue<decltype(func), uint64_t, uint64_t> bkts(func, mbq::increasing, 20, batchSizePop, shift);

    // normal push & pops
    for (int i = 0; i < 100; i++) {
        bkts.push(func(dists[i]), i);
    }


    for (int i = 0; i < 100; i++) {
        EXPECT_EQ(i, std::get<0>(bkts.pop()));
    }

    // empty
    EXPECT_EQ(UINT64_MAX, std::get<0>(bkts.pop()));


    // underflow
    for (int i = 0; i < 20; i++) {
        bkts.push(func(dists[i]), i);
    }
    for (int i = 0; i < 20; i++) {
        EXPECT_EQ(i, std::get<0>(bkts.pop()));
    }

    // empty
    EXPECT_EQ(UINT64_MAX, std::get<0>(bkts.pop()));
}