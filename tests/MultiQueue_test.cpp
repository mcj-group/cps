#include <gtest/gtest.h>
#include <iostream>
#include <cstdlib>
#include <thread>
#include <vector>
#include "MultiQueue.h"

using PQElement = std::tuple<double,int>;

PQElement MQItems[10] = {
    {5, 41},  {6, 11},  {2, 31},  {1, 15}, {7, 45},
    {3, 12},  {9, 23},  {4, 35},  {8, 12}, {10, 31}
};

const uint64_t queueNum = 2;
const uint64_t threadNum = 1;

// Set the push & pop buffer size to 1.
// This ensures that the MQ pushes and pops like a normal queue
const uint64_t batchSizePop = 1;
const uint64_t batchSizePush = 1;

const auto emptyPrefetchLambda = [] (uint32_t v) -> void {};

TEST(MQTest, Less) {
    // disable prefetching on 5th argument
    using MQ = mbq::MultiQueue<
        decltype(emptyPrefetchLambda),
        std::less<PQElement>,
        double, int, false
    >;
    
    MQ mq(emptyPrefetchLambda, queueNum, threadNum, batchSizePop, batchSizePush);

    // Hack to assign each thread a [0..threadNum-1] thread ID without
    // an explicit thread pool management.
    // Need to call this at the beginning of the thread task.
    mq.initTID();

    for (int i = 0; i < 10; i++) {
        mq.push(
            std::get<0>(MQItems[i]),    // priority
            std::get<1>(MQItems[i])     // task
        );
    }

    std::vector<PQElement> sortedItems(MQItems, MQItems+10);
    // sort in descending priority order
    std::sort(
        sortedItems.begin(), sortedItems.end(), 
        std::greater<PQElement>()
    );

    double prio;
    int item;
    for (int i = 0; i < 10; i++) {
        std::tie(prio, item) = mq.pop().get();
        EXPECT_EQ(std::get<0>(sortedItems[i]), prio);
        EXPECT_EQ(std::get<1>(sortedItems[i]), item);
    }
}

TEST(MQTest, Greater) {
    // disable prefetching on the last argument
    using MQ = mbq::MultiQueue<
        decltype(emptyPrefetchLambda), 
        std::greater<PQElement>, 
        double, int, false
    >;
    
    MQ mq(emptyPrefetchLambda, queueNum, threadNum, batchSizePop, batchSizePush);
    for (int i = 0; i < 10; i++) {
        mq.push(
            std::get<0>(MQItems[i]),    // priority
            std::get<1>(MQItems[i])     // task
        );
    }

    std::vector<PQElement> sortedItems(MQItems, MQItems+10);
    // sort in ascending priority order
    std::sort(
        sortedItems.begin(), sortedItems.end(), 
        std::less<PQElement>()
    );

    double prio;
    int item;
    for (int i = 0; i < 10; i++) {
        std::tie(prio, item) = mq.pop().get();
        EXPECT_EQ(std::get<0>(sortedItems[i]), prio);
        EXPECT_EQ(std::get<1>(sortedItems[i]), item);
    }
}