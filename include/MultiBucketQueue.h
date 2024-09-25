#pragma once

#include <cstdlib>
#include <iostream>
#include <tuple>
#include <vector>
#include <map>
#include <algorithm>
#include <mutex>
#include <queue>
#include <random>
#include <atomic>
#include <boost/optional.hpp>
#include <boost/align/aligned_allocator.hpp>

#include "BucketStructs.h"

namespace mbq {

#define CACHELINE 64

#define FORWARD_PREFETCH_IDX 1

// #define PERF 1

// ------------------- MultiQueue Wrapper for bucket queues ------------------
// D:           type for lambda that gets the bucketID of a task
// P:           type for prefetcher lambda
// Comparator:  the comparator for deciding min or max heap
// PrioType:    type of priority
// T:           type of the task
// usePrefetch: on-off switch for enabling prefetching during pop
template<
    class D,
    class P,
    typename Comparator, 
    typename PrioType, 
    typename T,
    bool usePrefetch=false,
    bool unpackUnderflow=false
>
class MultiBucketQueue {
    using BktElement = std::tuple<BucketID, T>;
    using PQElement = std::tuple<PrioType, T>;
    Comparator compare;
    D getBucketID;
    P prefetch;

    // Wrapper for individual bucket queues
    struct PQContainer {
        uint64_t pushes = 0;
        uint64_t pops = 0;
        BucketQueue<D, PrioType, T, unpackUnderflow>* bq;

        // Allows atomically peeking of the top bucketID in this queue
        // to decide during pop which of the two queues have the 
        // more prioritized top bucket. 
        std::atomic<BucketID> topBkt{NULL_BKT};

        std::atomic<bool> queueLock{false};
        inline void lock() { while (queueLock.load(std::memory_order_acquire)); }
        inline void unlock() { queueLock.store(false, std::memory_order_release); }
        // Returns true if lock was previously acquired, false otherwise
        // A return value of false means that the lock is successfully acquired
        inline bool tryLock() { 
            // Perform non-stubborn try-locking: does not wait on an acquired lock
            // Already locked, try another queue
            bool flag = queueLock.load(std::memory_order_acquire);
            if (flag) return flag;
            // Not locked, try setting flag
            return !queueLock.compare_exchange_weak(
                flag, true, std::memory_order_acq_rel, std::memory_order_acquire);
        }

        ~PQContainer() {
            bq->del();
            free(bq);
        }

    } __attribute__((aligned (CACHELINE)));

    // The list of bucket queues
    std::vector<
        PQContainer,
        boost::alignment::aligned_allocator<PQContainer, CACHELINE>
    > queues;
    uint64_t numQueues;

    uint64_t numBuckets;
    uint64_t delta;
    BucketID UNDER_BKT;

    // Termination:
    //  - numIdle records the number of threads that believe
    //    there are no more work to do.
    //  - numEmpty records number of queues that are empty
    uint64_t numThreads;
    std::atomic<uint64_t> numIdle{0};
    std::atomic<uint64_t> numEmpty;

    // Manages the push&pop buffers for each thread.
    // Each thread indexes their container with the 
    // thread_local tID.
    struct BufContainer {
        std::vector<PQElement> pushBuffer;
        std::vector<T> popBuffer;
        uint32_t popBufIdx;
        BucketID poppedBkt;

        BufContainer(uint32_t pushBufSize, uint32_t popBufSize) {
            pushBuffer.reserve(pushBufSize);
            popBuffer.reserve(popBufSize);
            popBufIdx = 0;
            poppedBkt = 0;
        }
        
    } __attribute__((aligned (CACHELINE)));
    std::vector<
        BufContainer,
        boost::alignment::aligned_allocator<BufContainer, CACHELINE>
    > localBufs;

    // TODO: dynamically change
    uint64_t batchPopNum;
    uint64_t batchPushNum;
    uint64_t stickiness;
    
public:

    MultiBucketQueue(D d, P p, uint64_t nQ, uint64_t nT,
                     uint64_t shift, uint64_t nB,
                     uint64_t batchSizePop, uint64_t batchSizePush,
                     BucketOrder order, uint64_t s=1, BucketID maxBkt=0)
        : getBucketID(d)
        , prefetch(p)
        , queues(nQ)
        , numQueues(nQ)
        , numBuckets(nB)
        , delta(shift)
        , numThreads(nT)
        , numEmpty(nQ)
        , batchPopNum(batchSizePop)
        , batchPushNum(batchSizePush)
        , stickiness(s)
    {
        assert(batchSizePop >= 1);
        assert(batchSizePush >= 1);
        assert(nT >= 1);
        assert(nQ > 1);
        assert(nB > 3); // underflow + normal bkt + overflow
        for (uint64_t i = 0; i < numQueues; i++) {
            queues[i].bq = new BucketQueue<D, PrioType, T, unpackUnderflow>(
                d, order, numBuckets, batchPopNum, delta, maxBkt);
        }
        for (uint64_t i = 0 ; i < numThreads; i++) {
            localBufs.push_back(BufContainer(batchSizePush, batchSizePop));
        }
        UNDER_BKT = (order == increasing) ? (-1) : (INT64_MAX - 1);
    };

    // Each thread initializes its own tID by calling initTID()
    // before starting the actual processing.
    // Note(Ray): this is a simple hack to get 0 to (threadNum-1)
    // tIDs as c++ pthreads allocate arbitrary thread IDs
    inline static thread_local uint32_t tID;
    inline static thread_local uint32_t lastPickedQueue;
    inline static thread_local uint64_t stickyCnt = 1;
    std::atomic<uint32_t> id = 0;
    void initTID() { 
        tID = id.fetch_add(1);
        lastPickedQueue = tID;
    }

    // Selects a random queue amongst the list of queues
#ifdef PERF
    uint64_t __attribute__ ((noinline)) threadLocalRandom() {
#else
    inline uint64_t threadLocalRandom() {
#endif
        static thread_local uint64_t x = pthread_self();
        uint64_t z = (x += UINT64_C(0x9E3779B97F4A7C15));
        z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
        return (z ^ (z >> 31)) % numQueues;
    }

    // Performs actual insertion of tasks in the push buffer
#ifdef PERF
    void __attribute__ ((noinline)) pushInternal() {
#else
    inline void pushInternal() {
#endif
        auto& pushBuffer = localBufs[tID].pushBuffer;
        uint64_t pushSize = pushBuffer.size();
        if (pushSize == 0) return;

        uint64_t queue;

        // Stickiness: try to lock the last used queue
        bool queueAcquired = false;
        if (stickyCnt < stickiness) {
            if (!queues[lastPickedQueue].tryLock()) {
                // successfully locked, continue using the lastPickedQueue
                queueAcquired = true;
                queue = lastPickedQueue;
                stickyCnt++;
            }
        }

        // If the last used queue is locked, go with the normal procedure
        // Select and lock a random queue
        while (!queueAcquired) {
            queue = threadLocalRandom();
            if (!queues[queue].tryLock()) {
                queueAcquired = true;
                stickyCnt = 1; // reset the count
                lastPickedQueue = queue;
            }
        }
        auto& q = queues[queue];

        // Update numEmpty status
        if (q.bq->empty()) {
            numEmpty.fetch_sub(1, std::memory_order_acq_rel);
        }
        
        q.bq->pushBatch(0, pushSize, pushBuffer);
        q.pushes += pushSize;

        // Update topBkt if needed
        BucketID m = q.topBkt.load(std::memory_order_acquire);
        BucketID cur = q.bq->getTopBkt();
        if (cur != m)
            q.topBkt.store(cur, std::memory_order_release);

        q.unlock();
        pushBuffer.clear();
    }


    // Push the task into local pushBuffer until it fills up
#ifdef PERF
    void __attribute__ ((noinline)) push(PrioType prio, T key) {
#else
    inline void push(PrioType prio, T key) {
#endif
        // Each thread has a local buffer to push to.
        // pushBufSize can be dynamically adjusted for smaller push batch sizes,
        // but the max size of the buffer is always batchPushNum.
        auto& pushBuffer = localBufs[tID].pushBuffer;
        pushBuffer.push_back({prio, key});
        if (pushBuffer.size() < batchPushNum - 1) return;

        pushInternal();
    }

    // Simplified Termination detection idea from the 2021 MultiQueue paper
    // url: https://arxiv.org/abs/2107.01350 
    // Repeatedly try popping and stop when numIdle >= threadNum,
    // That is, stop when all threads agree that there are no more work
    inline boost::optional<PQElement> pop() {
        auto item = popInternal();
        if (item) return item;

        // increment count and keep on trying to pop
        uint64_t num = numIdle.fetch_add(1, std::memory_order_acq_rel) + 1;
        do {
            item = popInternal();
            if (item) break;
            if (num >= numThreads) return boost::none;
            
            num = numIdle.load(std::memory_order_relaxed);

        } while (true);

        numIdle.fetch_sub(1, std::memory_order_acq_rel);
        return item;
    }

    // Performs actual popping of tasks
    inline boost::optional<PQElement> popInternal() {
        // Once all the tasks in the pop buffer is consumed,
        // perform another batch pop from the bucket queue
        auto& buf = localBufs[tID];
        auto& popBuffer = buf.popBuffer;
        auto& popBufIdx = buf.popBufIdx;
        if (popBufIdx < popBuffer.size()) {
            if (usePrefetch) {
                // Prefetch the task at FORWARD_PREFETCH_IDX ahead of 
                // the task being popped, such that its priority
                // becomes available in cache when popped.
                uint32_t prefetchIdx = popBufIdx + FORWARD_PREFETCH_IDX;
                if (prefetchIdx < popBuffer.size())
                    prefetch(popBuffer[prefetchIdx]);
            }
            PrioType p = (buf.poppedBkt == UNDER_BKT) 
                ? ((PrioType)getBucketID(popBuffer[popBufIdx]) << delta)
                : ((PrioType)buf.poppedBkt << delta);
            PQElement task = {p, popBuffer[popBufIdx++]};
            return task;
        }

        // Empty the push buffer before trying to pop.
        // Note: instead of directly popping tasks from the 
        // push buffer as in previous work, we follow
        // the global priority ordering more closely by 
        // updating the MultiQueue more frequently.
        // This way, the tasks in a thread's local push buffer
        // becomes visible to other threads earlier.
        // This reduces potential priority drift amonst threads
        // at the cost of higher communication costs.
        pushInternal();

        popBufIdx = 0;
        popBuffer.clear();
        uint64_t poppingQueue = numQueues;
        while (true) {

            // Stickiness: try to pick the last used queue
            bool queueAcquired = false;
            if (stickyCnt < stickiness) {
                if (!queues[lastPickedQueue].tryLock()) {
                    // successfully locked, continue using the lastPickedQueue
                    queueAcquired = true;
                    poppingQueue = lastPickedQueue;
                    stickyCnt++;
                }
            }

            // If the last used queue is locked, go with the normal procedure
            // Pick the higher priority max of queue i and j
            if (!queueAcquired) {
                uint64_t i = threadLocalRandom();
                uint64_t j = threadLocalRandom();
                while (j == i) {
                    j = threadLocalRandom();
                }

                BucketID bI = queues[i].topBkt.load(std::memory_order_acquire);
                BucketID bJ = queues[j].topBkt.load(std::memory_order_acquire);

                // check if there are no tasks available
                if (bI == NULL_BKT && bJ == NULL_BKT) {
                    uint64_t emptyQueues = numEmpty.load(std::memory_order_acquire);
                    if (emptyQueues >= queues.size()) break;
                    else continue;
                }

                // Prioritize the queue with that has more prioritized top bucket
                // and try to lock it
                if (bI != NULL_BKT && bJ != NULL_BKT) {
                    poppingQueue = compare(bJ, bI) ? i : j;
                } else if (bJ == NULL_BKT) {
                    poppingQueue = i;
                } else {
                    poppingQueue = j;
                }
                if (queues[poppingQueue].tryLock()) continue;
                queueAcquired = true;
                stickyCnt = 1;
                lastPickedQueue = poppingQueue;
            }

            // lock acquired, perform popping

            auto& q = queues[poppingQueue];
            if (q.bq->empty()) {
                q.unlock();
                continue;
            }

            auto res = q.bq->popBatch(popBuffer);
            uint64_t size = std::get<0>(res);
            q.pops += size;

            // Update this queue's topBkt if necessary
            BucketID cur = q.bq->getTopBkt();
            BucketID m = q.topBkt.load(std::memory_order_acquire);
            if (cur != m) {
                q.topBkt.store(cur, std::memory_order_release);
                if (cur == NULL_BKT) // empty
                    numEmpty.fetch_add(1, std::memory_order_acq_rel);
            }
            q.unlock();

            if (size == 0) continue;

            // return the first task in the pop buffer
            BucketID poppedBkt = std::get<1>(res);
            buf.poppedBkt = poppedBkt;
            PrioType p = (poppedBkt == UNDER_BKT) 
                ? ((PrioType)getBucketID(popBuffer[popBufIdx]) << delta)
                : ((PrioType)poppedBkt << delta);
            PQElement task = {p, popBuffer[popBufIdx++]};
            return task;
        }
        
        return boost::none;
    }

    // Note: this is only called at the end of algorithm,
    // so it is not lock protected
    inline void stat() const {
        int totalPushes = 0, totalPops = 0, totalUnpacks = 0;
        std::vector<uint64_t> cnts(numBuckets, 0);
        std::vector<uint64_t> sizes(numBuckets, 0);
        for (uint64_t i = 0; i < numQueues; i++) {
            totalPushes += queues[i].pushes;
            totalPops += queues[i].pops;
            totalUnpacks += queues[i].bq->numUnpacks;
            for (uint64_t j = 0; j < numBuckets; j++) {
                cnts[j] += queues[i].bq->bkts[j].cnt;
                uint64_t cap = 1 << queues[i].bq->bkts[j].lgCapacity;
                sizes[j] = std::max(sizes[j], cap);
            }
        }
        for (uint i = 0; i < cnts.size(); i++) {
            std::cout << i << ": " << cnts[i] << ", max " << sizes[i] <<"\n";
        }
        std::cout << "total pushes "<< totalPushes << "\n";
        std::cout << "total pops "<< totalPops << "\n";
        std::cout << "total unpacks "<< totalUnpacks << "\n";
    }
    
    // Note: this is only called at the end of algorithm as a
    // sanity check, therefore it is not lock protected.
    inline bool empty() const {
        for (uint i = 0; i < numQueues;i++) {
            if (!queues[i].bq->empty()) {
                return false;
            }
        }
        return true;
    }

};

} // namespace mbq