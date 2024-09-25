#pragma once

#include <float.h>
#include <cstring>
#include <boost/optional.hpp>

namespace mbq {

// #define PERF 1
// #define SHRINK 1

// ------------------- Adapted from Julienne  ------------------
// url: https://github.com/jshun/ligra 

#define newA(__E,__n) (__E*) malloc((__n)*sizeof(__E))

typedef int64_t BucketID;
static constexpr BucketID NULL_BKT = INT64_MAX;
static constexpr uint64_t MIN_LG_CAPACITY = 7;
static constexpr uint64_t MIN_CAPACITY = 128;
static constexpr uint64_t LG_CAPACITY_STEP = 1;

template<typename T>
T getEmptyType() {
    if (std::is_integral<T>::value) {
        if (sizeof(T) == 4) return (T)UINT32_MAX;
        if (sizeof(T) == 8) return (T)UINT64_MAX;
    }
    return (T)NULL;
}

template <typename T>
struct StaticArr {
    T* A;
    uint64_t head = 0;
    uint64_t tail = 0;
    uint64_t capacity;

    StaticArr(uint64_t size) : capacity(size) {
        A = newA(T, size);
    }

    uint64_t remain() {
        if (isEmpty()) return 0;
        return (head < tail)
            ? tail - head
            : capacity - head + tail;
    }

    uint64_t getTail() { return tail; }

    void del() { free(A); }

    bool isEmpty() { return head == tail; }

    inline bool push(T val) {
        uint64_t modMask = capacity - 1;
        uint64_t nt = (tail + 1) & modMask;
        if (nt == head) return false;
        A[tail] = val;
        tail = nt;
        return true;
    }

    inline boost::optional<T> pop() {
        if (head == tail) return boost::none;
        T val = A[head];
        uint64_t modMask = capacity - 1;
        head = (head + 1) & modMask;
        return val;
    }
};

// Bucket (ring buffer) with wrap around:
//  - when head and tail meets --> empty
//  - when tail-head == capacity --> full, resize
//  - for both head and tail, max is capacity-1
template <typename T>
struct Bucket {
    T* A;
    uint64_t lgCapacity;
    uint64_t head;
    uint64_t tail;
    uint64_t cnt;
    // static constexpr 
    T EMPTY_KEY = getEmptyType<T>();
    using BktElement = std::tuple<BucketID, T>;

    Bucket() : lgCapacity(MIN_LG_CAPACITY), head(0), tail(0), cnt(0) {
        A = newA(T, (1 << MIN_LG_CAPACITY));
    }

    inline uint64_t size() {
        if (isEmpty()) return 0;
        uint64_t capacity = 1 << lgCapacity;
        return (head < tail)
            ? tail - head
            : capacity - head + tail;
    }

    inline void del() { if (A != NULL) free(A); }

    inline bool isEmpty() { return head == tail; }

#ifdef PERF
    void __attribute__ ((noinline)) upsize() {
#else
    inline void upsize() {
#endif
        uint64_t capacity = 1 << lgCapacity;
        uint64_t mult = 1 << LG_CAPACITY_STEP;
        uint64_t new_capacity = mult * capacity;

        // Since we are here, the original slots should
        // already be completely full.
        A = (T*) realloc(A, new_capacity * sizeof(T));
        assert(A != NULL);
        head = 0;
        tail = capacity;
        lgCapacity += LG_CAPACITY_STEP;
    }

#ifdef PERF
    void __attribute__ ((noinline)) downsize() {
#else
    inline void downsize() {
#endif

        // shrink if only 1/4 of space is occupied
        // and capacity is > MIN_CAPACITY
        uint64_t capacity = 1 << lgCapacity;
        uint64_t remain = size();
        if (remain >= (capacity >> 2) || lgCapacity == MIN_LG_CAPACITY)
            return;

        // check if copying is needed
        uint64_t half = capacity >> 1;
        if (head >= half || tail >= half) {
            if (head > tail) {
                // wrapped around, copy the head portion to the tail
                // before:  A = [--t           h--]
                // after:   A = [h----t           ]
                uint64_t copySize = capacity - head;
                memcpy(A+tail, A+head, copySize*sizeof(T));
                head = 0;
                tail = remain;
            } else if (head < half && tail >= half) {
                // crossed halfway point, copy the portion above half
                // to the start of A
                // before:  A = [     h-----t     ]
                // after:   A = [---t h--         ]
                uint64_t copySize = tail - half;
                memcpy(A, A+half, copySize*sizeof(T));
                tail = copySize;
            } else {
                // simply copy to the start
                // before:  A = [         h----t  ]
                // after:   A = [h----t           ]
                memcpy(A, A+head, remain*sizeof(T));
                head = 0;
                tail = remain;
            }
        }

        // Note: apparently this triggers extra
        //       copying and results in slow down.
        // A = (T*) realloc(A, half * sizeof(T));
        assert(A != NULL);
        lgCapacity--;
    }

    // increment tail and fill in the slot
#ifdef PERF
    bool __attribute__ ((noinline)) push(T val) {
#else 
    inline bool push(T val) {
#endif
        // try to increment tail without surpassing head
        uint64_t modMask = (1 << lgCapacity) - 1;
        uint64_t nt = (tail + 1) & modMask;
        A[tail] = val;

        // head and new tail meets, but here it means 
        // the bucket is full
        if (nt == head) upsize();
        else tail = nt;
        cnt++;
        return true;
    }

    // increment head and return the item
    // loop when the key is not yet filled in
#ifdef PERF
    T __attribute__ ((noinline)) pop() {
#else
    inline T pop() {
#endif
        // try to increment head without surpassing tail
        if (head == tail) return EMPTY_KEY;

        T val = A[head];
        uint64_t modMask = (1 << lgCapacity) - 1;
        head = (head + 1) & modMask;

#ifdef SHRINK
        downsize();
#endif
        return val;
    }

    // For non-integer tasks
#ifdef PERF
    std::tuple<T,bool> __attribute__ ((noinline)) popNonInt() {
#else
    inline std::tuple<T,bool> popNonInt() {
#endif
        // try to increment head without surpassing tail
        if (head == tail) return {0, false};

        T val = A[head];
        uint64_t modMask = (1 << lgCapacity) - 1;
        head = (head + 1) & modMask;

#ifdef SHRINK
        downsize();
#endif
        return {val, true};
    }

#ifdef PERF
    uint64_t __attribute__ ((noinline)) popBatch(uint64_t num, std::vector<T>& popBuffer) {
#else
    inline uint64_t popBatch(uint64_t num, std::vector<T>& popBuffer) {
#endif
        // try to increment head without surpassing tail
        if (head == tail) return 0;

        uint64_t capacity = 1 << lgCapacity;

        // if head > tail, wrapped around, then we
        // only pop up to the capacity to ensure we do not
        // incur extra cache misses
        uint32_t popNum = (head < tail) ? 
            std::min(tail-head, num) : 
            std::min(capacity-head, num);
        uint64_t modMask = capacity - 1;
        popBuffer.insert(popBuffer.end(), A+head, A+head+popNum);
        head = (head + popNum) & modMask;

#ifdef SHRINK
        downsize();
#endif
        return popNum;
    }
};

enum BucketOrder {
    decreasing,
    increasing
};

template <class D, typename PrioType, typename T, bool unpackUnderflow=false>
struct BucketQueue {
public:
    const BucketOrder order; // increase or decreasing
    Bucket<T>* bkts; // list of buckets
    BucketID curBkt; // index of the lowest bucket in current range, no underflow
    BucketID topBktID; // the current bucket id with range calculated, can be underflow
    uint64_t curRange; // the current range of open buckets
    uint64_t numElms; // total number of elements across open buckets
    D getBucketID; // lambda for peeking a task's current priority and its BucketID
    uint64_t numOpenBkts; 
    BucketID overflow; // index for overflow bucket
    BucketID underflow = 0; // index for underflow bucket
    uint64_t popBatchNum;
    uint64_t numUnpacks = 0;
    uint64_t delta;
    BucketID UNDER_BKT; // an ID signifying that the current BucketQueue has tasks in underflow
    T EMPTY_KEY = getEmptyType<T>();

    using BktElement = std::tuple<BucketID, T>;
    using PQElement = std::tuple<PrioType, T>;

    // Create a bucketing structure.
    //   d : map from identifier -> bucket
    //   order : the order to iterate over the buckets
    //   total_buckets: the total buckets to materialize
    //
    //   For an identifier i:
    //   d(i) is the bucket currently containing i
    //   d(i) = UINT64_MAX if i is not in any bucket
    BucketQueue(D d, BucketOrder _order, uint64_t totalBkts, 
            uint64_t _popBatchNum, uint64_t shift, uint64_t maxBkt=0) :
        order(_order), curBkt(1), topBktID(maxBkt), numElms(0), 
        getBucketID(d), numOpenBkts(totalBkts-2), overflow(totalBkts-1),
        popBatchNum(_popBatchNum), delta(shift)
    {
        // Initialize array consisting of the materialized buckets.
        bkts = new Bucket<T>[totalBkts]();
        if (order == increasing) {
            UNDER_BKT = -1;
            curRange = 0;
        } else {
            UNDER_BKT = INT64_MAX - 1;
            curRange = (maxBkt / numOpenBkts) + (maxBkt % numOpenBkts != 0);
        }
    }

    void del() {
        uint64_t total = overflow + 1;
        for (uint64_t i=0; i<total; i++) { bkts[i].del(); }
        free(bkts);
    }

    inline bool empty() {
        return numElms == 0;
    }

    // Returns the next non-empty bucket from the bucket structure. 
    // Called when the current bucket becomes empty
    // When there are no buckets left, return false
#ifdef PERF
    bool __attribute__ ((noinline)) nextBucket() {
#else
    inline bool nextBucket() {
#endif
        // Check underflow first to make sure underflow is 
        // always emptied before any other buckets
        if (!bkts[underflow].isEmpty()) {
            curBkt = underflow;
            return true;
        }
        if (bkts[curBkt].isEmpty() && numElms > 0) {
            curBkt++;

            if (curBkt == overflow) {
                // all the other buckets are empty,
                // unpack from overflow
                for (int i = 0; i < overflow; i++) {
                    bkts[i].head = 0;
                    bkts[i].tail = 0;
                }
                unpack();
                numUnpacks++;
            }
        }
        if (numElms == 0) return false;

        if (!bkts[underflow].isEmpty())
            topBktID = UNDER_BKT;
        else 
            topBktID = (order == increasing)
                ? curRange * numOpenBkts + curBkt - 1
                : curRange * numOpenBkts - curBkt;
        
        return true;
    }

    // insert the task to its corresponding bucket
#ifdef PERF
    void __attribute__ ((noinline)) insertInBucket(BucketID b, T val) {
#else
    inline void insertInBucket(BucketID b, T val) {
#endif
        curBkt = std::min(curBkt, b);
        if (order == increasing) {
            BucketID newTop = curRange * numOpenBkts + curBkt - 1;
            topBktID = std::min(topBktID, newTop);
        } else {
            BucketID newTop = curRange * numOpenBkts - curBkt;
            topBktID = (topBktID == NULL_BKT)
                ? newTop
                : std::max(topBktID, newTop);
        }

        bkts[b].push(val);
    }

    // Once all the open buckets are processed, unpack
    // from the overflow bucket.
    // When this is called, underflow is assumed to be empty
#ifdef PERF
    void __attribute__ ((noinline)) unpack() {
#else
    inline void unpack() {
#endif
        uint64_t m = bkts[overflow].tail;
        
        if (order == increasing) curRange++; // increment range
        else curRange--;

        if (m != numElms) {
            std::cout << "m = " << m << " numElms = " << numElms << std::endl;
            std::cout << "curBkt = " << topBktID << std::endl;
            std::cout << "mismatch" << std::endl;
            exit(0);
        }

        // Re-bucket the keys from overflow.
        // Keys that stay in the overflow are shifted to the start.
        curBkt = 1;
        uint64_t new_tail = 0;
        bool none_in_range = true;
        for (uint64_t i = 0; i < m; i++) {
            T k = bkts[overflow].A[i];
            BucketID bkt = toRange(getBucketID(k));
            if (bkt != overflow) {
                insertInBucket(bkt, k);
                none_in_range = false;
            } else {
                bkts[overflow].A[new_tail] = k;
                new_tail++;
            }
        }
        bkts[overflow].tail = new_tail; 

        //none in range
        if(none_in_range && numElms > 0) {
            // adjust curRange
            if(order == increasing) {
                uint64_t minBkt = UINT64_MAX;
                for (uint64_t i = 0; i < m; i++){
                    uint64_t b = (uint64_t)getBucketID(bkts[overflow].A[i]);
                    if (b < minBkt) minBkt = b;
                }
                curRange = minBkt/numOpenBkts-1; //will be incremented in next unpack call
            } else {
                uint64_t minBkt = 0;
                for (uint64_t i = 0; i < m; i++){
                    uint64_t b = (uint64_t)getBucketID(bkts[overflow].A[i]);
                    if (b > minBkt) minBkt = b;
                }
                curRange = (numOpenBkts+minBkt)/numOpenBkts+1; //will be decremented in next unpack() call
            }
        }
    }

    // Maps the deltaPrio of a task to a corresponding bucket
    // increasing: [curRange*numOpenBkts, (curRange+1)*numOpenBkts)
    // decreasing: [(curRange-1)*numOpenBkts, curRange*numOpenBkts)
    inline BucketID toRange(uint64_t bkt) const {
        if (order == increasing) {
            if (bkt < curRange*numOpenBkts) // underflow
                return underflow;
            return (bkt < (curRange+1)*numOpenBkts) ? ((bkt%numOpenBkts) + 1) : overflow;
        } else {
            if (bkt >= (curRange)*numOpenBkts) // underflow
                return underflow;
            return (bkt >= (curRange-1)*numOpenBkts) ? (numOpenBkts - (bkt%numOpenBkts)) : overflow;
        }
    }

    // Dynamically adjusts the range of mapped buckets
    // when there is a tiny wavefront and higher than normal
    // usage of the underflow bucket.
    // Only used for astar.
#ifdef PERF
    inline void adjustRange() {
#else
    void __attribute__ ((noinline)) adjustRange() {
#endif
        uint32_t h = bkts[underflow].head;
        uint32_t t = bkts[underflow].tail;
        uint32_t lgCapacity = bkts[underflow].lgCapacity;
        uint64_t modMask = (1 << lgCapacity) - 1;
        uint32_t i = h;
        if(order == increasing) {
            BucketID minBkt = INT64_MAX;
            while (i != t){
                BucketID b = getBucketID(bkts[underflow].A[i]);
                if (b < minBkt) minBkt = b;
                i = (i + 1) & modMask;
            }
            curRange = minBkt/numOpenBkts-1; //will be incremented in next unpack call
        } else {
            BucketID minBkt = -1;
            while (i != t){
                BucketID b = getBucketID(bkts[overflow].A[i]);
                if (b > minBkt) minBkt = b;
                i = (i + 1) & modMask;
            }
            curRange = (numOpenBkts+minBkt)/numOpenBkts+1; //will be decremented in next unpack() call
        }

        // Re-bucket the keys to overflow.
        for (int64_t i = 1; i < overflow; i++) {
            while(true) {
                T key = bkts[i].pop();
                if (key == EMPTY_KEY) break;
                bkts[overflow].push(key);
            }
        }

        // Re-bucket the keys in underflow
        // Some keys may be bucketed to underflow again
        // because other threads updated its prios.
        // Therefore we record the current number of keys
        // in the underflow and pop that amount to avoid deadlock
        // or re-adjusting range.
        uint32_t cnt = bkts[underflow].size();
        for(uint32_t p = 0; p < cnt; p++) {
            T key = bkts[underflow].pop();
            if (key == EMPTY_KEY) break;
            BucketID b = toRange(getBucketID(key));
            bkts[b].push(key);
        }
        curBkt = 1;
    }

    // Push a single task
#ifdef PERF
    void __attribute__ ((noinline)) push(BucketID b, T key) {
#else
    inline void push(BucketID b, T key) {
#endif
        BucketID bkt = toRange(b);
        insertInBucket(bkt, key);
        numElms++;

        if (unpackUnderflow) {
            // TODO: change these numbers after testing
            if (numElms >= 5) {
                uint32_t underflowSize = bkts[underflow].size();
                float ratio = (float)underflowSize / (float)numElms;
                float adjustTH = 0.4;
                if (ratio > adjustTH) {
                    adjustRange();
                }
            }
        }
    }

    // Push a batch of task, the tasks are assumed to have PrioType
#ifdef PERF
    void __attribute__ ((noinline)) pushBatch(uint64_t start, uint64_t size, std::vector<PQElement>& pushBuffer) {
#else
    inline void pushBatch(uint64_t start, uint64_t size, std::vector<PQElement>& pushBuffer) {
#endif
        uint64_t idx = start;
        for (uint64_t i = 0; i < size; i++) {
            BucketID deltaPrio = std::get<0>(pushBuffer[idx]) >> delta;
            BucketID bkt = toRange(deltaPrio);
            insertInBucket(bkt, std::get<1>(pushBuffer[idx]));
            idx++;
        }
        numElms += size;

        if (unpackUnderflow) {
            if (numElms >= 5) {
                uint32_t underflowSize = bkts[underflow].size();
                float ratio = (float)underflowSize / (float)numElms;
                float adjustTH = 0.4; // TODO: change this after testing
                if (ratio >= adjustTH) {
                    adjustRange();
                }
            }
        }
    }

    // Pops the key from either the underflow or curBkt
    // if both are empty, then advance nextBucket. 
    // Returns: key, bkt(overall)
#ifdef PERF
    std::tuple<T, BucketID> __attribute__ ((noinline)) pop() {
#else
    inline std::tuple<T, BucketID> pop() {
#endif
        while (true) {
            uint64_t key = bkts[curBkt].pop();
            if (key != EMPTY_KEY) {
                numElms--;
                if (numElms == 0) topBktID = NULL_BKT;
                BucketID retBkt;
                if (curBkt == underflow) retBkt = getBucketID(key);
                else retBkt = (order == increasing)
                    ? curRange*numOpenBkts+curBkt-1
                    : curRange*numOpenBkts-curBkt;
                return {key, retBkt};
            }

            // curBkt is empty
            if (!nextBucket()) {
                // underflow and buckets are all empty
                return {EMPTY_KEY, NULL_BKT};
            }

            // numElms > 0, retry
        }
        return {EMPTY_KEY, NULL_BKT};
    }

    // For non-integer tasks
#ifdef PERF
    std::tuple<T, BucketID> __attribute__ ((noinline)) popNonInt() {
#else
    inline std::tuple<T, BucketID> popNonInt() {
#endif
        while (true) {
            auto item = bkts[curBkt].popNonInt();
            T key = std::get<0>(item);
            bool success = std::get<1>(item);
            if (success) {
                numElms--;
                if (numElms == 0) topBktID = NULL_BKT;
                BucketID retBkt;
                if (curBkt == underflow) retBkt = getBucketID(key);
                else retBkt = (order == increasing)
                    ? curRange*numOpenBkts+curBkt-1
                    : curRange*numOpenBkts-curBkt;
                return {key, retBkt};
            }

            // curBkt is empty
            if (!nextBucket()) {
                // underflow and buckets are all empty
                return {0, NULL_BKT};
            }

            // numElms > 0, retry
        }
        return {0, NULL_BKT};
    }

#ifdef PERF
    std::tuple<uint64_t, BucketID> __attribute__ ((noinline)) popBatch(std::vector<T>& popBuffer) {
#else
    inline std::tuple<uint64_t, BucketID> popBatch(std::vector<T>& popBuffer) {
#endif
        while (true) {
            if (bkts[curBkt].size() != 0) {
                BucketID retBkt;
                if (curBkt == underflow) retBkt = UNDER_BKT;
                else retBkt = (order == increasing)
                    ? curRange*numOpenBkts+curBkt-1
                    : curRange*numOpenBkts-curBkt;
                uint64_t numPopped = bkts[curBkt].popBatch(popBatchNum, popBuffer);
                numElms -= numPopped;
                if (numElms == 0) topBktID = NULL_BKT;
                return {numPopped, retBkt};
            }

            // curBkt is empty
            if (!nextBucket()) {
                // underflow and buckets are all empty
                return {0, NULL_BKT};
            }

            // numElms > 0, retry
        }
        return {0, NULL_BKT};
    }

    inline BucketID getTopBkt() { return topBktID; }
};

} // namespace mbq