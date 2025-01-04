#ifndef CDM_H
#define CDM_H
#include "../Utils/BOBHash32.h"
#include <cmath>
#include <cstdint>

#include "./Sketch.h"

class CDM : public Sketch{
   public:
    uint32_t INDEX_SEED;
    uint32_t FP_SEED;
    uint32_t EMPTY_SLOT;
    int bucket_count;
    int bucket_size;
    int bucket_slot;
    int total_slot;
    int guard_count;
    uint32_t guard_count_bit;
    uint64_t startTime;
    uint32_t window;

    BOBHash32 hash_loc;
    BOBHash32 hash_fp;

    uint32_t* guard_max_slots;
    uint32_t* buckets_slots;
    uint64_t* timestamp_slots;

   public:
    CDM () {

    }

    void init(double _1, double memory, int _bucket_slot, double _2, uint32_t _FP_SEED, uint32_t _INDEX_SEED, uint64_t curTime, uint32_t window) {
        INDEX_SEED = _INDEX_SEED;
        FP_SEED = _FP_SEED;
        EMPTY_SLOT = UINT32_MAX; //UINT32_MAX;

        // sliding window attributes
        this->window = window;

        bucket_slot = std::max(_bucket_slot, 1);
        bucket_size = bucket_slot;
        bucket_count = round(memory / (64 * bucket_size + 32));
        guard_count = bucket_count;
        guard_count_bit = log2(guard_count);
        if ((1 << guard_count_bit) != guard_count){
            guard_count_bit = 1 << (guard_count_bit + 1);
        }else{
            guard_count_bit = 1 << guard_count_bit;
        }
        guard_count_bit--;
        total_slot = bucket_count * bucket_size;
        guard_max_slots = new uint32_t[guard_count];
        for (int i = 0; i < guard_count; ++i) {
            guard_max_slots[i] = UINT32_MAX;
        }
        buckets_slots = new uint32_t[total_slot];
        timestamp_slots = new uint64_t[total_slot];

        hash_fp.initialize(FP_SEED);
        hash_loc.initialize(INDEX_SEED);

        startTime = curTime;

        for (int i = 0; i < total_slot; ++i) {
            buckets_slots[i] = EMPTY_SLOT;
            timestamp_slots[i] = startTime;
        }
    }

    ~CDM() {
        delete[] buckets_slots;
        delete[] timestamp_slots;
        delete[] guard_max_slots;
    }

    uint32_t query(uint64_t curTime) {
        //uint32_t curTime = static_cast<uint32_t>(curTime64 - startTime);
        double estimated_n = 0.0;
        for (int i = 0; i < bucket_count; ++i) {
            int no_empty_slot = 0;
            uint32_t guard_max = guard_max_slots[i];
            for (int j = 0; j < bucket_slot; ++j) {
                if (buckets_slots[i * bucket_size + j] != EMPTY_SLOT && ((curTime - timestamp_slots[i * bucket_size + j]) <= window)) {
                    no_empty_slot++;
                }
            }
            estimated_n += (std::pow(2, 32) * no_empty_slot) / (1.0 * guard_max);
        }
        return static_cast<uint32_t>(std::round(estimated_n));
    }

    void manualDelete(uint32_t ele, uint64_t curTime) {
        uint32_t fingerprint = hash_fp.run((char*) &ele, sizeof(uint32_t));
        fingerprint -= (fingerprint == UINT32_MAX);

        int guard_index = fingerprint % guard_count;
        uint32_t& guard_max = guard_max_slots[guard_index];
        if (fingerprint >= guard_max) {
            return;
        }
        uint32_t fp_low = hash_loc.run((char*) &ele, sizeof(uint32_t)) & guard_count_bit;
        fingerprint &= ~guard_count_bit;
        fingerprint |= fp_low;
        uint32_t bucket_index = guard_index;
        uint32_t slot_index = bucket_index * bucket_size;

        // uint32_t bucket_index = hash_loc.run((char*) &ele, sizeof(uint32_t)) % bucket_count;
        // uint32_t slot_index = bucket_index * bucket_size;
        for (int i = 0; i < bucket_slot; ++i) {
            if (buckets_slots[slot_index + i] == fingerprint) {
                buckets_slots[slot_index + i] = EMPTY_SLOT;
                timestamp_slots[slot_index + i] = curTime;
                return;
            }
        }
    }

    void insert(uint32_t ele, uint64_t curTime) {
        //uint32_t curTime = static_cast<uint32_t>(curTime64 - startTime);
        uint64_t outdatedTime = curTime - this->window;

        uint32_t fingerprint = hash_fp.run((char*) &ele, sizeof(uint32_t));
        fingerprint -= (fingerprint == UINT32_MAX);

        int guard_index = fingerprint % guard_count;
        uint32_t& guard_max = guard_max_slots[guard_index];
        if (fingerprint >= guard_max) {
            return;
        }
        uint32_t fp_low = hash_loc.run((char*) &ele, sizeof(uint32_t)) & guard_count_bit;
        fingerprint &= ~guard_count_bit;
        fingerprint |= fp_low;
        uint32_t bucket_index = guard_index;
        uint32_t slot_index = bucket_index * bucket_size;

        // uint32_t bucket_index = hash_loc.run((char*) &ele, sizeof(uint32_t)) % bucket_count;
        // uint32_t slot_index = bucket_index * bucket_size;

        __builtin_prefetch(buckets_slots + slot_index);
        for (int i = 0; i < bucket_slot; i++)
            if (timestamp_slots[slot_index + i] < outdatedTime) {
                timestamp_slots[slot_index + i] = curTime;
                buckets_slots[slot_index + i] = EMPTY_SLOT;
            }

        // if (fingerprint < buckets_slots[slot_index]) {
        uint32_t max_slot = 0, max_val = 0;

        for (int i = 0; i < bucket_slot; ++i) {
            if (buckets_slots[slot_index + i] == fingerprint) {
                timestamp_slots[slot_index + i] = curTime;
                return;
            }
        }
        for (int i = 0; i < bucket_slot; ++i) {
            if (buckets_slots[slot_index + i] >= guard_max) {
                buckets_slots[slot_index + i] = fingerprint;
                timestamp_slots[slot_index + i] = curTime;
                return;
            } else if (buckets_slots[slot_index + i] > max_val) {
                max_val = buckets_slots[slot_index + i];
                max_slot = slot_index + i;
            }
        }

        if (fingerprint < max_val) {
            buckets_slots[max_slot] = fingerprint;
            timestamp_slots[max_slot] = curTime;
            guard_max = std::min(guard_max, max_val);
        } else {
            guard_max = std::min(guard_max, fingerprint);
        }
        // }
    }
};

#endif //CDM_H