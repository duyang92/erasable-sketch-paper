#ifndef __RFDS_LINEAR_H_
#define __RFDS_LINEAR_H_

#include "../Utils/BOBHash32.h"
#include <cmath>
#include <cstdint>
#include <iostream>

class linear_hash_table {
   public:
    uint32_t* table;
    uint32_t EMPTY_BUCKET;
    uint32_t index_seed;
    int bucket_count, current_size, max_size;

    uint64_t* timestamp_slots;

    BOBHash32 hash_loc;

    linear_hash_table(int size, double load_factor, int _index_seed, uint64_t curTime) {
        this->bucket_count = size;
        this->max_size = size * load_factor;
        this->current_size = 0;
        this->EMPTY_BUCKET = UINT32_MAX;
        this->table = new uint32_t[bucket_count + 1];
        this->timestamp_slots = new uint64_t[bucket_count];
        for (int i = 0; i < bucket_count; ++i) {
            table[i] = EMPTY_BUCKET;
            timestamp_slots[i] = curTime;
        }
        // z in RFDS
        this->table[bucket_count] = 0;
        
        this->hash_loc.initialize(_index_seed);
    }

    linear_hash_table() {
    }

    ~linear_hash_table() {
        if (table) delete[] table;
        if (timestamp_slots) delete[] timestamp_slots;
    }

    void _clear(uint64_t& curTime, uint64_t& outdatedTime) {
        for (int i = 0; i < bucket_count; ++i) {
            if (timestamp_slots[i] < outdatedTime) {
                timestamp_slots[i] = curTime;
                if (table[i] != EMPTY_BUCKET && table[i] != 0) {
                    table[i] = 0;
                    current_size--;
                }
            }
        }
    }

    bool insert(uint32_t& key, uint64_t curTime, uint64_t& outdatedTime) {
        if (current_size == max_size) {
            _clear(curTime, outdatedTime);
            if (current_size == max_size) {
                return false;
            }
        }

        int index = hash_loc.run((char*) &key, sizeof(uint32_t)) % bucket_count;
        int start = index, available = -1;
        do {
            if (table[index] == key) {
                timestamp_slots[index] = curTime;
                return true;
            }
            if (table[index] == 0) {
                available = index;
            }
            index = (index + 1) % bucket_count;
        } while (index != start && table[index] != EMPTY_BUCKET);

        if (available != -1) {
            index = available;
        }

        table[index] = key;
        timestamp_slots[index] = curTime;
        current_size++;
        return true;
    }

    int _find(uint32_t& key) {
        int index = hash_loc.run((char*) &key, sizeof(uint32_t)) % bucket_count;
        int start = index;
        do {
            if (table[index] == key) {
                return index;
            }
            index = (index + 1) % bucket_count;
        } while (index != start && table[index] != EMPTY_BUCKET);
        return -1;
    }

    bool erase(uint32_t& key, uint64_t curTime) {
        int index = _find(key);
        if (index == -1) {
            return false;
        }
        table[index] = 0;
        timestamp_slots[index] = curTime;
        current_size--;
        return true;
    }

    int size() {
        return current_size;
    }
};

class RFDS_LINEAR : public Sketch {
#define C 240.0
   public:
    double epsilon;
    double alpha;
    double memory;
    int thresh;
    uint32_t FP_SEED;
    uint32_t INDEX_SEED;
    linear_hash_table* B;

    uint32_t window;

    BOBHash32 hash_fp;

   public:
    RFDS_LINEAR() {
    }

    void init(double _alpha, double _memory, int _, double load_factor, uint32_t _FP_SEED, uint32_t _INDEX_SEED, uint64_t curTime, uint32_t window) {
        alpha = _alpha;
        memory = 1.0 * _memory;
        double max_elements = round(memory / 64);
        thresh = max_elements * load_factor;
        epsilon = sqrt(C / ((1 - alpha) * thresh));

        FP_SEED = _FP_SEED;
        INDEX_SEED = _INDEX_SEED;

        this->window = window;
        hash_fp.initialize(FP_SEED);

        B = new linear_hash_table(max_elements, load_factor, INDEX_SEED, curTime);
    }

    ~RFDS_LINEAR() {
        if (B) delete B;
    }

    void manualDelete(uint64_t ele, uint64_t curTime) {
        uint32_t fingerprint = hash_fp.run((char*) &ele, sizeof(uint64_t));
        fingerprint -= (fingerprint == UINT32_MAX);
        B->erase(fingerprint, curTime);
    }

    void insert(uint64_t ele, uint64_t curTime, uint32_t _flag) {
        if (_flag == 0) {
            this->manualDelete(ele, curTime);
            return;
        }
        uint64_t outdatedTime = curTime - this->window;

        uint32_t fingerprint = hash_fp.run((char*) &ele, sizeof(uint64_t));
        fingerprint -= (fingerprint == UINT32_MAX);

        uint32_t lz = 0;

        bool flag = true;

        lz = __builtin_clz(hash_fp.run((char*) &fingerprint, sizeof(uint32_t)));
        if (lz >= B->table[B->bucket_count]) {
            flag = B->insert(fingerprint, curTime, outdatedTime);
        }

        if (!flag) {  // 调整
            ++B->table[B->bucket_count];
            for (int i = 0; i < B->bucket_count; ++i) {
                uint32_t cur = B->table[i];
                if (cur != B->EMPTY_BUCKET && cur != 0 && __builtin_clz(hash_fp.run((char*) &cur, sizeof(uint32_t))) < B->table[B->bucket_count]) {
                    B->table[i] = 0;
                    B->current_size--;
                }
            }
            if (lz >= B->table[B->bucket_count]) {
                B->insert(fingerprint, curTime, outdatedTime);
            }
        }
    }

    uint32_t query(uint64_t curTime) {
        uint64_t outdatedTime = curTime - this->window;
        // std::cout << "B->size = " << B->size() << " B->z = " << B->table[B->bucket_count] << std::endl;
        B->_clear(curTime, outdatedTime);
        // std::cout << "B->size = " << B->size() << " B->z = " << B->table[B->bucket_count] << std::endl;
        return static_cast<uint32_t>(B->size() * pow(2, B->table[B->bucket_count]));
    }
};

#endif