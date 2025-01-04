#ifndef __RFDS_CUCKOO_INCREZ_H_
#define __RFDS_CUCKOO_INCREZ_H_

#include "../Utils/BOBHash32.h"
#include <cmath>
#include <cstdint>
#include "./Sketch.h"

class cuckoo_hash_table_increz {
   public:
    uint32_t* table1;
    uint32_t* table2;
    uint32_t EMPTY_BUCKET;
    uint32_t index_seed1, index_seed2;
    int bucket_count, current_size, max_size;
    int max_rehash_attempts;
    double load_factor;
    uint32_t fail_key;
    uint64_t fail_time;
    uint32_t area;

    uint64_t* timestamp_slots1;
    uint64_t* timestamp_slots2;

    uint32_t* location_slots1;
    uint32_t* location_slots2;

    BOBHash32 hash_loc1;
    BOBHash32 hash_loc2;

    cuckoo_hash_table_increz(int size, double load_factor, int INDEX_SEED, uint64_t curTime, uint32_t _area) {
        this->bucket_count = size / 2;
        this->max_size = size * load_factor;
        this->load_factor = load_factor;
        this->current_size = 0;
        this->EMPTY_BUCKET = UINT32_MAX;
        this->table1 = new uint32_t[bucket_count];
        this->timestamp_slots1 = new uint64_t[bucket_count];
        this->table2 = new uint32_t[bucket_count + 1];
        this->timestamp_slots2 = new uint64_t[bucket_count];
        this->area = _area;

        for (int i = 0; i < bucket_count; ++i) {
            table1[i] = EMPTY_BUCKET;
            timestamp_slots1[i] = curTime;
            table2[i] = EMPTY_BUCKET;
            timestamp_slots2[i] = curTime;
        }
        // z in RFDS
        this->table2[bucket_count] = 0;

        this->hash_loc1.initialize(INDEX_SEED);
        this->hash_loc2.initialize(rand()%MAX_PRIME32);
        this->max_rehash_attempts = 700;
    }

    cuckoo_hash_table_increz() {
    }

    ~cuckoo_hash_table_increz() {
        if (table1) delete[] table1;
        if (timestamp_slots1) delete[] timestamp_slots1;
        if (table2) delete[] table2;
        if (timestamp_slots2) delete[] timestamp_slots2;
    }

    void _clear(uint64_t& curTime, uint64_t& outdatedTime) {
        for (int i = 0; i < bucket_count; ++i) {
            if (timestamp_slots1[i] < outdatedTime) {
                timestamp_slots1[i] = curTime;
                if (table1[i] != EMPTY_BUCKET) {
                    table1[i] = EMPTY_BUCKET;
                    current_size--;
                }
            }
            if (timestamp_slots2[i] < outdatedTime) {
                timestamp_slots2[i] = curTime;
                if (table2[i] != EMPTY_BUCKET) {
                    table2[i] = EMPTY_BUCKET;
                    current_size--;
                }
            }
        }
    }

    int insert(uint32_t& key, uint64_t& curTime, uint64_t& outdatedTime, uint32_t areaNo) {
        if (current_size == max_size) {
            _clear(curTime, outdatedTime);
            if (current_size == max_size) {
                return 1;
            }
        }

        bool su = _insert(key, curTime, outdatedTime, areaNo);

        return su ? 0 : 2;
    }

    bool _insert(uint32_t& key, uint64_t& curTime, uint64_t& outdatedTime, uint32_t areaNo) {
        int attempts = 0;
        uint32_t temp_key = key;
        uint64_t temp_time = curTime;

        int index1 = hash_loc1.run((char*) &temp_key, sizeof(uint32_t)) % bucket_count;
        int index2 = hash_loc2.run((char*) &temp_key, sizeof(uint32_t)) % bucket_count;
        if (table1[index1] == temp_key) {
            if (areaNo != area) {
                table1[index1] = EMPTY_BUCKET;
                current_size --;
            }
            timestamp_slots1[index1] = temp_time;
            return true;
        }
        if (table2[index2] == temp_key) {
            if (areaNo != area) {
                table2[index2] = EMPTY_BUCKET;
                current_size --;
            }
            timestamp_slots2[index2] = temp_time;
            return true;
        }

        if (areaNo != area)
            return true;

        if (table1[index1] == EMPTY_BUCKET) {
            table1[index1] = temp_key;
            timestamp_slots1[index1] = temp_time;
            current_size++;
            return true;
        }
        if (table2[index2] == EMPTY_BUCKET) {
            table2[index2] = temp_key;
            timestamp_slots2[index2] = temp_time;
            current_size++;
            return true;
        }

        int next_table_index = rand() % 2;

        while (attempts < max_rehash_attempts) {
            uint32_t next_kickout_key;
            uint64_t next_kickout_time;

            if (next_table_index == 0) {
                next_kickout_key = table1[index1];
                next_kickout_time = timestamp_slots1[index1];
                table1[index1] = temp_key;
                timestamp_slots1[index1] = temp_time;
            } else {
                next_kickout_key = table2[index2];
                next_kickout_time = timestamp_slots2[index2];
                table2[index2] = temp_key;
                timestamp_slots2[index2] = temp_time;
            }

            temp_key = next_kickout_key;
            temp_time = next_kickout_time;

            if (next_table_index == 0) {
                index2 = hash_loc2.run((char*) &temp_key, sizeof(uint32_t)) % bucket_count;
                if (table2[index2] == EMPTY_BUCKET) {
                    table2[index2] = temp_key;
                    timestamp_slots2[index2] = temp_time;
                    current_size++;
                    return true;
                }
                next_table_index = 1;
            } else {
                index1 = hash_loc1.run((char*) &temp_key, sizeof(uint32_t)) % bucket_count;
                if (table1[index1] == EMPTY_BUCKET) {
                    table1[index1] = temp_key;
                    timestamp_slots1[index1] = temp_time;
                    current_size++;
                    return true;
                }
                next_table_index = 0;
            }
            attempts++;
        }
        fail_key = temp_key;
        fail_time = temp_time;
        return false;
    }

    bool erase(uint32_t& key, uint64_t curTime) {
        int index1 = hash_loc1.run((char*) &key, sizeof(uint32_t)) % bucket_count;
        if (table1[index1] == key) {
            table1[index1] = EMPTY_BUCKET;
            timestamp_slots1[index1] = curTime;
            current_size--;
            return true;
        }

        int index2 = hash_loc2.run((char*) &key, sizeof(uint32_t)) % bucket_count;
        if (table2[index2] == key) {
            table2[index2] = EMPTY_BUCKET;
            timestamp_slots2[index2] = curTime;
            current_size--;
            return true;
        }

        return false;
    }

    int size() {
        return current_size;
    }
};

class RFDS_CUCKOO_INCREZ : public Sketch {
#define C 240.0
   public:
    double epsilon;
    double alpha;
    double memory;
    int thresh;
    uint32_t FP_SEED;
    uint32_t INDEX_SEED;
    cuckoo_hash_table_increz* B;

    uint32_t window;
    uint32_t area;

    BOBHash32 hash_fp;

   public:
    RFDS_CUCKOO_INCREZ() {
    }

    void init(double _alpha, double _memory, int _, double load_factor, uint32_t _FP_SEED, uint32_t _INDEX_SEED, uint64_t curTime, uint32_t window, uint32_t _area) {
        alpha = _alpha;
        memory = _memory;
        double max_elements = round(memory / 64);
        thresh = max_elements * load_factor;
        epsilon = sqrt(C / ((1 - alpha) * thresh));

        FP_SEED = _FP_SEED;
        INDEX_SEED = _INDEX_SEED;

        this->window = window;
        hash_fp.initialize(FP_SEED);

        this->area = _area;

        B = new cuckoo_hash_table_increz(max_elements, load_factor, INDEX_SEED, curTime, _area);
    }

    ~RFDS_CUCKOO_INCREZ() {
        if (B) delete B;
    }

    void manualDelete(uint32_t ele, uint64_t curTime) {
        uint32_t fingerprint = hash_fp.run((char*) &ele, sizeof(uint32_t));
        fingerprint -= (fingerprint == UINT32_MAX);
        B->erase(fingerprint, curTime);
    }

    void insert(uint32_t ele, uint64_t curTime, uint32_t areaNo) {

        uint64_t outdatedTime = curTime - this->window;

        uint32_t fingerprint = hash_fp.run((char*) &ele, sizeof(uint32_t));
        fingerprint -= (fingerprint == UINT32_MAX);

        uint32_t lz = 0;

        int flag = 0;

        lz = __builtin_clz(hash_fp.run((char*) &fingerprint, sizeof(uint32_t)));
        if (lz >= B->table2[B->bucket_count]) {
            flag = B->insert(fingerprint, curTime, outdatedTime, areaNo);
        }

        if (flag == 1) {
            ++B->table2[B->bucket_count];
            for (int i = 0; i < B->bucket_count; ++i) {
                uint32_t cur1 = B->table1[i];
                if (cur1 != B->EMPTY_BUCKET && __builtin_clz(hash_fp.run((char*) &cur1, sizeof(uint32_t))) < B->table2[B->bucket_count]) {
                    B->table1[i] = B->EMPTY_BUCKET;
                    B->timestamp_slots1[i] = curTime;
                    B->current_size--;
                }
                uint32_t cur2 = B->table2[i];
                if (cur2 != B->EMPTY_BUCKET && __builtin_clz(hash_fp.run((char*) &cur2, sizeof(uint32_t))) < B->table2[B->bucket_count]) {
                    B->table2[i] = B->EMPTY_BUCKET;
                    B->timestamp_slots2[i] = curTime;
                    B->current_size--;
                }
            }
            if (flag == 2) {
                fingerprint = B->fail_key;
                curTime = B->fail_time;
                lz = __builtin_clz(hash_fp.run((char*) &fingerprint, sizeof(uint32_t)));
            }
            if (lz >= B->table2[B->bucket_count]) {
                B->insert(fingerprint, curTime, outdatedTime, areaNo);
                while (flag == 2) {
                    cuckoo_hash_table_increz* tmp = new cuckoo_hash_table_increz(B->bucket_count * 2, B->load_factor, rand() % MAX_PRIME32, curTime, areaNo);
                    tmp->table2[B->bucket_count] = B->table2[B->bucket_count];
                    for (int i = 0; i < B->bucket_count; ++i) {
                        if (B->table1[i] != B->EMPTY_BUCKET) {
                            flag = tmp->insert(B->table1[i], B->timestamp_slots1[i], outdatedTime, areaNo);
                            if (flag == 2) {
                                break;
                            }
                        }
                        if (B->table2[i] != B->EMPTY_BUCKET) {
                            flag = tmp->insert(B->table2[i], B->timestamp_slots2[i], outdatedTime, areaNo);
                            if (flag == 2) {
                                break;
                            }
                        }
                    }
                    if (flag == 0) flag = tmp->insert(fingerprint, curTime, outdatedTime, areaNo);
                    if (flag == 0) {
                        delete B;
                        B = tmp;
                    }else{
                        delete tmp;
                    }
                }
            }
        }
    }

    uint32_t query(uint64_t curTime) {
        uint64_t outdatedTime = curTime - this->window;
        // std::cout << "B->size = " << B->size() << " B->z = " << B->table2[B->bucket_count] << std::endl;
        B->_clear(curTime, outdatedTime);
        // std::cout << "B->size = " << B->size() << " B->z = " << B->table2[B->bucket_count] << std::endl;
        return static_cast<uint32_t>(B->size() * pow(2, B->table2[B->bucket_count]));
    }
};

#endif