#include "./murmurhash.h"
#include "./DLL.h"

void update(uint32_t key_i, uint32_t opt_i) {
    uint32_t max_col = 0, max_val = 0;
    uint32_t hashValue, hashIndex, FP;
    MurmurHash3_x86_32(&key_i, SEED1, &hashValue);
    hashIndex = hashValue % M;
    MurmurHash3_x86_32(&key_i, SEED2, &FP);
    switch (opt_i) {
        case 0 : {
            for (int col = 0; col < D; ++col) {
                if (B[hashIndex][col] == FP) {
                    return;
                } else if (B[hashIndex][col] > max_val) {
                    max_val = B[hashIndex][col];
                    max_col = col;
                }
            }
            if (FP < G[hashIndex]) {
                if (FP < B[hashIndex][max_col]) {
                    if (B[hashIndex][max_col] != INT32_MAX)
                        G[hashIndex] = B[hashIndex][max_col];
                    B[hashIndex][max_col] = FP;
                } else
                    G[hashIndex] = FP;
            }
            break;
        }
        case 1 : {
            for (int col = 0; col < D; col ++) {
                if (B[hashIndex][col] == FP) {
                    B[hashIndex][col] = INT32_MAX;
                    return;
                }
            }
            break;
        }
    }
}