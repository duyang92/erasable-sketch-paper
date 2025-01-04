#ifndef DLL_H
#define DLL_H
#include "stdint.h"
#define M 32
#define D 5
#define SEED1 21324
#define SEED2 51231
static uint32_t B[M][D] = {0};
static uint32_t G[M] = {0};
void update(uint32_t, uint32_t);
#endif //DLL_H
