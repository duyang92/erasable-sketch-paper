#ifndef SKETCH_H
#define SKETCH_H
#define TCP_SIZE 8
#define TCP_CHAR_SIZE 64
#include <cstdint>

class Sketch {
public:
    virtual void init(double _1, double _2, int _3, double _4, uint32_t _5, uint32_t _6, uint64_t _7, uint32_t _8) = 0;
    virtual uint32_t query(uint64_t _1) = 0;
    virtual void manualDelete(uint64_t _1, uint64_t _2) = 0;
    virtual void insert(uint64_t _1, uint64_t _2, uint32_t _3) = 0;
};
#endif //SKETCH_H
