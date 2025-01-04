#include <iostream>
#include <unordered_map>
#include "./Utils/load_data.h"
#include "./AlgsLoc/cdm.h"
#include "./AlgsLoc/rfds_linear.h"
#include "./AlgsLoc/rfds_double.h"
#include "./AlgsLoc/rfds_cuckoo_increz.h"

#define MAX_REGION 17
#define TRAFFIC_SIZE 40360427
BOBHash32 hash_id;

int TWS = 200000;
int startExp = 15, endExp = 15;
const int insnum = 1e6, window = 900; // when is frequency window, its value should be set more than 1<<16
double ans[MAXNUM + window + 10];
int repmax = 10;
/*
 * 0: CDM
 * 1: RFDS linear
 * 2: RFDS double
 * 3: RFDS cuckoo increz
 * 4: RFDS cuckoo rehash
 */
void testThroughput(int sketchNo, int mem) {
    Sketch * skt[MAX_REGION];
    switch (sketchNo) {
        case 0: {
            for (int i = 0; i < MAX_REGION; ++i)
                skt[i] = new CDM();
            break;
        }
        case 1: {
            for (int i = 0; i < MAX_REGION; ++i)
                skt[i] = new RFDS_LINEAR();
            break;
        }
        case 2: {
            for (int i = 0; i < MAX_REGION; ++i)
                skt[i] = new RFDS_DOUBLE();
            break;
        }
        case 3: {
            for (int i = 0; i < MAX_REGION; ++i)
                skt[i] = new RFDS_CUCKOO_INCREZ();
            break;
        }
        /*case 4: {
            for (int i = 0; i < MAX_REGION; ++i)
                skt[i] = new RFDS_CUCKOO_REHASH();
            break;
        }*/
        default: {
            std::cerr << "Invalid sketch type" << std::endl;
            return;
        }
    }
    int repmax = 1;
    clock_t start = clock();
    for (int rep = 1; rep <= repmax; ++rep) {
        srand((unsigned int) time(NULL));
        uint32_t _item = 1;
        uint64_t startTime = timestamps[_item];
        uint64_t curTime = startTime;
        for (int i = 0; i <= 1; ++i) {
            skt[i]->init(1.0, mem, 8, 0.8, rand() % MAX_PRIME32, rand() % MAX_PRIME32, startTime, window, i);
        }
        while (_item < MAXNUM) {
            //for (int i = 1; i <= 1; ++i) {
                skt[1]->insert(flow[_item], timestamps[_item], locations[_item]);
            //}
            _item ++;
        }
    }
    clock_t current = clock();
    double throughput_mpps = repmax * (flow_number / 1000000.0) / (((double)current - start) / CLOCKS_PER_SEC);
    cout << "Update Throughput: " << throughput_mpps << " Mpps" << endl;
    for (int i = 0; i <= 1; ++i) {
        delete skt[i];
    }
}

void testCDM() {
    fstream out;
    out.open("./Dist/Traffic/traffic_erasable.txt", ios::out);
    // Control the test memory from 1KB to 32KB, step = *2
    for (int mem = (1 << startExp); mem <= (1 << endExp); mem <<= 1) {
        double sums[MAX_REGION];
        double stds[MAX_REGION];
        int cnts[MAX_REGION];
        double ansMap[MAX_REGION];
        for (int i = 1; i < MAX_REGION; i++) {
            sums[i] = 0;
            stds[i] = 0;
            cnts[i] = 0;
        }
        for (int rep = 1; rep <= repmax; rep++) {
            uint32_t _item = 1;
            CDM cdmMap[MAX_REGION];
            srand((unsigned int) time(NULL));
            uint64_t startTime = timestamps[_item];
            uint64_t preTime = startTime;
            uint64_t curTime = startTime;
            std::unordered_map<int, bool> inmap[MAX_REGION];
            std::unordered_map<uint64_t, std::unordered_set<int>> timeEleMap[MAX_REGION];
            std::unordered_map<uint32_t, uint64_t> eleNewTimeMap[MAX_REGION];
            std::unordered_map<uint32_t, uint32_t> eleLocationMap;
            for (int i = 0; i < MAX_REGION; i++) {
                ansMap[i] = 0;
                cdmMap[i].init(1.0, mem, 8, 0.8, rand() % MAX_PRIME32, rand() % MAX_PRIME32, startTime, window, i);
            }
            // real cardinality in multiple sliding windows
            uint32_t cleanWindowItemStart = _item;
            uint64_t cleanWindowTimeStart = timestamps[cleanWindowItemStart];
            while (curTime <= startTime + (TWS) * window && _item < MAXNUM) {
                uint32_t loc = locations[_item];
                uint32_t keyId = flow[_item];
                if (eleLocationMap[keyId] == 0) {
                    eleLocationMap[keyId] = loc;
                } else {
                    if (eleLocationMap[keyId] != loc) { // the location has been changed
                        if (curTime - eleNewTimeMap[eleLocationMap[keyId]][keyId] <= window) {
                            // manual delete this item from area eleLocationMap[keyId]
                            inmap[eleLocationMap[keyId]][keyId] = false;
                            ansMap[eleLocationMap[keyId]] --;
                            eleNewTimeMap[eleLocationMap[keyId]].erase(keyId);
                        }
                        eleLocationMap[keyId] = loc;
                    }
                }
                for (int i = 1; i < MAX_REGION; ++i) {
                    cdmMap[i].insert(keyId, curTime, loc);
                }
                // clean the outdated items in previous sliding window
                while (curTime > startTime + window && cleanWindowTimeStart < curTime - window) {
                    // clean the outdated items in each sliding window
                    for (int i = 1; i < MAX_REGION; ++i) {
                        std::unordered_set<int> clean_eles = timeEleMap[i][cleanWindowTimeStart];
                        if (clean_eles.empty()) {
                            continue;
                        }
                        for (auto iter = clean_eles.begin(); iter != clean_eles.end(); iter++) {
                            if (curTime - eleNewTimeMap[i][flow[*iter]] > window)
                                if (inmap[i][flow[*iter]] == true) {
                                    inmap[i][flow[*iter]] = false;
                                    ansMap[i] --;
                                }
                        }
                        timeEleMap[i][cleanWindowTimeStart].clear();
                    }
                    cleanWindowTimeStart = timestamps[cleanWindowItemStart ++];
                }
                //std::cout << "loc: " << loc << ", curTime: " << curTime << ", keyId: " << keyId << std::endl;
                timeEleMap[loc][curTime].insert(_item);
                eleNewTimeMap[loc][keyId] = curTime;
                if (inmap[loc][keyId] == false) {
                    inmap[loc][keyId] = true;
                    ansMap[loc] ++;
                }
                // show results (curTime - startTime) > test_factor * window && curTime != startTime && curTime != preTime
                if (curTime - preTime >= window && curTime > preTime) {
                    for (int i = 1; i < MAX_REGION; ++i) {
                        double ss = 0;
                        if (ansMap[i] == 0.0)
                            continue;
                        ss = fabs((double) cdmMap[i].query(curTime) / ansMap[i] - 1.0);
                        sums[i] += ss;
                        stds[i] += std::pow(ss, 2);
                        cnts[i] ++;
                        if (i == 1) {
                            out << curTime << " " << ansMap[i] << " " << cdmMap[i].query(curTime) << "\n";
                            //printf("query time %lld Region %d %.6lf", curTime, i, ss);
                        } else {
                            //printf("Region %d %d\t%.6lf\t%.6lf\t", i, cdmMap[i].query(curTime), ansMap[i], ss);
                            //printf(" Region %d %.6lf", i, ss);
                        }
                        if (i == MAX_REGION - 1) {
                            //printf("\n");
                        }
                    }
                    preTime = preTime + (curTime - preTime) / 5000;
                }
                curTime = timestamps[++ _item];
            }
        }
        printf("\n=================================================================================\n");
        double aave = 0.0;
        double astd = 0.0;
        for (int i = 1; i < MAX_REGION; ++i) {
            printf("[Region %d] memory:%d Byte\t ave error:%.6lf\t ave std:%.6lf\n", i, mem / 8, sums[i] / cnts[i], std::sqrt(stds[i] / cnts[i]));
            aave += sums[i] / cnts[i];
            astd += std::sqrt(stds[i] / cnts[i]);
        }
        aave = aave / (MAX_REGION - 1);
        astd = astd / (MAX_REGION - 1);
        out.close();
        printf("Memory:%d Byte\t ave error:%.6lf\t ave std:%.6lf\n", mem / 8, aave, astd);
    }
}

int main() {
    /*load_data3_traffic();
    std::cout << "finished load" << std::endl;
    for (int mem = (1 << 9); mem <= (1 << 17); mem <<= 1) {   
       std::cout << "[CDM] ";
        testThroughput(0, mem);
        std::cout << "[linear]";
        testThroughput(1, mem);
        std::cout << "[double]";
        testThroughput(2, mem);
        std::cout << "[cuckoo] ";
        testThroughput(3, mem);
    }*/
    
    load_data3_traffic();
    std::cout << "[CDM] ";
    testCDM();
    /*std::cout << "[RFDS linear] ";
    testRFDSLinear();
    std::cout << "[RFDS double] ";
    testRFDSDouble();
    std::cout << "[RFDS cuckoo]";
    testRFDSCuckoo();*/
    return 0;
}

