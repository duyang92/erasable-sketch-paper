#include <iostream>
#include <unordered_map>
#include "./Utils/load_data.h"
#include "./Algs/cdm.h"
#include "./Algs/rfds_linear.h"
#include "./Algs/rfds_double.h"
#include "./Algs/rfds_cuckoo_increz.h"
#include "./Algs/rfds_cuckoo_rehash.h"
#define CAIDA_SIZE 38083457
#define CLICK_SIZE 443652

BOBHash32 hash_id;
unordered_map<int, int> inmap;
unordered_map<uint64_t, unordered_set<int>> timeEleMap;
unordered_map<int, uint64_t> eleNewTimeMap;
// 50 in caida, window = 100000
// 100000 in click stream, window = 50000
int TWS = 1000;
//int startExp = 8, endExp = 10;
int insnum = 1e6, window; // when is frequency window, its value should be set more than 1<<16
const int freq_window = 100000;
//double ans[MAXNUM + window + 10];

void testThroughput(int sketchNo, int mem, int dat, int _window) {
    Sketch * skt;
    switch (sketchNo) {
        case 0: {
            skt = new CDM();
            break;
        }
        case 1: {
            skt = new RFDS_LINEAR();
            break;
        }
        case 2: {
            skt = new RFDS_DOUBLE();
            break;
        }
        case 3: {
            skt = new RFDS_CUCKOO_INCREZ();
            break;
        }
        case 4: {
            skt = new RFDS_CUCKOO_REHASH();
            break;
        }
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
        skt->init(1.0, 1 << mem, 8, 0.8, rand() % MAX_PRIME32, rand() % MAX_PRIME32, startTime, window);
        while (_item < flow_number) {
            skt->insert(flow[_item], timestamps[_item ++]);
        }
    }
    clock_t current = clock();
    double throughput_mpps;
    if (dat == 0)
        throughput_mpps = repmax * (flow_number / 1000000.0) / (((double)current - start) / CLOCKS_PER_SEC);
    else
        throughput_mpps = repmax * (flow_number / 1000000.0) / (((double)current - start) / CLOCKS_PER_SEC);
    cout << "Update Throughput: " << throughput_mpps << " Mpps" << endl;
    delete skt;
}

void testCDM(int startExp, int endExp, int _window = -1, int repmax = 1) {
    fstream out;
    out.open("./Dist/Click/click_erasable.txt", ios::out);
    if (_window != -1)
        window = _window;
    // Control the test memory from 1KB to 32KB, step = *2
    for (int mem = (1 << startExp); mem <= (1 << endExp); mem <<= 1) {
        double sum = 0;
        int cnt = 0;
        double std_value = 0;
        for (int rep = 1; rep <= repmax; rep++) {
            srand((unsigned int) time(NULL));
            inmap.clear();
            timeEleMap.clear();
            eleNewTimeMap.clear();
            double ans = 0;
            int _item = 1;
            uint64_t start_time = timestamps[_item];
            uint64_t pre_time = start_time;
            uint64_t cur_time = start_time;
            CDM cdm;
            cdm.init(1.0, mem, 8, 0.8, rand() % MAX_PRIME32, rand() % MAX_PRIME32, start_time, window);
            // real cardinality in current sliding window

            uint32_t clean_window_item_start = _item;
            uint64_t clean_window_start = timestamps[clean_window_item_start];
            while (cur_time <= start_time + (TWS) * window && _item < MAXNUM) {
                cdm.insert(flow[_item], cur_time);
                // clean the outdated items in previous sliding window
                while (cur_time >= start_time + window && clean_window_start < cur_time - window) {
                    std::unordered_set<int> clean_eles = timeEleMap[clean_window_start];
                    if (clean_eles.empty()) {
                        clean_window_start = timestamps[clean_window_item_start ++];
                        continue;
                    }
                    for (auto iter = clean_eles.begin(); iter != clean_eles.end(); iter++) {
                        /*inmap[flow[*iter]] --;
                        if (inmap[flow[*iter]] == 0) {
                            ans --;
                        }*/
                        if (cur_time - eleNewTimeMap[flow[*iter]] > window)
                            if (inmap[flow[*iter]] == 1) {
                                inmap[flow[*iter]] = 0;
                                ans --;
                            }
                    }
                    timeEleMap[clean_window_start].clear();
                    clean_window_start = timestamps[clean_window_item_start ++];
                }
                eleNewTimeMap[flow[_item]] = cur_time;
                timeEleMap[cur_time].insert(_item);
                if (inmap[flow[_item]] == 0) {
                    inmap[flow[_item]] = 1;
                    ans ++;
                }
                // show results (cur_time - start_time) % (window / 2) == 0 && cur_time != start_time
                if (cur_time - pre_time >= window && cur_time && cur_time > start_time) {
                    double ss = 0;
                    if (ans <= 5) {
                        cur_time = timestamps[++ _item];
                        continue;
                    }
                    ss = fabs((double) cdm.query(cur_time) / ans - 1.0);
//                     printf("query time %lld\t%d\t%.6lf\t%.6lf\n", cur_time, cdm.query(cur_time), ans, ss);
                    out << cur_time << " " << ans << " " << cdm.query(cur_time) << "\n";
                    sum += ss;
                    std_value += std::pow(ss, 2);
                    cnt++;
                    pre_time = pre_time + (cur_time - pre_time) / 1000;
                }
                cur_time = timestamps[++ _item];
            }
        }
        printf("memory:%d Byte\t ave error:%.6lf\t ave std:%.6lf\n", mem / 8, sum / cnt, std::sqrt(std_value / cnt));
        out.close();
    }
}

int main() {
    int start = 15;
    int end = 15;
    int datNo = 1;
    load_data2_clickstream();
    testCDM(start, end, 7200000, 50);
    return 0;
}

