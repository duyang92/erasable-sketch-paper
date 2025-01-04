#include <iostream>
#include <unordered_map>
#include "./Utils/load_data.h"
#include "./AlgsTCP/cdm.h"
#include "./AlgsTCP/rfds_linear.h"
#include "./AlgsTCP/rfds_double.h"
#include "./AlgsTCP/rfds_cuckoo_increz.h"
#define CAIDA_SIZE 38083457
#define CLICK_SIZE 443652

BOBHash32 hash_id;
// 50 in caida, window = 100000
// 100000 in click stream, window = 50000
int TWS = 100;
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
        skt->init(1.0, mem, 8, 0.8, rand() % MAX_PRIME32, rand() % MAX_PRIME32, startTime, window);
        while (_item < flow_number) {
            skt->insert(flow[_item], timestamps[_item], flags[_item ++]);
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

/*
void testCDM(int startExp, int endExp, int _window = -1, int repmax = 1) {
    if (_window != -1)
        window = _window;
    // Control the test memory from 1KB to 32KB, step = *2
    for (int mem = (1 << startExp); mem <= (1 << endExp); mem <<= 1) {
        double sum = 0;
        int cnt = 0;

        for (int rep = 1; rep <= repmax; rep++) {
            srand((unsigned int) time(NULL));
            unordered_map<uint64_t, int> inmap;
            unordered_map<uint64_t, unordered_set<int>> timeEleMap;
            unordered_map<uint64_t, uint64_t> eleNewTimeMap;
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
                cdm.insert(tcpFlows[_item], timestamps[_item], flags[_item]);
                /*if (eleNewTimeMap.find(tcpFlows[_item]) != eleNewTimeMap.end() && cur_time - eleNewTimeMap[tcpFlows[_item]] <= window && flags[_item] == 0) {
                    ans --;
                    eleNewTimeMap.erase(tcpFlows[_item]);
                    inmap[tcpFlows[_item]] = 0;
                }#1#
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
                        }#1#
                        if (cur_time - eleNewTimeMap[tcpFlows[*iter]] > window)
                            if (inmap[tcpFlows[*iter]] == 1) {
                                inmap[tcpFlows[*iter]] = 0;
                                ans --;
                            }
                    }
                    timeEleMap[clean_window_start].clear();
                    clean_window_start = timestamps[clean_window_item_start ++];
                }
                if (true) {
                    eleNewTimeMap[tcpFlows[_item]] = cur_time;
                    timeEleMap[cur_time].insert(_item);
                    if (inmap[tcpFlows[_item]] == 0) {
                        inmap[tcpFlows[_item]] = 1;
                        ans ++;
                    }
                }
                // show results (cur_time - start_time) % (window / 2) == 0 && cur_time != start_time  cur_time - pre_time >= window && cur_time && cur_time > start_time
                if (cur_time - pre_time >= window && cur_time && cur_time > start_time) {
                    double ss = 0;
                    if (ans == 0.0)
                        continue;
                    ss = fabs((double) cdm.query(cur_time) / ans - 1.0);
                    printf("query time %lld\t%d\t%.6lf\t%.6lf\n", cur_time, cdm.query(cur_time), ans, ss);
                    sum += ss;
                    cnt++;
                    pre_time = pre_time + (cur_time - pre_time) / 8;
                }
                cur_time = timestamps[++ _item];
            }
        }
        printf("memory:%d Byte\t ave error:%.6lf\n", mem / 8, sum / cnt);
    }
}
*/
void testCDM(int startExp, int endExp, int _window = -1, int repmax = 1) {
    fstream out;
    out.open("./Dist/TCP/tcp_erasable.txt", ios::out);
    if (_window != -1)
        window = _window;
    // Control the test memory from 1KB to 32KB, step = *2
    for (int mem = (1 << startExp); mem <= (1 << endExp); mem <<= 1) {
        double sum = 0;
        int cnt = 0;
        double std_value = 0;
        for (int rep = 1; rep <= repmax; rep++) {    
            srand((unsigned int) time(NULL));
            unordered_map<uint64_t, int> inmap;
            unordered_map<uint64_t, unordered_set<int>> timeEleMap;
            unordered_map<uint64_t, uint64_t> eleNewTimeMap;
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
                cdm.insert(flow[_item], cur_time, flags[_item]);
                if (eleNewTimeMap.find(flow[_item]) != eleNewTimeMap.end() && cur_time - eleNewTimeMap[flow[_item]] <= window && flags[_item] == 0) {
                    ans --;
                    eleNewTimeMap.erase(flow[_item]);
                    inmap[flow[_item]] = 0;
                }
                // clean the outdated items in previous sliding window
                while (cur_time >= start_time + window && clean_window_start < cur_time - window) {
                    std::unordered_set<int> clean_eles = timeEleMap[clean_window_start];
                    if (clean_eles.empty()) {
                        clean_window_start = timestamps[clean_window_item_start ++];
                        continue;
                    }
                    for (auto iter = clean_eles.begin(); iter != clean_eles.end(); iter++) {
                        if (cur_time - eleNewTimeMap[flow[*iter]] > window)
                            if (inmap[flow[*iter]] == 1) {
                                inmap[flow[*iter]] = 0;
                                ans --;
                            }
                    }
                    timeEleMap[clean_window_start].clear();
                    clean_window_start = timestamps[clean_window_item_start ++];
                }
                if (flags[_item] == 1) {
                    eleNewTimeMap[flow[_item]] = cur_time;
                    timeEleMap[cur_time].insert(_item);
                    if (inmap[flow[_item]] == 0) {
                        inmap[flow[_item]] = 1;
                        ans ++;
                    }
                }
                // show results (cur_time - start_time) % (window / 2) == 0 && cur_time != start_time
                if (cur_time - pre_time >= window && cur_time && cur_time > start_time) {
                    double ss = 0;
                    if (ans <= 5) {
                        cur_time = timestamps[++ _item];
                        continue;
                    }
                    ss = fabs((double) cdm.query(cur_time) / ans - 1.0);
                     //printf("query time %lld\t%d\t%.6lf\t%.6lf\n", cur_time, cdm.query(cur_time), ans, ss);
                    sum += ss;
                    std_value += std::pow(ss, 2);
                    cnt++;
                    out << cur_time << " " << ans << " " << cdm.query(cur_time) << "\n";
                    pre_time = pre_time + (cur_time - pre_time) / 1000;
                }
                cur_time = timestamps[++ _item];
            }
        }
        printf("memory:%d Byte\t ave error:%.6lf\t ave std:%.6lf\n", mem / 8, sum / cnt, std::sqrt(std_value / cnt));
        out.close();
    }
}

void testRFDSLinear(int startExp, int endExp, int _window = -1, int repmax = 1) {
    int random_seed1[repmax + 1];
    random_seed1[0] = 53535;
    random_seed1[1] = 13221;
    random_seed1[2] = 17123;
    random_seed1[3] = 66123;
    random_seed1[4] = 91321;
    random_seed1[5] = 88712;
    random_seed1[6] = 64411;
    random_seed1[7] = 21212;
    random_seed1[8] = 44121;
    random_seed1[9] = 66127;
    random_seed1[10] = 11127;
    
    fstream out;
    out.open("./Dist/TCP/tcp_linear.txt", ios::out);
    if (_window != -1)
        window = _window;
    // Control the test memory from 1KB to 32KB, step = *2
    for (int mem = (1 << startExp); mem <= (1 << endExp); mem <<= 1) {
        double sum = 0;
        int cnt = 0;
        double std_value = 0;
        for (int rep = 1; rep <= repmax; rep++) {
            srand((unsigned int) time(NULL));
            unordered_map<uint64_t, int> inmap;
            unordered_map<uint64_t, unordered_set<int>> timeEleMap;
            unordered_map<uint64_t, uint64_t> eleNewTimeMap;
            double ans = 0;
            int _item = 1;
            uint64_t start_time = timestamps[_item];
            uint64_t pre_time = start_time;
            uint64_t cur_time = start_time;
            RFDS_LINEAR rfds_linear;
            rfds_linear.init(1.0, mem, 8, 0.8, random_seed1[rep], rand() % MAX_PRIME32, start_time, window);
            // real cardinality in current sliding window
            uint32_t clean_window_item_start = _item;
            uint64_t clean_window_start = timestamps[clean_window_item_start];
            while (cur_time <= start_time + (TWS) * window && _item < MAXNUM) {
                rfds_linear.insert(flow[_item], timestamps[_item], flags[_item]);
                if (eleNewTimeMap.find(flow[_item]) != eleNewTimeMap.end() && cur_time - eleNewTimeMap[flow[_item]] <= window && flags[_item] == 0) {
                    ans --;
                    eleNewTimeMap.erase(flow[_item]);
                    inmap[flow[_item]] = 0;
                }
                // clean the outdated items in previous sliding window
                while (cur_time >= start_time + window && clean_window_start < cur_time - window) {
                    std::unordered_set<int> clean_eles = timeEleMap[clean_window_start];
                    if (clean_eles.empty()) {
                        clean_window_start = timestamps[clean_window_item_start ++];
                        continue;
                    }
                    for (auto iter = clean_eles.begin(); iter != clean_eles.end(); iter++) {
                        if (cur_time - eleNewTimeMap[flow[*iter]] > window)
                            if (inmap[flow[*iter]] == 1) {
                                inmap[flow[*iter]] = 0;
                                ans --;
                            }
                    }
                    timeEleMap[clean_window_start].clear();
                    clean_window_start = timestamps[clean_window_item_start ++];
                }
                if (flags[_item] == 1) {
                    eleNewTimeMap[flow[_item]] = cur_time;
                    timeEleMap[cur_time].insert(_item);
                    if (inmap[flow[_item]] == 0) {
                        inmap[flow[_item]] = 1;
                        ans ++;
                    }
                }
                // show results (cur_time - start_time) % (window / 2) == 0 && cur_time != start_time  cur_time - pre_time >= window && cur_time && cur_time > start_time
                if (cur_time - pre_time >= window && cur_time && cur_time > start_time) {
                    double ss = 0;
                    if (ans == 0.0)
                        continue;
                    ss = fabs((double) rfds_linear.query(cur_time) / ans - 1.0);
                    //printf("query time %lld\t%d\t%.6lf\t%.6lf\n", cur_time, rfds_linear.query(cur_time), ans, ss);
                    sum += ss;
                    std_value += std::pow(ss, 2);
                    cnt++;
                    out << cur_time << " " << ans << " " << rfds_linear.query(cur_time) << "\n";
                    pre_time = pre_time + (cur_time - pre_time) / 1000;
                }
                cur_time = timestamps[++ _item];
            }
        }
        printf("memory:%d Byte\t ave error:%.6lf\t ave std:%.6lf\n", mem / 8, sum / cnt, std::sqrt(std_value / cnt));
        out.close();
    }
}

void testRFDSDouble(int startExp, int endExp, int _window = -1, int repmax = 1) {
    int random_seed1[repmax + 1];
    random_seed1[0] = 53535;
    random_seed1[1] = 13221;
    random_seed1[2] = 17123;
    random_seed1[3] = 66123;
    random_seed1[4] = 91321;
    random_seed1[5] = 88712;
    random_seed1[6] = 64411;
    random_seed1[7] = 21212;
    random_seed1[8] = 44121;
    random_seed1[9] = 66127;
    random_seed1[10] = 11127;
    
    fstream out;
    out.open("./Dist/TCP/tcp_double.txt", ios::out);
    if (_window != -1)
        window = _window;
    // Control the test memory from 1KB to 32KB, step = *2
    for (int mem = (1 << startExp); mem <= (1 << endExp); mem <<= 1) {
        double sum = 0;
        int cnt = 0;
        double std_value = 0;
        for (int rep = 1; rep <= repmax; rep++) {
            srand((unsigned int) time(NULL));
            unordered_map<uint64_t, int> inmap;
            unordered_map<uint64_t, unordered_set<int>> timeEleMap;
            unordered_map<uint64_t, uint64_t> eleNewTimeMap;
            double ans = 0;
            int _item = 1;
            uint64_t start_time = timestamps[_item];
            uint64_t pre_time = start_time;
            uint64_t cur_time = start_time;
            RFDS_DOUBLE rfds_double;
            rfds_double.init(1.0, mem, 8, 0.8, random_seed1[rep], rand() % MAX_PRIME32, start_time, window);
            // real cardinality in current sliding window
            uint32_t clean_window_item_start = _item;
            uint64_t clean_window_start = timestamps[clean_window_item_start];
            while (cur_time <= start_time + (TWS) * window && _item < MAXNUM) {
                rfds_double.insert(flow[_item], timestamps[_item], flags[_item]);
                if (eleNewTimeMap.find(flow[_item]) != eleNewTimeMap.end() && cur_time - eleNewTimeMap[flow[_item]] <= window && flags[_item] == 0) {
                    ans --;
                    eleNewTimeMap.erase(flow[_item]);
                    inmap[flow[_item]] = 0;
                }
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
                if (flags[_item] == 1) {
                    eleNewTimeMap[flow[_item]] = cur_time;
                    timeEleMap[cur_time].insert(_item);
                    if (inmap[flow[_item]] == 0) {
                        inmap[flow[_item]] = 1;
                        ans ++;
                    }
                }
                // show results (cur_time - start_time) % (window / 2) == 0 && cur_time != start_time  cur_time - pre_time >= window && cur_time && cur_time > start_time
                if (cur_time - pre_time >= window && cur_time && cur_time > start_time) {
                    double ss = 0;
                    if (ans == 0.0)
                        continue;
                    ss = fabs((double) rfds_double.query(cur_time) / ans - 1.0);
                    //printf("query time %lld\t%d\t%.6lf\t%.6lf\n", cur_time, cdm.query(cur_time), ans, ss);
                    sum += ss;
                    std_value += std::pow(ss, 2);
                    cnt++;
                    out << cur_time << " " << ans << " " << rfds_double.query(cur_time) << "\n";
                    pre_time = pre_time + (cur_time - pre_time) / 1000;
                }
                cur_time = timestamps[++ _item];
            }
        }
        printf("memory:%d Byte\t ave error:%.6lf\t ave std:%.6lf\n", mem / 8, sum / cnt, std::sqrt(std_value / cnt));
        out.close();
    }
}

void testRFDSCuckoo(int startExp, int endExp, int _window = -1, int repmax = 1) {
    fstream out;
    out.open("./Dist/TCP/tcp_cuckoo.txt", ios::out);
    if (_window != -1)
        window = _window;
    // Control the test memory from 1KB to 32KB, step = *2
    for (int mem = (1 << startExp); mem <= (1 << endExp); mem <<= 1) {
        double sum = 0;
        int cnt = 0;
        double std_value = 0;
        for (int rep = 1; rep <= repmax; rep++) {
            srand((unsigned int) time(NULL));
            unordered_map<uint64_t, int> inmap;
            unordered_map<uint64_t, unordered_set<int>> timeEleMap;
            unordered_map<uint64_t, uint64_t> eleNewTimeMap;
            double ans = 0;
            int _item = 1;
            uint64_t start_time = timestamps[_item];
            uint64_t pre_time = start_time;
            uint64_t cur_time = start_time;
            RFDS_CUCKOO_INCREZ rfds_cuckoo_increz;
            rfds_cuckoo_increz.init(1.0, mem, 8, 0.8, rand() % MAX_PRIME32, rand() % MAX_PRIME32, start_time, window);
            // real cardinality in current sliding window
            uint32_t clean_window_item_start = _item;
            uint64_t clean_window_start = timestamps[clean_window_item_start];
            while (cur_time <= start_time + (TWS) * window && _item < MAXNUM) {
                rfds_cuckoo_increz.insert(flow[_item], timestamps[_item], flags[_item]);
                if (eleNewTimeMap.find(flow[_item]) != eleNewTimeMap.end() && cur_time - eleNewTimeMap[flow[_item]] <= window && flags[_item] == 0) {
                    ans --;
                    eleNewTimeMap.erase(flow[_item]);
                    inmap[flow[_item]] = 0;
                }
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
                if (flags[_item] == 1) {
                    eleNewTimeMap[flow[_item]] = cur_time;
                    timeEleMap[cur_time].insert(_item);
                    if (inmap[flow[_item]] == 0) {
                        inmap[flow[_item]] = 1;
                        ans ++;
                    }
                }
                // show results (cur_time - start_time) % (window / 2) == 0 && cur_time != start_time  cur_time - pre_time >= window && cur_time && cur_time > start_time
                if (cur_time - pre_time >= window && cur_time && cur_time > start_time) {
                    double ss = 0;
                    if (ans == 0.0)
                        continue;
                    ss = fabs((double) rfds_cuckoo_increz.query(cur_time) / ans - 1.0);
                    //printf("query time %lld\t%d\t%.6lf\t%.6lf\n", cur_time, cdm.query(cur_time), ans, ss);
                    sum += ss;
                    std_value += std::pow(ss, 2);
                    cnt++;
                    out << cur_time << " " << ans << " " << rfds_cuckoo_increz.query(cur_time) << "\n";
                    pre_time = pre_time + (cur_time - pre_time) / 1000;
                }
                cur_time = timestamps[++ _item];
            }
        }
        printf("memory:%d Byte\t ave error:%.6lf\t ave std:%.6lf\n", mem / 8, sum / cnt, std::sqrt(std_value / cnt));
    }
}

void testAccWithDiffMem(int datNo, int start, int end, int _window, int repmax = 1) {
    switch (datNo) {
        case 0:
            load_data1_TCP();
            break;
        case 1:
            load_data2_clickstream();
            break;
    }
    window = _window;
    std::cout << "[CDM]" << std::endl;
    testCDM(start, end, _window, repmax);
//     std::cout << "[RFDS linear]" << std::endl;
//     testRFDSLinear(start, end,_window,repmax);
//     std::cout << "[RFDS double]" << std::endl;
//     testRFDSDouble(start, end,_window, repmax);
    // std::cout << "[RFDS cuckoo]" << std::endl;
    // testRFDSCuckoo(start, end, _window, repmax);
}

int main() {
    int datNo = 0;
    int start = 15;
    int end = 15;
    int _window = 10000000;
    testAccWithDiffMem(datNo, start, end, _window, 30);
    /*load_data1_TCP();
    for (int mem = (1 << start); mem <= (1 << end); mem <<= 1) {
        std::cout << mem << std::endl;
        testThroughput(0, mem, datNo, _window);
        testThroughput(1, mem, datNo, _window);
        testThroughput(2, mem, datNo, _window);
        testThroughput(3, mem, datNo, _window);
    }*/
    
    /*int datNo = 0;
    int start = 17;
    int end = 17;
    int _window = 10000000;
    testAccWithDiffMem(datNo, start, end, _window, 5);*/
    //testAccWithDiffWin(datNo, 10, 100000, 1000000);
    return 0;
}

