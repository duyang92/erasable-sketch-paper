#include "./BOBHash32.h"
#include <fstream>
#include <sstream>
#include <string.h>
#include <vector>

#define KEY_SIZE 16
#define SESSION_SIZE 64
#define TCP_CHAR_SIZE 64
#define TCP_SIZE 8
const int MAXNUM=3*1e7+10;
//uint32_t flow[MAXNUM+10];
unsigned long long timestamps[MAXNUM + 10];
uint32_t locations[MAXNUM + 10];
vector<uint64_t> flow;
uint32_t flags[MAXNUM + 10];
int flow_number = 0;

BOBHash32 load_hash_id;
struct Cmp {
    bool operator()(const char* a, const char* b) const {
        return memcmp(a, b, TCP_CHAR_SIZE) == 0;
    }
};

struct HashFunc {
    unsigned int operator()(const char* key) const {
        unsigned int hashValue = 0;
        //MurmurHash3_x86_32(key, KEY_SIZE, 0, &hashValue);
        hashValue = load_hash_id.run(key, TCP_SIZE);
        return hashValue;
    }
};

std::unordered_map<char*, uint32_t, HashFunc, Cmp> session_map;


uint32_t convertIPv4ToUint32(char* ipAddress) {
    uint32_t result = 0;
    int octet = 0;
    char ipCopy[KEY_SIZE];
    strncpy(ipCopy, ipAddress, sizeof(ipCopy) - 1);
    ipCopy[sizeof(ipCopy) - 1] = '\0';
    char* token = strtok(ipCopy, ".");
    while (token != nullptr) {
        octet = std::stoi(token);
        result = (result << 8) + octet;
        token = strtok(nullptr, ".");
    }
    return result;
}


void load_data0(){
    for(int i=1;i<=MAXNUM;i++) {
        flow[i]=i;
    }
}

void load_data1_CAIDA(){
    //memset(timestamps, sizeof(uint64_t) * (MAXNUM + 10), 0);
    ifstream input( "./data/caida_exp_00.dat", ios::in | ios::binary );
    char buf[2000]={0};
    unsigned int ip;
    unsigned long long timestamp;
    //flow.push_back(0);
    //timestamps.push_back(0);
    for(int i=1;i<=MAXNUM;i++) {
        input.read(reinterpret_cast<char*>(&ip), sizeof(ip));
        input.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
        //flow.push_back(ip);
        flow_number ++;
        flow[i] = ip;
        //timestamps.push_back(timestamp);
        timestamps[i] = timestamp;
    }
    input.close();
    std::cout << flow_number << std::endl;
}

void load_data1_TCP(){
    load_hash_id.initialize(rand() % MAX_PRIME32);
    //memset(timestamps, sizeof(uint64_t) * (MAXNUM + 10), 0);
    ifstream input( "./data/caida_tcp_stream.txt", ios::in);
    if (input.is_open())
        std::cout << "TCP dataset has opened." << std::endl;
    char ipAddress[65];
    char timestampStr[65];
    char flagStr[3];
    unsigned long long timestamp;
    //flow.push_back(0);
    //timestamps.push_back(0);
    flow.push_back(0);
    for(int i=1;i<=MAXNUM;i++) {
        input >> timestampStr >> ipAddress >> flagStr;
        flow.push_back(load_hash_id.run(ipAddress, TCP_SIZE));
        timestamp = (unsigned long long) std::atoll(timestampStr);
        timestamps[i] = timestamp;
        flags[i] = (unsigned int) std::atoi(flagStr);
        flow_number ++;
    }
    input.close();
    std::cout << flow_number << std::endl;
}

void load_data2_clickstream(){
    load_hash_id.initialize(rand() % MAX_PRIME32);
    ifstream input( "./data/clickstreams_dataset.txt", ios::in);
    if (input.is_open())
        std::cout << "Click datafile has opened." << std::endl;
    uint32_t sessionID = 0;
    uint64_t timestamp;
    char session_str[65];
    char timestamp_str[65];
    flow.push_back(0);
    //timestamps.push_back(0);
    for (int i = 1; i <= MAXNUM; i++) {
        input >> session_str >> timestamp_str;
        if (session_map.find(session_str) == session_map.end()) {
            session_map[session_str] = sessionID ++;
        }
        timestamp = (unsigned long long) std::atoll(timestamp_str);
        //flow[i] = session_map[session_str];
        timestamps[i] = timestamp;
        flow.push_back(session_map[session_str]);
        //timestamps.push_back(timestamp);
        flow_number ++;
    }
    input.close();
}

void load_data3_traffic() {
    ifstream input( "./data/traffic_data_sort_V2.txt", ios::in);
    if (input.is_open())
        std::cout << "Traffic datafile has opened." << std::endl;
    uint32_t carID;
    uint32_t status;
    uint64_t timestamp;
    uint32_t location;
    char carIDStr[10];
    char timestamp_str[64];
    char status_str[5];
    char number[5];
    flow.push_back(0);
    for (int i = 1; i <= MAXNUM; ++i) {
        input >> carIDStr >> status_str >> timestamp_str >> number;
        carID = (unsigned int) std::atoi(carIDStr);
        //status = (unsigned int) std::atoi(status_str);
        timestamp = (unsigned long long) std::atoll(timestamp_str);
        location = (unsigned int) std::atoi(number);
        //flow[i] = carID;
        flow.push_back(carID);
        timestamps[i] = timestamp;
        locations[i] = location;
        flow_number ++;
    }
    input.close();
}