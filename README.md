## ErasableSketch

### Introduction

Cardinality estimation, critical for databases, networking, and security, often lacks efficient support for deletions in dynamic environments. Traditional methods like HyperLogLog handle only insertions, while recent solutions such as RFDS suffer from a time complexity linear to its hash table size, insufficient memory utilization, and unstable performance on both software platforms and hardware accelerators. In this paper, we present Erasable Sketch, a novel framework that supports efficient item-level deletions while maintaining $O(1)$ time per-item processing and sublinear memory usage. By integrating item-wise adaptive sampling and a virtual bucket array structure, Erasable Sketch enables arbitrary deletions with minimized overhead. It also supports both manual and policy-driven deletions, addressing real-world data management needs. We implement Erasable Sketch on FPGA-based platforms. Extensive experiments demonstrate superior performance and memory efficiency in real-world evaluations, improving the per-item processing throughput by up to $544.30\times$ and reducing the estimation error by up to $6.09\times$, offering a scalable and robust solution for dynamic cardinality estimation.

### About this repo

The core **ErasableSketch** structure is implemented in **./Algs**.

Other baseline methods are also implemented in **./Algs**.

The general modules are placed under the **/Utils**.

The related algorithms of **temporal policy based deletion in User behavior streams** are implemented in **./Algs**.

The related algorithms of **temporal & manual policy based deletion in TCP flows** are implemented in **./AlgsTCP**.

The related algorithms of **spatial-temporal policy based deletion in traffic stream** are implemented in **./AlgsLoc**.

### Requirements

- g++ (gcc-version >= 13.1.0)

### How to build

Before run those codes, you need to download the datasets from https://catalog.caida.org/details/dataset/passive_2019_pcap , then move them to ./data .

We preprocessed the datasets to remove all IPv6 packets to ensure that each packet contains a source IP address, a destination IP address, and a timestamp.

You can use the following commands to build and run.

```
g++ ./tcp_CDM_4KB.cpp -o tcp_CDM_4KB -std=c++17 -mavx2 -O3
./tcp_CDM_4KB
```
```
g++ ./tcp_rfds_linear_4KB.cpp -o tcp_rfds_linear_4KB -std=c++17 -mavx2 -O3
./tcp_rfds_linear_4KB
```
```
g++ ./tcp_rfds_double_4KB.cpp -o tcp_rfds_double_4KB -std=c++17 -mavx2 -O3
./tcp_rfds_double_4KB
```
```
g++ ./tcp_rfds_cuckoo_4KB.cpp -o tcp_rfds_cuckoo_4KB -std=c++17 -mavx2 -O3
./tcp_rfds_cuckoo_4KB
```
> The CDM (i.e., Cardinality Deletion Model) refers to our method, Erasable Sketch.
> The compile and run commands for **temporal policy-based deletion in user behavior streams** and **spatial-temporal policy-based deletion in traffic streams** are similar to those for **temporal and manual policy-based deletion in TCP flows**.
