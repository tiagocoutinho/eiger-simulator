# Benchmarks

### Hosts

#### ctsimdetector

* 8 CPU (Intel(R) Xeon(R) Gold 6136 CPU @ 3.00GHz)
  * hypervisor: Xen
  * caches: L1d: 32K, L1i: 32K, L2: 1024K, L3: 25344K
  * thread p/ core: 1, cores p/ socket: 1
* 8 GB RAM
* ETH 10Gb

#### ctsimingestion

* 8 CPU (Intel(R) Xeon(R) Gold 6136 CPU @ 3.00GHz)
  * hypervisor: Xen
  * caches: L1d: 32K, L1i: 32K, L2: 1024K, L3: 25344K
  * thread p/ core: 1, cores p/ socket: 1
* 32 GB RAM
* ETH 10Gb
* SSD hard drive on /dev/xvdb1

## 2020-02-26

* eiger-simulator
  *  git version: ff48f0
  * sample dataset from Eiger
  * sending LZ4 compressed images through ZMQ (lima does not support bitshuffle yet)
  * LZ4 image size is around 5.7MB

* lima-core 1.9.1 on conda with python 3.7
* lima-camera-eiger 1.9.3 with patch to support configurable http port

### Description

> No saving benchmark max. **sustained** speed between detector and lima.
> Variables: *nb processing tasks*; *exposure time*

### Commands

run `eiger-simulator` in **ctsimdetector** with:

```
eiger-simulator --max-memory=1_000_000_000 \
                --dataset=/home/sicilia/mx-datasets/EIGER_9M_datasets/transthyretin_200Hz/200Hz_0.1dpf_1_master.h5
```

(initializes 175 frames in memory)

run `eigersim/examples/lima.py` in **ctsimingestion** with:

```
python lima.py --url=10.24.24.4:8000 \
               --max-buffer-size=5 \
               --nb-processing-tasks=$NB_PROCESSING \
               -n 10_000 \
               -e $EXPOSURE_TIME
```
* URL is forced with IP to ensure the 10Gb interface is used
* max buffer size is set 5% to ensure the acquisition can be sustained at least for 10k frames.

### Results

| # proc. tasks | Max. freq. | Flux |
| ---:| -------:| ---------:|
|  1  | 66.7 Hz |  280 MB/s |
|  2  |  140 Hz |  798 MB/s |
|  3  |  220 Hz | 1254 MB/s |
|  4  |  270 Hz | 1540 MB/s |
