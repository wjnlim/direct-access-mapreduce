# direct-access-mapreduce — MapReduce Framework with Direct Data Access

An experimental MapReduce prototype for direct intermediate-data access over shared block storage.

## Overview
This repository contains the implementation of this **experimental MapReduce prototype**.

Conventional MapReduce systems exchange intermediate data through **application-level transfer services** such as shuffle handlers and fetchers. These services coordinate the movement of intermediate data between workers during the shuffle phase.

This prototype implements a **MapReduce data-access model** that provides **direct intermediate-data access** over **shared block storage**. Reducers read intermediate files stored on a **shared iSCSI volume**
instead of transferring data between nodes through dedicated transfer services.

The implementation was developed to examine the feasibility of the direct-access model and to explore system-level behavior. It is not designed as a production-ready MapReduce system and does not attempt to evaluate performance improvements.

## Direct-Access Model
MapReduce intermediate data follows a characteristic access pattern: it is written once by a mapper and subsequently read by reducers.
This single-writer, multiple-reader pattern naturally constrains concurrent file access.

Based on this property, the model explores a direct-access approach in which intermediate data is stored on shared block storage and accessed directly by workers.

It operates without a shared filesystem layer, avoiding additional filesystem-level coordination.

In this model, mappers write intermediate data to files on a shared iSCSI volume, and reducers open and read these files from the shared storage.

## Shared Storage Behavior & Experimental Controls

Operating the model without a shared filesystem layer exposes a key system constraint when multiple nodes access the same block device: file visibility across nodes depends on how filesystem metadata is cached and updated on each host.

In the shared iSCSI storage environment in which the model operates, files created on one node may not become visible to other nodes because iSCSI provides no cache-coherence mechanisms across hosts.

During experimentation with multi-node access to the shared volume, this behavior manifested as file lookup failures when reducers attempted to open intermediate files stored on the shared volume. In particular, stale directory metadata and cached negative dentries prevented reducers from opening these intermediate files.

To address these issues, several explicit controls were introduced:
- **Pre-allocated intermediate files** were created in advance on the shared volume and reused as intermediate-data containers to maintain consistent directory metadata across nodes.
- **Explicit durability control** was applied after writes using `fsync()` on both the file descriptor and the underlying block device to ensure that file updates became visible across hosts.
- **O_DIRECT reads** were performed by reducers to bypass the page cache for direct reads from shared storage.

These controls allow intermediate files on the shared iSCSI volume to become consistently visible to worker nodes without introducing a shared filesystem layer.

A detailed discussion of the kernel-level behavior, cache interactions, and experimental observations is provided in the accompanying technical report
([mr_direct_data_access.pdf](docs/mr_direct_data_access.pdf)).
## System Architecture & Implementation

To realize the direct-access model in an executable form, **the MapReduce prototype** was implemented as a layered system stack.

The system is composed of three primary layers: a MapReduce framework layer, an asynchronous communication layer, and an event-driven I/O engine. These components together support distributed task coordination and the execution of MapReduce jobs in the prototype.


The diagram below illustrates the execution structure of the framework and the interaction between the master, worker nodes, and shared storage during MapReduce job execution.

![Execution Structure of the Direct-Access MapReduce Framework](images/Execution%20Structure%20of%20the%20Direct-Access%20MapReduce.png)

### MapReduce Framework Layer

The framework layer implements the MapReduce execution logic used in the prototype.

- Input data is partitioned into **input splits** and distributed to worker nodes before job execution.
- The master assigns map/reduce tasks and coordinates task execution.
- Map tasks read input splits and write intermediate data to assigned **pre-allocated files**('sharedfiles' on diagram) on **the shared storage** of their worker nodes.
- The master tracks the locations of these intermediate files and coordinates reduce task execution.
- Reduce tasks open and read intermediate files **directly** from the corresponding mapper-node shared storage.
- Reducers produce **output partitions** as the final result.

This design removes application-level intermediate data transfer services (e.g., shuffle handlers and fetchers) typically used for exchanging intermediate data between workers.

### Asynchronous Message-Passing Layer

The prototype uses a message-passing library ([msg_pass](https://github.com/wjnlim/msg_pass.git)) to support master–worker communication.

- Messages are exchanged asynchronously between master and worker processes.

### Epoll-Based Event Engine

The prototype uses an epoll-based event engine ([ep_engine](https://github.com/wjnlim/ep_engine.git)), which provides the event-driven I/O foundation for the communication layer.

## Repository Layout and Usage
### Scripts and Configuration Files
The repository includes several scripts and configuration files used to set up the cluster environment and run MapReduce jobs.

- ```sharedfiles```: Contains a list of **\<filename\>:\<size\>** pairs, one per line, 
specifying the pre-allocated intermediate files.

- ```alloc_shared_files.sh```: Creates the shared intermediate files (listed in ```sharedfiles```) on the device provided as input.

- ```set_env.sh```: Sets the ```DA_MR_HOME``` envrionment variable, which defines the project’s base directory.

- ```workers```: Contains **\<worker name\>:\<ip\>** pairs. Used by scripts for worker name ↔ IP resolution.

- ```mnt_targets.sh```: Logs into all worker nodes' iSCSI targets and mounts them under the ```$DA_MR_HOME/mnt``` directory.

- ```init_workers.sh```: Initializes worker nodes by creating directories for input splits(```$DA_MR_HOME/data/inputs```), output partitions(```$DA_MR_HOME/data/outputs```), and MapReduce executables(```$DA_MR_HOME/mapred_bin```).
It also runs the ```mnt_targets.sh``` on each node, and generates the ```workers_n_splits``` metadata file, which stores
**\<worker\>:\<num_of_input_split\>** pairs used for input split distribution.


- ```gen_wordcount_input.sh```: Generates a random input file for the WordCount MapReduce example.

- ```distr_input.sh```: Distributes an input file across the specified workers and produces a metadata file containing **\<file path\>:\<worker\>** mappings.

- ```rm_isplits.sh```: Deletes the metadata and input splits associated with a given input.

- ```rm_outputs.sh```: Deletes the metadata and partitions associated with a given output.

- ```print_output.sh```: Prints the final output by merging all output partitions.

- ```run_worker.sh```: Start a worker process.

- ```start_workers.sh```: Starts worker processes on all worker nodes.

- ```stop_workers.sh```: Stops worker processes on all worker nodes.

- ```run_mapred.sh```: Copies a user-provided MapReduce executable to all workers and launches the ```master``` program to execute the MapReduce workflow.

### Cluster prerequisites
The project assumes a 4-node cluster environment with passwordless SSH from the master to all nodes.
1. Prepare 4 nodes, for example:
   - master/worker0 (172.30.1.33) // this also runs the master process
   - worker1 (172.30.1.34)
   - worker2 (172.30.1.35)
   - worker3 (172.30.1.36)
1. Prepare a dedicated raw block device on each node for the shared iSCSI target, 
e.g.:
    - /dev/sdX
1. Obtain each node’s initiator name using: ```cat /etc/iscsi/initiatorname.iscsi```:
   - worker0: iqn.1994-05.com.redhat:AAAA
   - worker1: iqn.1994-05.com.redhat:BBBB
   - worker2: iqn.1994-05.com.redhat:CCCC
   - worker3: iqn.1994-05.com.redhat:DDDD

### System setup
#### Per-Node Setup (the following steps must be performed on **each node**)
1. Configure the iSCSI target.\
Replace \<worker name\>, /dev/sdX, /dev/sdY, \<ip\>,  and initiator IQNs ('iqn.1994-05.com.redhat:AAAA',...) with your values.
```bash
# prerequisites
# install targetcli
yum install targetcli -y
systemctl start target
systemctl enable target

# open port 3260 for iSCSI
firewall-cmd --permanent --add-port=3260/tcp
firewall-cmd --reload

# install initiator
yum install iscsi-initiator-utils -y
systemctl enable iscsid iscsi
systemctl start iscsid iscsi

# create a block backstore
# naming format: <worker name>_shared
sudo targetcli "backstores/block create name=<worker name>_shared dev=/dev/sdX"

# create an iSCSI target with IQN
# naming format: iqn.2022-05.<ip>:<worker name>
sudo targetcli "iscsi/ create iqn.2022-05.<ip>:<worker name>"

# create a LUN
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/luns \
create /backstores/block/<worker name>_shared"

# Add all initiator names to the ACL, enabling them to
# connect the target
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/acls \
create iqn.1994-05.com.redhat:AAAA"
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/acls \
create iqn.1994-05.com.redhat:BBBB"
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/acls \
create iqn.1994-05.com.redhat:CCCC"
sudo targetcli "iscsi/iqn.2022-05.<ip>:<worker name>/tpg1/acls \
create iqn.1994-05.com.redhat:DDDD"
# save the configuration
sudo targetcli "saveconfig"

# discover and log in to the local target
iscsiadm --mode discovery --type sendtargets --portal <ip>
iscsiadm -m node -T iqn.2022-05.<ip>:<worker name> -l

# verify device (e.g., /dev/sdY)
lsscsi

# format the shared device with the ext4 file system
sudo mkfs.ext4 /dev/sdY
```

2. Build and install the project:
```bash
git clone https://github.com/wjnlim/da_mr.git

mkdir da_mr/build
cd da_mr/build

cmake -DCMAKE_INSTALL_PREFIX=<your install directory> ..
cmake --build . --target install
```
3. Set the project environment variable:
```bash
# Change directory to da_mr/
cd ..
./set_env.sh && source ~/.bashrc
```
4. Configure the ```sharedfiles``` file (default: 64 files named shfileXX, each 2 MB) and create the shared files on the target device (ensure the node is logged into the target):
```bash
./alloc_shared_files.sh /dev/sdY
```
<!-- 8. configure the ```workers``` file with **\<worker name\>:\<ip\>** pairs, one per line, on **each node** -->
---
#### Master-Node Setup
1. configure the ```workers``` file with **\<worker name\>:\<ip\>** pairs, one per line.
2. **After the per-node setup is complete**, Initialize the workers from the master:
```bash
./init_workers.sh
```
### Run a WordCount example
1. Write and compile a MapReduce program
(example: [mr_wordcount.c](mr_wordcount.c)):
```bash
gcc mr_wordcount.c -o mr_wordcount -I <your install directory>include/ \
<your install directory>lib/libda_mr.a -lpthread
```
2. (Optional) Generate a WordCount input:
```bash
# ex) ./gen_wordcount_input.sh inputs/32M_input 32Mi
./gen_wordcount_input.sh <path to input> 32Mi
```
3. Distribute the input across the workers (this will generate a .meta file for the input):
```bash
# across all workers, 2MB chunks
# ex) ./distr_input.sh inputs/32M_input 2M 
# or across worker0 and worker1 only
# ex) ./distr_input.sh inputs/32M_input 2M -w "worker0 worker1"
./distr_input.sh <path to input> <chunk size>
```
4. Start worker processes on all worker nodes from the **master**:
```bash
./start_workers.sh
```
5. Run the MapReduce job (this will generate a .meta file for the output):
```bash
# ex) ./run_mapred.sh mr_wordcount inputs/32M_input.meta outputs/32M_output.meta
./run_mapred.sh <path to the wordcount executable> \
<path to the input metadata> <path to the output metadata>
```
6. Print the output:
```bash
# ex) ./print_output.sh outputs/32M_output.meta -s # '-s' for sorted output
./print_output.sh <path to the output metadata>
```