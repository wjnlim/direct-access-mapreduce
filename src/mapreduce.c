
#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
// #include <sys/queue.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <dirent.h>
#include <signal.h>

#include "da_mr/mapreduce.h"
#include "msg_pass/mp_buf_sizes.h"
#include "msg_pass/mp_client.h"
#include "utils/my_err.h"
#include "utils/socket_helper.h"
#include "ep_engine/epoll_event_engine.h"
#include "shared_file_struct.h"
#include "MR_msg_type.h"

#define FNV_OFFSET 2166136261u
#define FNV_PRIME 16777619
#define INT_SIZE sizeof(int)
#define INITIAL_KVS_CAPACITY 128

#define BLOCK_SIZE 4096


typedef struct KV {
    const char* key;
    const char* value;
} KV;

typedef struct KV_arr {
    // KV_list kvs;
    KV* kvs;
    int kvs_capacity;
    int n_kvs;
    int kvs_size;
} KV_arr;

Mp_client* g_parent_conn;
EP_engine* g_engine;
pthread_t g_ev_loop_thr;

const char* g_task_type;
const char* g_job_id;
const char* g_task_id;

typedef struct Mapper_context {

    bool got_shared_file;
    struct {
        char shared_dir[PATH_MAX];
        char shared_file_name[NAME_MAX];
        // char path[PATH_MAX];
        int size;
        int capacity;
        int metadata_size;
    } mapoutput;

    int n_partitions;
    KV_arr* partitions;
    int partitions_data_size;
    int partitions_metadata_size;


    const char* inputsplit_path;
    const char* shared_dev_file;

    pthread_mutex_t lock;
    pthread_cond_t cv;
    // pthread_t ev_loop_thr;
} Mapper_context;

static Mapper_context g_mctx;

typedef struct Reducer_context {

    int partition_number;
    char intermediate_file_path[PATH_MAX];
    char output_file_path[PATH_MAX];
    FILE* outputfile;

    char* aligned_file_buf;
    int aligned_file_buf_size;
    int intermediate_metadata_size;

    KV_arr copied_mapoutput;
    struct {
        int kv_idx;
    } KV_iterator;

    bool copy_intermediate_file_done;
    bool copy_intermediate_file_err;    
    pthread_mutex_t lock;
    pthread_cond_t cv;
    // pthread_t ev_loop_thr;
} Reducer_context;

static Reducer_context g_rctx;

/*
    Get a shared file for intermediate map output from parent
*/
static void request_shared_file();
static void task_done(bool task_failed);
static void mapper_ctx_init_partitions();

static void request_intermediate_file();
static void test_request_intermediate_file();

static uint32_t hash_key(const char* key) {
    uint32_t hash = FNV_OFFSET;
    for (const char* c = key; *c != '\0'; c++) {
        hash ^= (uint32_t)(*c);
        hash *= FNV_PRIME;
    }
    
    return hash;
}

static uint32_t mr_hash_partition(const char* key) {
    return hash_key(key) % g_mctx.n_partitions;
}

static int KV_arr_add(KV_arr* kva, const char* key, const char* val) {
    if (kva->kvs_capacity == kva->n_kvs) {
        kva->kvs_capacity *= 2;
        kva->kvs = realloc(kva->kvs, kva->kvs_capacity * sizeof(KV));
    }
    // add kv
    char* k = strdup(key);
    if (k == NULL) {
        err_msg_sys("[(%d)(%s)mapreduce.c: KV_arr_add()] strdup(key) error", getpid(), g_task_id);
        return -1;
    }
    kva->kvs[kva->n_kvs].key = k;
    char* v = strdup(val);
    if (v == NULL) {
        err_msg_sys("[(%d)(%s)mapreduce.c: KV_arr_add()] strdup(val) error", getpid(), g_task_id);
        free(k);
        return -1;
    }
    kva->kvs[kva->n_kvs].value = v;

    int kv_size = strlen(k) + strlen(v) + 2;
    // kva->kvs[kva->n_kvs].size = strlen(k) + strlen(v) + 2;

    kva->kvs_size += kv_size;
    kva->n_kvs++;

    // g_mctx.partitions_data_size += kv_size;
    return kv_size;
}



static void notify_mapoutput() {
    char msg[MP_MAXMSGLEN];
    // T_MAPOUTPUT <job_id> <tid> <sharedfile path> <metadata size> <has data>
    // if data has written in the shared file ? 1 : 0
    sprintf(msg, "%s %s %s %s %d %d\n", T_MAPOUTPUT, g_job_id, g_task_id,
                    g_mctx.mapoutput.shared_file_name,
                    g_mctx.mapoutput.metadata_size, g_mctx.mapoutput.size > 0 ? 1 : 0);
    if (mp_client_send_request(g_parent_conn, msg, NULL, NULL) < 0) {
        err_exit("[(%d)(%s)mapreduce.c: notify_mapoutput()] mp_client_send_request() error", getpid(), g_task_id);
    }
}

static int write_to_mapoutput_file() {
    char mpath[PATH_MAX];
    snprintf(mpath, sizeof(mpath), "%s/%s", g_mctx.mapoutput.shared_dir, g_mctx.mapoutput.shared_file_name);
    printf("\n[[mapred(%d)(tid: %s)]] Writting intermediate data to '%s'\n", getpid(), g_task_id, mpath);
    // int ofd = open( , O_WRONLY); // no O_TRUNC to overwrite the file on same blocks
    int ofd = open(mpath, O_WRONLY);
    if (ofd < 0) {
        err_msg_sys("[(%d)(%s)mapreduce.c: write_to_mapoutput_file()] open(shf fd) error", getpid(), g_task_id);
        return -1;
    }
    
    FILE* ofs = fdopen(ofd, "w");
    if (ofs == NULL) {
        err_msg_sys("[(%d)(%s)mapreduce.c: write_to_mapoutput_file()] fdopen() error", getpid(), g_task_id);
        close(ofd);
        return -1;
    }

    int metadata_size = 0;
    int n_written = 0;
    // Write metadata
    int offset = 0;
    // for p in partitions
    for (int i = 0 ; i < g_mctx.n_partitions; i++) {
        printf("[[mapred(%d)(tid: %s)]] metadata: p_num: %d, offset: %d, n_kvs: %d, kvs_size: %d\n", 
            getpid(), g_task_id, i, offset, g_mctx.partitions[i].n_kvs, 
                                                    g_mctx.partitions[i].kvs_size);
    //  write meta data // <partition number> <offset> <# of kvs> <total(key-value)s size>...
        if (fwrite(&i, INT_SIZE, 1, ofs) < 1) {
            err_msg_sys("[(%d)(%s)mapreduce.c: write_to_mapoutput_file()] fwrite(i) error", getpid(), g_task_id);
            goto FWRITE_ERR;
        }
        if (fwrite(&offset, INT_SIZE, 1, ofs) < 1) {
            err_msg_sys("[(%d)(%s)mapreduce.c: write_to_mapoutput_file()] fwrite(offset) error", getpid(), g_task_id);
            goto FWRITE_ERR;
        }
        if (fwrite(&g_mctx.partitions[i].n_kvs, INT_SIZE, 1, ofs) < 1) {
            err_msg_sys("[(%d)(%s)mapreduce.c: write_to_mapoutput_file()] fwrite(n_kvs) error", getpid(), g_task_id);
            goto FWRITE_ERR;
        }
        if (fwrite(&g_mctx.partitions[i].kvs_size, INT_SIZE, 1, ofs) < 1) {
            err_msg_sys("[(%d)(%s)mapreduce.c: write_to_mapoutput_file()] fwrite(kvs_size) error", getpid(), g_task_id);
            goto FWRITE_ERR;
        }
        offset += g_mctx.partitions[i].kvs_size;
        n_written += (4 * INT_SIZE);
    }
    metadata_size = n_written;
    assert(metadata_size == g_mctx.partitions_metadata_size);
    // g_mapoutput_file_size += g_partitions_metadata_size;

    KV_arr* kva;
    KV* kv;
    int n;
    // for p in partitions
    for (int i = 0; i < g_mctx.n_partitions; i++) {
    //  write (key-value)s // <key><value><key><value>....
        kva = &g_mctx.partitions[i];
        for (int j = 0; j < kva->n_kvs; j++) {
            kv = &kva->kvs[j];
            n = fprintf(ofs, "%s%c%s%c", kv->key, '\0', kv->value, '\0');
            assert(n == (int)(strlen(kv->key) + strlen(kv->value) + 2));
            free((void*)kv->key);
            free((void*)kv->value);

            n_written += n;
        }
    }
    // assert(g_mapoutput_file_size == g_partitions_data_size);
    // fflush
    if (fflush(ofs) < 0) {
        err_msg_sys("[(%d)(%s)mapreduce.c: write_to_mapoutput_file()] fflush(ofs) error", getpid(), g_task_id);
        goto FFLUSH_ERR;
    }

    if (fsync(ofd) < 0) {
        err_msg_sys("[(%d)(%s)mapreduce.c: write_to_mapoutput_file()] fsync(ofd) error", getpid(), g_task_id);
        goto FSYNC_OFD_ERR;
    }

    int dev_fd = open(g_mctx.shared_dev_file, O_RDONLY);
    if (dev_fd < 0) {
        err_msg_sys("[(%d)(%s)mapreduce.c: write_to_mapoutput_file()] open(dev fd) error", getpid(), g_task_id);
        goto OPEN_DEV_ERR;
    }

    if (fsync(dev_fd) < 0) {
        err_msg_sys("[(%d)(%s)mapreduce.c: write_to_mapoutput_file()] fsync(dev fd) error", getpid(), g_task_id);
        goto FSYNC_DEV_ERR;
    }

    fclose(ofs);
    close(dev_fd);

    assert(n_written == g_mctx.partitions_data_size);
    g_mctx.mapoutput.size = n_written;
    g_mctx.mapoutput.metadata_size = metadata_size;

    return 0;

FSYNC_DEV_ERR:
    close(dev_fd);
OPEN_DEV_ERR:
FSYNC_OFD_ERR:
FFLUSH_ERR:
FWRITE_ERR:
    fclose(ofs);
    return -1;
}

static int flush_partitions() {
    int ret = write_to_mapoutput_file();
    notify_mapoutput();
    
    mapper_ctx_init_partitions();
    return ret;
}

void MR_emit_kv(char* key, char* val) {
    int kv_size = strlen(key) + strlen(val) + 2;
    // if data reach a shared file size then spill the data into the file
    if (g_mctx.partitions_data_size + kv_size > g_mctx.mapoutput.capacity) {
        // write all parition data to the shared file
        if (flush_partitions() < 0) {
            task_done(true);
        }
        // get new shared file
        request_shared_file();
        // g_mctx.partitions_data_size = g_mctx.partitions_metadata_size;
    }

    uint32_t partition_number = mr_hash_partition(key);
    if (KV_arr_add(&g_mctx.partitions[partition_number], key, val) < kv_size) {
        // err_exit_sys("[(%d)(%s)mapreduce.c: MR_emit_kv()] KV_arr_add() error");
        notify_mapoutput();
        task_done(true);
    }
    g_mctx.partitions_data_size += kv_size;

    // g_partitions_data_size += kv_size;
}

static void got_shared_file(Req_completion* rc, void* cb_arg) {
    if (rc->status != REQ_ERR_NONE) {
        err_exit("[(%d)(%s)mapreduce.c: got_shared_file()] req error, req status: %s\n"
        "req: %s, resp: %s", getpid(), g_task_id, req_status_str(rc->status), rc->request_msg, rc->respose_msg);
    }

    const char* msg = rc->respose_msg;
    if (strncmp(msg, T_SHF, strlen(T_SHF)) == 0) {
        sscanf(msg+strlen(T_SHF), " %s %s %d\n", g_mctx.mapoutput.shared_dir, 
            g_mctx.mapoutput.shared_file_name, &g_mctx.mapoutput.capacity);
        // sprintf(g_mctx.mapoutput.path, "%s/%s", g_mctx.shared_dir, g_mctx.shared_file_name);
        g_mctx.mapoutput.size = 0;
        pthread_mutex_lock(&g_mctx.lock);
        g_mctx.got_shared_file = true;
        // g_mctx.mapoutput_file = &g_mctx.shared_file;
        // g_mctx.mapoutput_file_size = 0;
        pthread_cond_signal(&g_mctx.cv);
        pthread_mutex_unlock(&g_mctx.lock);
    } else {
        err_exit("[(%d)(%s)mapreduce.c: got_shared_file()] Unknown resp msg: %s", getpid(), g_task_id, msg);
    }
}

/*
    Get a shared file for intermediate map output from parent
*/
static void request_shared_file() {
    char msg[MP_MAXMSGLEN];
    sprintf(msg, "%s %s %s\n", T_GET_SHF, g_job_id, g_task_id);

    // Shared_file* sf;
    if (mp_client_send_request(g_parent_conn, msg, got_shared_file, NULL) < 0) {
        err_exit("[(%d)(%s)mapreduce.c: request_shared_file()] mp_client_send_request() error", getpid(), g_task_id);
    }

    // waiting the shared file
    pthread_mutex_lock(&g_mctx.lock);
    while (!g_mctx.got_shared_file) {
        pthread_cond_wait(&g_mctx.cv, &g_mctx.lock);
    }
    g_mctx.got_shared_file = false;
    pthread_mutex_unlock(&g_mctx.lock);

    // return shf_p;
}

// static void map_task_done(bool task_failed) {
static void task_done(bool task_failed) {
    // Send done msg
    char msg[MP_MAXMSGLEN];
    // T_DONE <job_id> <task_id> <task_failed>
    sprintf(msg, "%s %s %s %d\n", T_DONE, g_job_id, g_task_id, task_failed);
    if (mp_client_send_request(g_parent_conn, msg, NULL, NULL) < 0) {
        err_exit("[(%d)(%s)mapreduce.c: task_done()] mp_client_send_request() error", getpid(), g_task_id);
    }

    // TODO: clean up task
    // if (strcmp(g_task_type, "m") == 0) {
    //     // clean up map task context
    // } else {
    //     // clean up readuce task context
    // }
    ep_engine_stop_event_loop(g_engine);
    int err = pthread_join(g_ev_loop_thr, NULL);
    if (err) {
        err_exit_errn(err, "[(%d)(%s)mapreduce.c: task_done()] pthread_join() error", getpid(), g_task_id);
    }

    exit(EXIT_SUCCESS);
}

static void run_map_task(Mapper map) {
    // notify parent task launched
    char msg[MP_MAXMSGLEN];
    // T_MT_LNCHD <job_id> <task_id>
    sprintf(msg, "%s %s %s\n", T_MT_LNCHD, g_job_id, g_task_id);
    if (mp_client_send_request(g_parent_conn, msg, NULL, NULL) < 0) {
        err_exit("[(%d)(%s)mapreduce.c: run_map_task()] mp_client_send_request() error", getpid(), g_task_id);
    }
    // init_map_task(inputsplit_path);

    printf("[[mapred(%d)(tid: %s)]] Running map task with inputsplit: %s\n", 
        getpid(), g_task_id, g_mctx.inputsplit_path);
    // g_partitions_data_size = g_partitions_metadata_size;
    request_shared_file();
    // map task
    map(g_mctx.inputsplit_path);

    bool t_failed = false;
    if (g_mctx.partitions_data_size > g_mctx.partitions_metadata_size) {
        if (flush_partitions() < 0) {
            t_failed = true;
        }
    }
    task_done(t_failed);
}

static void test_run_map_task(Mapper map) {
    // notify parent task launched
    char msg[MP_MAXMSGLEN];
    // T_MT_LNCHD <job_id> <task_id>
    sprintf(msg, "%s %s %s\n", T_MT_LNCHD, g_job_id, g_task_id);
    if (mp_client_send_request(g_parent_conn, msg, NULL, NULL) < 0) {
        err_exit("[(%d)(%s)mapreduce.c: run_map_task()] mp_client_send_request() error", getpid(), g_task_id);
    }
    // init_map_task(inputsplit_path);

    printf("[[mapred(%d)(tid: %s)]] Running test map task with inputsplit: %s\n", 
        getpid(), g_task_id, g_mctx.inputsplit_path);
    // g_partitions_data_size = g_partitions_metadata_size;
    request_shared_file();
    g_mctx.mapoutput.metadata_size = g_mctx.partitions_metadata_size;
    g_mctx.mapoutput.size = 1;
    notify_mapoutput();

    request_shared_file();
    g_mctx.mapoutput.metadata_size = g_mctx.partitions_metadata_size;
    g_mctx.mapoutput.size = 1;
    notify_mapoutput();
    // map task
    // map(g_mctx.inputsplit_path);
    printf("[[mapred(%d)(tid: %s)]] Doing dummy map task\n", getpid(), g_task_id);

    bool t_failed = false;

    task_done(t_failed);
}

static void* ep_engine_event_loop_thread(void* arg) {

    int err = ep_engine_start_event_loop(g_engine);
    if (err < 0) {
        err_exit("[(%d)(%s)mapreduce.c: ep_engine_event_loop_thread()] ep_engine_start_event_loop() error", getpid(), g_task_id);
    }

    return NULL;
}

static void KV_arr_init(KV_arr* kva) {
    if (kva->kvs == NULL) {
        kva->kvs = (KV*)malloc(INITIAL_KVS_CAPACITY * sizeof(KV));
        if (kva->kvs == NULL) {
            err_exit_sys("[(%d)(%s)mapreduce.c: KV_arr_init()] malloc error", getpid(), g_task_id);
        }
        kva->kvs_capacity = INITIAL_KVS_CAPACITY;
    }
    kva->n_kvs = 0;
    kva->kvs_size = 0;
}

static void mapper_ctx_init_partitions() {
    for (int i = 0; i < g_mctx.n_partitions; i++) {
        KV_arr_init(&g_mctx.partitions[i]);
    }

    g_mctx.partitions_data_size = g_mctx.partitions_metadata_size;
}

static void mapper_ctx_init(const char* inputsplit_path,
            int n_partitions, const char* shared_dev_file) {
    // g_parent_conn = parent_conn;
    // g_engine = engine;
    bzero(&g_mctx, sizeof(g_mctx));

    g_mctx.got_shared_file = false;
    g_mctx.mapoutput.size = 0;
    g_mctx.mapoutput.capacity = 0;
    g_mctx.mapoutput.metadata_size = 0;

    g_mctx.n_partitions = n_partitions;
    g_mctx.partitions = (KV_arr*)calloc(n_partitions, sizeof(KV_arr));
    g_mctx.partitions_metadata_size = n_partitions * 4 * INT_SIZE;
    mapper_ctx_init_partitions();

    // g_job_id = job_id;
    // g_task_id = task_id;

    g_mctx.inputsplit_path = inputsplit_path;
    g_mctx.shared_dev_file = shared_dev_file;

    pthread_mutex_init(&g_mctx.lock, NULL);
    pthread_cond_init(&g_mctx.cv, NULL);
    // g_ev_loop_thr = ev_loop_thr;
}

static void reducer_ctx_init(int partition_number, const char* output_path, 
                                                        int shared_file_size) {
    bzero(&g_rctx, sizeof(g_rctx));

    g_rctx.partition_number = partition_number;
    // bzero(g_rctx.intermediate_file_path, sizeof(g_rctx.intermediate_file_path));
    sprintf(g_rctx.output_file_path, "%s", output_path);
    g_rctx.outputfile = NULL;

    if(posix_memalign((void**)&g_rctx.aligned_file_buf, BLOCK_SIZE, shared_file_size)) {
        err_exit_sys("[(%d)(%s)mapreduce.c: reducer_ctx_init()] posix_memalign() error", getpid(), g_task_id);
    }
    g_rctx.aligned_file_buf_size = shared_file_size;
    g_rctx.intermediate_metadata_size = 0;

    KV_arr_init(&g_rctx.copied_mapoutput);
    g_rctx.KV_iterator.kv_idx = 0;

    g_rctx.copy_intermediate_file_done = false;
    g_rctx.copy_intermediate_file_err = false;

    pthread_mutex_init(&g_rctx.lock, NULL);
    pthread_cond_init(&g_rctx.cv, NULL);
}

// TODO: For now, we use a buffer large enough to hold a whole shared file
static int read_kvs_from_intermediate_file() {

    int ifd = open(g_rctx.intermediate_file_path, O_RDONLY | O_DIRECT);
    if (ifd < 0) {
        err_msg_sys("[(%d)(%s)mapreduce.c: read_kvs_from_intermediate_file()] open(%s) error", getpid(), g_task_id,
                                                    g_rctx.intermediate_file_path);
        return -1;
    }

    ssize_t nread;
    if ((nread = read(ifd, g_rctx.aligned_file_buf, g_rctx.aligned_file_buf_size)) < 0) {
        err_msg_sys("[(%d)(%s)mapreduce.c: read_kvs_from_intermediate_file()] read(metadata) error", getpid(), g_task_id);
        return -1;
    }
    assert(nread == g_rctx.aligned_file_buf_size);

    char* bufp = g_rctx.aligned_file_buf;
    // read meta data
    int metadata_offset = g_rctx.partition_number * 4 * INT_SIZE;
    // char* bufp = g_rctx.aligned_file_buf + metadata_offset;
    bufp += metadata_offset;
    int p_num, data_offset, n_kvs, kvs_size;
    memcpy(&p_num, bufp, INT_SIZE); bufp+=INT_SIZE;
    memcpy(&data_offset, bufp, INT_SIZE); bufp+=INT_SIZE;
    memcpy(&n_kvs, bufp, INT_SIZE); bufp+=INT_SIZE;
    memcpy(&kvs_size, bufp, INT_SIZE); bufp+=INT_SIZE;

    printf("[[mapred(%d)(tid: %s)]] metadata: p_num: %d, data_offset: %d, n_kvs: %d, kvs_size: %d\n",
                        getpid(), g_task_id, p_num, data_offset, n_kvs, kvs_size);

    bufp = g_rctx.aligned_file_buf + g_rctx.intermediate_metadata_size + data_offset;

    // add to kv list
    char* kp, *vp;
    int kv_size;
    // bufp = g_rctx.aligned_file_buf;
    int read_kvs_size = 0, n_read_kvs = 0;
    // while (partitions[partition_number].kvs_size < kvs_size) {
    while (read_kvs_size < kvs_size) {
        kp = bufp;
        vp = kp + strlen(kp) + 1;
        kv_size = KV_arr_add(&g_rctx.copied_mapoutput, kp, vp);
        if (kv_size < 0) {
            err_msg_sys("[(%d)(%s)mapreduce.c: read_kvs_from_intermediate_file()] KV_arr_add() error", getpid(), g_task_id);
            close(ifd);
            return -1;
        }
        bufp += kv_size;
        read_kvs_size += kv_size;
        n_read_kvs++;
    }

    printf("[[mapred(%d)(tid: %s)]] copied_mapoutput.kvs_size: %d, kvs_size: %d\n\n",
        getpid(), g_task_id, g_rctx.copied_mapoutput.kvs_size, kvs_size);
    // assert(partitions[partition_number].kvs_size == kvs_size);
    assert(read_kvs_size == kvs_size);
    // assert(partitions[partition_number].n_kvs == n_kvs);
    assert(n_read_kvs == n_kvs);
    close(ifd);

    return 0;
}

static void got_intermediate_file(Req_completion* rc, void* cb_arg) {
    if (rc->status != REQ_ERR_NONE) {
        err_exit("[(%d)(%s)mapreduce.c: got_intermediate_file()] req error, req status: %s\n"
        "req: %s, resp: %s", getpid(), g_task_id, req_status_str(rc->status), rc->request_msg, rc->respose_msg);
    }

    const char* msg = rc->respose_msg;
    if (strncmp(msg, T_INTF, strlen(T_INTF)) == 0) {
        sscanf(msg+strlen(T_INTF), " %s %d\n", 
            g_rctx.intermediate_file_path, &g_rctx.intermediate_metadata_size);
        if(read_kvs_from_intermediate_file() < 0) {
            // task_done(true);
            pthread_mutex_lock(&g_rctx.lock);
            g_rctx.copy_intermediate_file_err = true;
            pthread_cond_signal(&g_rctx.cv);
            pthread_mutex_unlock(&g_rctx.lock);
            return;
        }
        request_intermediate_file();
    } else if (strncmp(msg, T_START_REDUCE_PHASE, strlen(T_START_REDUCE_PHASE)) == 0) { 
        pthread_mutex_lock(&g_rctx.lock);
        g_rctx.copy_intermediate_file_done = true;
        pthread_cond_signal(&g_rctx.cv);
        pthread_mutex_unlock(&g_rctx.lock);
    } else {
        err_exit("[(%d)(%s)mapreduce.c: got_intermediate_file()] Unknown resp msg: %s", getpid(), g_task_id, g_task_id, msg);
    }

}

static void request_intermediate_file() {
    char msg[MP_MAXMSGLEN];
    sprintf(msg, "%s %s %s\n", T_GET_INTF, g_job_id, g_task_id);

    // Shared_file* sf;
    if (mp_client_send_request(g_parent_conn, msg, got_intermediate_file, NULL) < 0) {
        err_exit("[(%d)(%s)mapreduce.c: request_intermediate_file()] mp_client_send_request() error", getpid(), g_task_id);
    }
}

static int copy_phase() {
    request_intermediate_file();
    // waiting copy intermediate files done
    pthread_mutex_lock(&g_rctx.lock);
    while (!(g_rctx.copy_intermediate_file_done || g_rctx.copy_intermediate_file_err)) {
        pthread_cond_wait(&g_rctx.cv, &g_rctx.lock);
    }
    // g_rctx.copy_intermediate_file_done = false;
    pthread_mutex_unlock(&g_rctx.lock);

    return g_rctx.copy_intermediate_file_err ? -1 : 0;
}

static void test_got_intermediate_file(Req_completion* rc, void* cb_arg) {
    if (rc->status != REQ_ERR_NONE) {
        err_exit("[(%d)(%s)mapreduce.c: test_got_intermediate_file()] req error, req status: %s\n"
        "req: %s, resp: %s", getpid(), g_task_id, req_status_str(rc->status), rc->request_msg, rc->respose_msg);
    }

    const char* msg = rc->respose_msg;
    if (strncmp(msg, T_INTF, strlen(T_INTF)) == 0) {
        sscanf(msg+strlen(T_INTF), " %s %d\n", 
            g_rctx.intermediate_file_path, &g_rctx.intermediate_metadata_size);
        // if(read_kvs_from_intermediate_file() < 0) {
        //     task_done(true);
        // }
        test_request_intermediate_file();
    } else if (strncmp(msg, T_START_REDUCE_PHASE, strlen(T_START_REDUCE_PHASE)) == 0) { 
        pthread_mutex_lock(&g_rctx.lock);
        g_rctx.copy_intermediate_file_done = true;
        pthread_cond_signal(&g_rctx.cv);
        pthread_mutex_unlock(&g_rctx.lock);
    } else {
        err_exit("[(%d)(%s)mapreduce.c: test_got_intermediate_file()] Unknown resp msg: %s", getpid(), g_task_id, msg);
    }

}

static void test_request_intermediate_file() {
    char msg[MP_MAXMSGLEN];
    sprintf(msg, "%s %s %s\n", T_GET_INTF, g_job_id, g_task_id);

    // Shared_file* sf;
    if (mp_client_send_request(g_parent_conn, msg, test_got_intermediate_file, NULL) < 0) {
        err_exit("[(%d)(%s)mapreduce.c: test_request_intermediate_file()] mp_client_send_request() error", getpid(), g_task_id);
    }
}

static void test_copy_phase() {
    test_request_intermediate_file();
    // waiting copy intermediate files done
    pthread_mutex_lock(&g_rctx.lock);
    while (!g_rctx.copy_intermediate_file_done) {
        pthread_cond_wait(&g_rctx.cv, &g_rctx.lock);
    }
    // g_rctx.copy_intermediate_file_done = false;
    pthread_mutex_unlock(&g_rctx.lock);
}

static int KV_cmp(const void* kv_a, const void* kv_b) {
    return strcmp(((KV*)kv_a)->key, ((KV*)kv_b)->key);
}

static void reducer_ctx_open_outputfile() {
    printf("[[mapred(%d)(tid: %s)]] creating outputfile(%s)\n", getpid(), 
        g_task_id, g_rctx.output_file_path);
    g_rctx.outputfile = fopen(g_rctx.output_file_path, "w");
    assert(g_rctx.outputfile != NULL);
}

static KV* reducer_ctx_KV_iterator_init() {
    g_rctx.KV_iterator.kv_idx = 0;
    return &g_rctx.copied_mapoutput.kvs[g_rctx.KV_iterator.kv_idx];
    // const char* cur_key = (p->kvs[kv_idx]).key;
}

static const char* reducer_ctx_KV_iterator_get_next_val(const char* key) {
    // KV* kv = &partitions[partition_number].kvs[kv_idx];
    // KV_arr* p = &partitions[partition_number];
    KV_arr* kva = &g_rctx.copied_mapoutput;
    int kvi = g_rctx.KV_iterator.kv_idx;
    if (kvi < kva->n_kvs && 
        !strcmp(key, (kva->kvs[kvi]).key)) {
        // kv_idx++;
        g_rctx.KV_iterator.kv_idx++;
        return (kva->kvs[kvi]).value;
    }

    return NULL;
}

static const char* reducer_ctx_KV_iterator_next_key(const char* cur_key) {
    // KV* kv = &p->kvs[kv_idx];
    KV_arr* kva = &g_rctx.copied_mapoutput;
    while (g_rctx.KV_iterator.kv_idx < kva->n_kvs && 
                        !strcmp(cur_key, (kva->kvs[g_rctx.KV_iterator.kv_idx]).key)) {
        g_rctx.KV_iterator.kv_idx++;
    }

    return g_rctx.KV_iterator.kv_idx == kva->n_kvs ? 
                        NULL : (kva->kvs[g_rctx.KV_iterator.kv_idx]).key;
}


void MR_emit_result(const char* key, const char* result) {
    fprintf(g_rctx.outputfile, "%s\t%s\n", key, result);
}

static void reducer_ctx_close_outputfile() {
    fclose(g_rctx.outputfile);
}

static void run_reduce_task(Reducer reduce) {
    // notify parent task launched
    char msg[MP_MAXMSGLEN];
    // T_RT_LNCHD <job_id> <task_id>
    sprintf(msg, "%s %s %s\n", T_RT_LNCHD, g_job_id, g_task_id);
    if (mp_client_send_request(g_parent_conn, msg, NULL, NULL) < 0) {
        err_exit("[(%d)(%s)mapreduce.c: run_reduce_task()] mp_client_send_request() error", getpid(), g_task_id);
    }

    printf("[[mapred(%d)(tid: %s)]] Running reduce task with outputpath: %s\n", 
        getpid(), g_task_id, g_rctx.output_file_path);

    // copy phase
    if (copy_phase() < 0) {
        task_done(true);
    }
    // sort phase
    qsort(g_rctx.copied_mapoutput.kvs, g_rctx.copied_mapoutput.n_kvs, sizeof(KV), KV_cmp);
    
    // reduce phase
    // printf("[[mapred(%d)(tid: %s)]] open outputfile\n", getpid(), g_task_id);
    reducer_ctx_open_outputfile();

    const char* cur_key = reducer_ctx_KV_iterator_init()->key;
    while (cur_key != NULL) {
        reduce(cur_key, reducer_ctx_KV_iterator_get_next_val);
        cur_key = reducer_ctx_KV_iterator_next_key(cur_key);
    }

    reducer_ctx_close_outputfile();
    task_done(false);
}

static void test_run_reduce_task(Reducer reduce) {
    // notify parent task launched
    char msg[MP_MAXMSGLEN];
    // T_RT_LNCHD <job_id> <task_id>
    sprintf(msg, "%s %s %s\n", T_RT_LNCHD, g_job_id, g_task_id);
    if (mp_client_send_request(g_parent_conn, msg, NULL, NULL) < 0) {
        err_exit("[(%d)(%s)mapreduce.c: run_reduce_task()] mp_client_send_request() error", getpid(), g_task_id);
    }

    printf("[[mapred(%d)(tid: %s)]] Running test reduce task with outputpath: %s\n", 
        getpid(), g_task_id, g_rctx.output_file_path);

    // copy phase
    test_copy_phase();
    // sort phase
    // qsort(g_rctx.copied_mapoutput.kvs, g_rctx.copied_mapoutput.n_kvs, sizeof(KV), KV_cmp);
    
    // reduce phase
    printf("[[mapred(%d)(tid: %s)]] open outputfile\n", getpid(), g_task_id);
    reducer_ctx_open_outputfile();
    printf("[[mapred(%d)(tid: %s)]] Doing dummy reduce task\n", getpid(), g_task_id);

    reducer_ctx_close_outputfile();
    task_done(false);
}

void test_MR_run_task(int argc, char* argv[], Mapper map, Reducer reduce) {
    // if (!(argc == 8 || argc == 7)) {
    if (argc != 8) {
        err_exit("[(%d)mapreduce.c: MR_run_task()] "
                "usage[maptask]: %s <parent sockfd> <task type> <job_id> <task id>  " 
                "<inputsplit path> <num_partitions> <shared_dev_file>\n"
                "usage[reducetask]: %s <parent sockfd> <task type> <job_id> <task id>  "
                "<partition number str> <output path> <shared_file_size_str>", getpid(), argv[0], argv[0]);
    }

    int parent_sockfd;
    sscanf(argv[1], "%d", &parent_sockfd);
    // const char* task_type = argv[2];
    g_task_type = argv[2];
    g_job_id = argv[3];
    g_task_id = argv[4];

    g_engine = ep_engine_create(false, 0);
    // ep_engine_start_event_loop(engine);
    g_parent_conn = mp_client_create_from_fd(parent_sockfd, g_engine);
    if (g_parent_conn == NULL) {
        err_exit("[(%d)(%s)mapreduce.c: MR_run_task()] mp_client_create_from_fd() error", getpid(), g_task_id);
    }

    int err = pthread_create(&g_ev_loop_thr, NULL, ep_engine_event_loop_thread, NULL);
    if (err) {
        err_exit_errn(err, "[(%d)(%s)mapreduce.c: MR_run_task()] pthread_create(ep_engine_event_loop_thread) error", getpid(), g_task_id);
    }


    if (strcmp(g_task_type, "m") == 0) {
        // Map task <parent sockfd> <task type> <job_id> <task id> 
        //                  <inputsplit path> <num_partition_str> <shared_dev_file>
        int num_partitions;
        const char* inputsplit_path = argv[5];
        sscanf(argv[6], "%d", &num_partitions);
        const char* shared_dev_file = argv[7];
        // g_shared_dev_file = argv[7];

        mapper_ctx_init(inputsplit_path, num_partitions, shared_dev_file);
        // run_map_task(map);
        test_run_map_task(map);
    } else if (strcmp(g_task_type, "r") == 0) {
        // Reduce task <parent sockfd> <task type> <job id> <task id>
        //              <partition number str> <output path> <shared_file_size_str>

        int partition_number, shared_file_size;
        sscanf(argv[5], "%d", &partition_number);
        const char* output_path = argv[6];
        sscanf(argv[7], "%d", &shared_file_size);
        reducer_ctx_init(partition_number, output_path, shared_file_size);
        // run_reduce_task(reduce);
        test_run_reduce_task(reduce);
    } else {
        err_exit("[(%d)(%s)mapreduce.c: MR_run_task()] Unknown task type", getpid(), g_task_id);
    }

    // pthread_join(ev_loop_thr, NULL);
}

void MR_run_task(int argc, char* argv[], Mapper map, Reducer reduce) {
    // if (!(argc == 8 || argc == 7)) {
    if (argc != 8) {
        err_exit("[(%d)mapreduce.c: MR_run_task()] "
                "usage[maptask]: %s <parent sockfd> <task type> <job_id> <task id>  " 
                "<inputsplit path> <num_partitions> <shared_dev_file>\n"
                "usage[reducetask]: %s <parent sockfd> <task type> <job_id> <task id>  "
                "<partition number str> <output path> <shared_file_size_str>", getpid(), argv[0], argv[0]);
    }

    int parent_sockfd;
    sscanf(argv[1], "%d", &parent_sockfd);
    // const char* task_type = argv[2];
    g_task_type = argv[2];
    g_job_id = argv[3];
    g_task_id = argv[4];

    g_engine = ep_engine_create(false, 0);
    // ep_engine_start_event_loop(engine);
    g_parent_conn = mp_client_create_from_fd(parent_sockfd, g_engine);
    if (g_parent_conn == NULL) {
        err_exit("[(%d)(%s)mapreduce.c: MR_run_task()] mp_client_create_from_fd() error", getpid(), g_task_id);
    }

    int err = pthread_create(&g_ev_loop_thr, NULL, ep_engine_event_loop_thread, NULL);
    if (err) {
        err_exit_errn(err, "[(%d)(%s)mapreduce.c: MR_run_task()] pthread_create(ep_engine_event_loop_thread) error", getpid(), g_task_id);
    }


    if (strcmp(g_task_type, "m") == 0) {
        // Map task <parent sockfd> <task type> <job_id> <task id> 
        //                  <inputsplit path> <num_partition_str> <shared_dev_file>
        int num_partitions;
        const char* inputsplit_path = argv[5];
        sscanf(argv[6], "%d", &num_partitions);
        const char* shared_dev_file = argv[7];
        // g_shared_dev_file = argv[7];

        mapper_ctx_init(inputsplit_path, num_partitions, shared_dev_file);
        run_map_task(map);
        // test_run_map_task(map);
    } else if (strcmp(g_task_type, "r") == 0) {
        // Reduce task <parent sockfd> <task type> <job id> <task id>
        //              <partition number str> <output path> <shared_file_size_str>

        int partition_number, shared_file_size;
        sscanf(argv[5], "%d", &partition_number);
        const char* output_path = argv[6];
        sscanf(argv[7], "%d", &shared_file_size);
        reducer_ctx_init(partition_number, output_path, shared_file_size);
        run_reduce_task(reduce);
        // test_run_reduce_task(reduce);
    } else {
        err_exit("[(%d)(%s)mapreduce.c: MR_run_task()] Unknown task type", getpid(), g_task_id);
    }
}
