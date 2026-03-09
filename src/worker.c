
#define _GNU_SOURCE

#include <stdio.h>
#include <pthread.h>
#include <sys/queue.h>
#include <stdlib.h>
#include <linux/limits.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <assert.h>
#include <signal.h>
#include <sys/stat.h>
#include <errno.h>

#include "utils/my_err.h"
#include "ep_engine/epoll_event_engine.h"
#include "msg_pass/mp_server.h"
#include "msg_pass/mp_client.h"
#include "shared_file_struct.h"
#include "MR_msg_type.h"
#include "task.h"
#include "utils_ds/hash_table.h"
#include "intermediate_file_sturct.h"

#define N_ENGINE_THREADS 4
#define WORKER_PORT 30031
#define LINE_MAX 512
#define LOCAL_HOST "127.0.0.1"

#define MR_EXEC_DIR "mapred_bin"
#define SHARED_FILE_LIST "sharedfiles"
#define SHARED_DIR "mnt"
#define MR_OUTPUT_DIR "data/outputs"

struct Worker_context {
    const char* name;
    Hash_table* name_sf_map;

    EP_engine* engine;
    pthread_t ev_loop_thr;

    Mp_server* job_msg_listener;
    Mp_server* task_msg_listener;
    Task_ev_handler* task_event_handler;
    // char shared_dir[PATH_MAX];
    const char* worker_ip;

    // const char* mr_exe_dir;
    // const char* shared_file_list;
    // const char* shared_dir;
    const char* shared_dev_file;
    char mr_exe_dir[PATH_MAX];
    char shared_file_list[PATH_MAX];
    char shared_dir[PATH_MAX];
    char worker_output_dir[PATH_MAX];

    Mp_client* master_conn;
    /*
        These objects should be created per job.
        But for now, we use a single job
    */
    char job_name[NAME_MAX];
    char job_id[NAME_MAX];
    char job_output_dir[PATH_MAX];
    Task_list map_tasks;
    Task_list reduce_tasks;
    Hash_table* tid_task_map;
    Hash_table* pid_task_map;

    Shared_file_list shf_q;
    int shared_file_size;

    pthread_mutex_t lock;
};

static Worker_context g_wctx;


static void handle_job_msg(Mp_srv_request* request, void* cb_arg);
static void handle_task_msg(Mp_srv_request* request, void* cb_arg);

// void shared_file_q_init(Shared_file_list* shf_q, Shared_file_list* shf_inuse) {
static void shared_file_q_init() {
    // static int id = 0;
    printf("Initiating sharedfile_Q...\n");
    // init shf_q and shf_inuse
    TAILQ_INIT(&g_wctx.shf_q);
    // TAILQ_INIT(&shf_inuse);

    char line[LINE_MAX];
    FILE* f = fopen(g_wctx.shared_file_list, "r");
    if (f == NULL) {
        err_exit_sys("[worker.c: shared_file_q_init()] fopen() error");
    }

    // char fname[NAME_MAX];
    while (fgets(line, sizeof(line), f) != NULL) {
        Shared_file* sf = (Shared_file*)malloc(sizeof(*sf));
        if (sf == NULL) {
            fclose(f);
            err_exit_sys("[worker.c: shared_file_q_init()] malloc() error");
        }
        sscanf(line, "%[^:]:%d", sf->name, &sf->capacity);
        sf->metadata_size = 0;

        if (g_wctx.shared_file_size <= 0) {
            g_wctx.shared_file_size = sf->capacity;
        }

        TAILQ_INSERT_TAIL(&g_wctx.shf_q, sf, sf_link);
        // sf_id_map[sf->id] = sf;
        if (!hash_table_put_locked(g_wctx.name_sf_map, sf->name, sf)) {
            err_exit("[worker.c: shared_file_q_init()] hash_table_put_locked() error");
        }
    }
    if (ferror(f)) {
        fclose(f);
        err_exit_sys("[worker.c: shared_file_q_init()] fgets() error");
    }

    fclose(f);
}

static void worker_add_task(Worker_context* wctx, Task* task) {
    if (task->type == MAP_TASK) {
        TAILQ_INSERT_TAIL(&wctx->map_tasks, task, task_link);
    } else {
        TAILQ_INSERT_TAIL(&wctx->reduce_tasks, task, task_link);
    }

    if (!hash_table_put_locked(wctx->tid_task_map, task->tid, task)) {
        err_exit("[worker.c: worker_add_task()] hash_table_put_locked() error");
    }
}

static void worker_delete_task(Worker_context* wctx, Task* task) {
    if (task->pid != -1) {
        char pid_str[11];
        sprintf(pid_str, "%d", task->pid);

        if(!hash_table_delete_locked(wctx->pid_task_map, pid_str)) {
            err_exit("[worker.c: worker_delete_task()] hash_table_delete_locked(pid) error");
        }
    }

    if(!hash_table_delete_locked(wctx->tid_task_map, task->tid)) {
        err_exit("[worker.c: worker_delete_task()] hash_table_delete_locked(tid) error");
    }

    if (task->type == MAP_TASK) {
        TAILQ_REMOVE(&wctx->map_tasks, task, task_link);
        map_task_destroy((Map_task*)task);
    } else {
        TAILQ_REMOVE(&wctx->reduce_tasks, task, task_link);
        reduce_task_destroy((Reduce_task*)task);
    }

}

void worker_add_launched_task(Worker_context* wctx, pid_t pid, Task* task) {
    char pid_str[11];
    sprintf(pid_str, "%d", pid);

    if (!hash_table_put_locked(wctx->pid_task_map, pid_str, task)) {
        err_exit("[worker.c: worker_add_launched_task()] hash_table_put_locked() error");
    }
}

void worker_add_task_conn(Worker_context* wctx, int task_sockfd) {
    mp_server_add_conn_from_fd(wctx->task_msg_listener, task_sockfd, LOCAL_HOST);
}

const char* worker_get_name(Worker_context* wctx) {
    return wctx->name;
}

const char* worker_get_mr_exe_dir(Worker_context* wctx) {
    return wctx->mr_exe_dir;
}

const char* worker_get_shared_dir(Worker_context* wctx) {
    return wctx->shared_dir;
}

const char* worker_get_ip(Worker_context* wctx) {
    return wctx->worker_ip;
}

const char* worker_get_shared_dev_file(Worker_context* wctx) {
    return wctx->shared_dev_file;
}

int worker_get_shared_file_size(Worker_context* wctx) {
    return wctx->shared_file_size;
}

Shared_file* worker_get_shared_file(Worker_context* wctx) {
    Shared_file* sf;
    pthread_mutex_lock(&wctx->lock);
    sf = TAILQ_FIRST(&wctx->shf_q);
    TAILQ_REMOVE(&wctx->shf_q, sf, sf_link);
    sf->metadata_size = 0;
    pthread_mutex_unlock(&wctx->lock);

    return sf;
}

Mp_client* worker_get_master_conn(Worker_context* wctx) {
    return wctx->master_conn;
}

void worker_release_shared_file(Worker_context* wctx, Shared_file* sf) {
    pthread_mutex_lock(&wctx->lock);
    TAILQ_INSERT_TAIL(&wctx->shf_q, sf, sf_link);
    pthread_mutex_unlock(&wctx->lock);
}

static void handle_init_conn(const char* msg_ptr, Mp_srv_request* request) {
    int port;
    sscanf(msg_ptr, " %d\n", &port);
    // connect to master
    const char* master_ip = mp_server_get_client_ip(request->client);
    g_wctx.master_conn = mp_client_create(master_ip, port, g_wctx.engine);
    if (g_wctx.master_conn == NULL) {
        err_exit("[worker.c: handle_init_conn()] mp_client_create(%s) error", master_ip);
    }

    char reply[MP_MAXMSGLEN];
    sprintf(reply, "%s\n", CONN_INITED);
    mp_server_request_done(request, reply);
}

static int mkdir_r(const char* dir) {
    char tmpdir[PATH_MAX];
    char *p = NULL;
    size_t len;

    sprintf(tmpdir, "%s", dir);
    len = strlen(tmpdir);
    if (tmpdir[len - 1] == '/')
        tmpdir[len - 1] = 0;
    for (p = tmpdir + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            if (mkdir(tmpdir, 0755) < 0 && errno != EEXIST) {
                err_msg_sys("[worker.c: mkdir_r()] mkdir(%s) error", tmpdir);
                return -1;
            }
            *p = '/';
        }
    }
    if (mkdir(tmpdir, 0755) < 0 && errno != EEXIST) {
        err_msg_sys("[worker.c: mkdir_r()] mkdir(%s) error", tmpdir);
        return -1;
    }

    return 0;
}

static void handle_job_init(const char* msg_ptr, Mp_srv_request* request) {
    //J_INIT <job_name> <job_id> <job_output_dir>
    char dir[PATH_MAX];
    sscanf(msg_ptr, " %s %s %s\n", g_wctx.job_name, g_wctx.job_id, dir);

    // make job output dir under the output_dir
    sprintf(g_wctx.job_output_dir, "%s/%s", g_wctx.worker_output_dir, dir);
    printf("[[worker]] making job output dir(%s)\n", g_wctx.job_output_dir);

    if (mkdir_r(g_wctx.job_output_dir) < 0) {
        err_exit_sys("[worker.c: handle_job_init()] mkdir_r(%s) error", g_wctx.job_output_dir);
    }

    char reply[MP_MAXMSGLEN];
    sprintf(reply, "%s\n", J_INITED);
    mp_server_request_done(request, reply);
}

static void handle_launch_mt(const char* msg_ptr, Mp_srv_request* request) {
    char job_id[NAME_MAX], task_id[NAME_MAX], inputsplit_path[PATH_MAX];
    int num_partitions;
    //J_LNCH_MT <job_id> <task_id> <inputsplit path> <num_partitons>
    sscanf(msg_ptr, " %s %s %s %d\n", job_id, task_id, 
                                            inputsplit_path, &num_partitions);


    // Create map task
    Map_task* mt = map_task_create(&g_wctx, task_id, job_id, g_wctx.job_name, inputsplit_path, num_partitions);
    if (mt == NULL) {
        err_exit("[worker.c: handle_launch_mt()] map_task_create() error");
    }
    worker_add_task(&g_wctx, (Task*)mt);
    // mt->t.launch_req = request;


    Task_launch_event* tle = task_launch_event_create((Task*)mt, request);
    if (tle == NULL) {
        err_exit("[worker.c: handle_launch_mt()] task_launch_event_create() error");
    }
    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tle);
}

static void handle_launch_rt(const char* msg_ptr, Mp_srv_request* request) {
    char job_id[NAME_MAX], task_id[NAME_MAX], 
        outputfile_name[NAME_MAX], outputfile_path[PATH_MAX];
    int partition_number;
    //J_LNCH_RT <job_id> <task_id> <outputfile name> <partition number>
    sscanf(msg_ptr, " %s %s %s %d\n", job_id, task_id, 
                                            outputfile_name, &partition_number);


    snprintf(outputfile_path, sizeof(outputfile_path), "%s/%s", g_wctx.job_output_dir, outputfile_name);
    // Create reduce task
    Reduce_task* rt = reduce_task_create(&g_wctx, task_id, job_id, g_wctx.job_name, 
        outputfile_path, partition_number);
    if (rt == NULL) {
        err_exit("[worker.c: handle_launch_rt()] reduce_task_create() error");
    }
    worker_add_task(&g_wctx, (Task*)rt);
    // rt->t.launch_req = request;


    Task_launch_event* tle = task_launch_event_create((Task*)rt, request);
    if (tle == NULL) {
        err_exit("[worker.c: handle_launch_rt()] task_launch_event_create() error");
    }
    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tle);
}

static void handle_fwd_interm_file_location(const char* msg_ptr, Mp_srv_request* request) {
    char job_id[NAME_MAX], task_id[NAME_MAX];
    Intermediate_file* intf = (Intermediate_file*)malloc(sizeof(*intf));
    if (intf == NULL) {
        err_exit("[worker.c: handle_fwd_interm_file_location()] malloc() error");
    }
    // J_FWD_INTF_LOC <job_id> <task_id> 
    // <intermediate file name> <intermediate metadata size> <intermediate file location>
    sscanf(msg_ptr, " %s %s %s %d %s\n", job_id, task_id, 
            intf->file_name, &intf->metadata_size, intf->worker_name);

    Task* t = hash_table_get_locked(g_wctx.tid_task_map, task_id);
    assert(t != NULL);
    assert(t->type == REDUCE_TASK);

    Task_fwd_intf_event* tfie = task_fwd_intf_event_create(t, intf);
    if (tfie == NULL) {
        err_exit("[worker.c: handle_fwd_interm_file_location()] task_fwd_intf_event_create() error");
    }

    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tfie);

    mp_server_request_done(request, NULL);
}

static void handle_start_reduce_phase(const char* msg_ptr, Mp_srv_request* request) {
    char job_id[NAME_MAX], tid[NAME_MAX];
    sscanf(msg_ptr, " %s %s\n", job_id, tid);

    Task* t = hash_table_get_locked(g_wctx.tid_task_map, tid);
    assert(t != NULL);
    assert(t->type == REDUCE_TASK);

    Task_start_reduce_phase_event* tsrpe = task_start_reduce_phase_event_create(t);
    if (tsrpe == NULL) {
        err_exit("[worker.c: handle_start_reduce_phase()] task_start_reduce_phase_event_create() error");
    }

    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tsrpe);

    mp_server_request_done(request, NULL);
}

static void handle_job_abort(const char* msg_ptr, Mp_srv_request* request) {
    // J_ABORT <job_id> <task_id>
    char job_id[NAME_MAX], task_id[NAME_MAX];
    sscanf(msg_ptr, " %s %s\n", job_id, task_id);
    Task* t = hash_table_get_locked(g_wctx.tid_task_map, task_id);
    assert(t != NULL);

    Task_abort_event* tae = task_abort_event_create(t);
    if (tae == NULL) {
        err_exit("[worker.c: handle_job_abort()] task_abort_event_create() error");
    }
    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tae);

    mp_server_request_done(request, NULL);
}

static void handle_task_cleanup(const char* msg_ptr, Mp_srv_request* request) {
    // J_TASK_CLEANUP <job id> <task id>
    char job_id[NAME_MAX], task_id[NAME_MAX];
    sscanf(msg_ptr, " %s %s\n", job_id, task_id);
    Task* t = hash_table_get_locked(g_wctx.tid_task_map, task_id);
    assert(t != NULL);

    char reply[MP_MAXMSGLEN];
    sprintf(reply, "%s %s %s\n", J_TASK_CLEANED_UP, t->job_id, t->tid);

    worker_delete_task(&g_wctx, t);

    mp_server_request_done(request, reply);

    if (TAILQ_EMPTY(&g_wctx.map_tasks) && TAILQ_EMPTY(&g_wctx.reduce_tasks)) {
        printf("[[worker]] Destroying master conn(client)...\n");
        mp_client_destroy(g_wctx.master_conn, NULL, NULL);
        g_wctx.master_conn = NULL;
    }
}

static void handle_job_msg(Mp_srv_request* request, void* cb_arg) {
    const char* msg = request->msg;
    if (strncmp(msg, INIT_CONN, strlen(INIT_CONN)) == 0) {
        printf("[[worker(j_msg)]] handling init conn: %s", msg);
        handle_init_conn(msg + strlen(INIT_CONN), request);

    } else if (strncmp(msg, J_INIT, strlen(J_INIT)) == 0) {
        printf("[[worker(j_msg)]] handling job init: %s", msg);
        handle_job_init(msg + strlen(J_INIT), request);

    } else if (strncmp(msg, J_LNCH_MT, strlen(J_LNCH_MT)) == 0) {
        printf("[[worker(j_msg)]] handling launch mt: %s", msg);
        handle_launch_mt(msg+strlen(J_LNCH_MT), request);

    } else if (strncmp(msg, J_LNCH_RT, strlen(J_LNCH_RT)) == 0) {
        printf("[[worker(j_msg)]] handling launch rt: %s", msg);
        handle_launch_rt(msg+strlen(J_LNCH_RT), request);

    } else if (strncmp(msg, J_FWD_INTF_LOC, strlen(J_FWD_INTF_LOC)) == 0) {
        printf("[[worker(j_msg)]] handling forward interm file loc: %s", msg);
        handle_fwd_interm_file_location(msg+strlen(J_FWD_INTF_LOC), request);

    } else if (strncmp(msg, J_START_REDUCE_PHASE, strlen(J_START_REDUCE_PHASE)) == 0) {
        printf("[[worker(j_msg)]] handling start reduce phase: %s", msg);
        handle_start_reduce_phase(msg+strlen(J_START_REDUCE_PHASE), request);

    } else if (strncmp(msg, J_ABORT, strlen(J_ABORT)) == 0) {
        printf("[[worker(j_msg)]] handling job abort: %s", msg);
        handle_job_abort(msg+strlen(J_ABORT), request);

    } else if (strncmp(msg, J_TASK_CLEANUP, strlen(J_TASK_CLEANUP)) == 0) {
        printf("[[worker(j_msg)]] handling cleanup: %s", msg);
        handle_task_cleanup(msg+strlen(J_TASK_CLEANUP), request);

    } else {
        err_exit("[worker.c: handle_job_msg()] Unknown job msg type (%s).", msg);
    }
}

static void handle_mt_launched(const char* msg_ptr, Mp_srv_request* request) {
    // T_MT_LNCHD <job_id> <task_id>
    char job_id[NAME_MAX], task_id[NAME_MAX];
    sscanf(msg_ptr, " %s %s\n", job_id, task_id);

    Task* t = hash_table_get_locked(g_wctx.tid_task_map, task_id);
    assert(t != NULL);
    assert(t->type == MAP_TASK);

    Task_launched_event* tlde = task_launched_event_create(t);
    if (tlde == NULL) {
        err_exit("[worker.c: handle_mt_launched()] task_launched_event_create() error");
    }
    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tlde);

    mp_server_request_done(request, NULL);
}

static void handle_rt_launched(const char* msg_ptr, Mp_srv_request* request) {
    // T_RT_LNCHD <job_id> <task_id>
    char job_id[NAME_MAX], task_id[NAME_MAX];
    sscanf(msg_ptr, " %s %s\n", job_id, task_id);

    Task* t = hash_table_get_locked(g_wctx.tid_task_map, task_id);
    assert(t != NULL);
    assert(t->type == REDUCE_TASK);

    Task_launched_event* tlde = task_launched_event_create(t);
    if (tlde == NULL) {
        err_exit("[worker.c: handle_rt_launched()] task_launched_event_create() error");
    }
    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tlde);

    mp_server_request_done(request, NULL);
}

static void handle_get_shared_file(const char* msg_ptr, Mp_srv_request* request) {
    // T_GET_SHF <job_id> <task_id>
    char job_id[NAME_MAX], task_id[NAME_MAX];
    sscanf(msg_ptr, " %s %s\n", job_id, task_id);

    Task* t = hash_table_get_locked(g_wctx.tid_task_map, task_id);
    assert(t != NULL);
    assert(t->type == MAP_TASK);

    Task_get_shf_event* tgse = task_get_shf_event_create(t, request);
    if (tgse == NULL) {
        err_exit("[worker.c: handle_get_shared_file()] task_get_shf_event_create() error");
    }
    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tgse);
}

static void handle_mapoutput(const char* msg_ptr, Mp_srv_request* request) {
    // T_MAPOUTPUT <job_id> <tid> <sharedfile path> <metadata size> <has data>
    char job_id[NAME_MAX], tid[NAME_MAX], shf_path[PATH_MAX];
    int metadata_size, has_data;
    sscanf(msg_ptr, " %s %s %s %d %d\n", job_id, tid, shf_path, &metadata_size, &has_data);

    Task* t = hash_table_get_locked(g_wctx.tid_task_map, tid);
    assert(t != NULL);
    assert(t->type == MAP_TASK);

    Shared_file* sf = hash_table_get_locked(g_wctx.name_sf_map, shf_path);
    assert(sf != NULL);
    sf->metadata_size = metadata_size;

    Task_mapoutput_event* tme = task_mapoutput_event_create(t, sf, has_data);
    if (tme == NULL) {
        err_exit("[worker.c: handle_mapoutput()] task_mapoutput_event_create() error");
    }
    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tme);

    mp_server_request_done(request, NULL);
}

static void handle_task_done(const char* msg_ptr, Mp_srv_request* request) {
    // T_DONE <job_id> <task_id> <task_failed>
    char job_id[NAME_MAX], tid[NAME_MAX];
    int t_failed;
    sscanf(msg_ptr, " %s %s %d\n", job_id, tid, &t_failed);
    Task* t = hash_table_get_locked(g_wctx.tid_task_map, tid);
    assert(t != NULL);
    // assert(t->type == MAP_TASK);
    // Assigned_map_task* amt = (Assigned_map_task*)t;

    Task_event* tev;
    if (t_failed) {
        tev = (Task_event*)task_failed_event_create(t);
        if (tev == NULL) {
            err_exit("[worker.c: handle_task_done()] task_failed_event_create() error");
        }
        // map_task_failed(amt);
    } else {
        tev = (Task_event*)task_succeeded_event_create(t);
        if (tev == NULL) {
            err_exit("[worker.c: handle_task_done()] task_succeeded_event_create() error");
        }
        // map_task_done(amt);
    }

    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tev);

    mp_server_request_done(request, NULL);
}

static void handle_get_intermediate_file(const char* msg_ptr, Mp_srv_request* request) {
    // T_GET_INTF <job_id> <task_id>
    char job_id[NAME_MAX], task_id[NAME_MAX];
    sscanf(msg_ptr, " %s %s\n", job_id, task_id);

    Task* t = hash_table_get_locked(g_wctx.tid_task_map, task_id);
    assert(t != NULL);
    assert(t->type == REDUCE_TASK);

    Task_get_intf_event* tgie = task_get_intf_event_create(t, request);
    if (tgie == NULL) {
        err_exit("[worker.c: handle_get_intermediate_file()] task_get_intf_event_create() error");
    }
    task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tgie);
}

static void handle_task_msg(Mp_srv_request* request, void* cb_arg) {
    const char* msg = request->msg;
    // char reply[MP_MAXMSGLEN];

    if (strncmp(msg, T_MT_LNCHD, strlen(T_MT_LNCHD)) == 0) {
        printf("[[worker(t_msg)]] handling mt launched: %s", msg);
        handle_mt_launched(msg+strlen(T_MT_LNCHD), request);

        // mp_server_request_done(request, NULL);
    } else if (strncmp(msg, T_RT_LNCHD, strlen(T_RT_LNCHD)) == 0) {
        printf("[[worker(t_msg)]] handling rt launched: %s", msg);
        handle_rt_launched(msg+strlen(T_RT_LNCHD), request);

    } else if (strncmp(msg, T_GET_SHF, strlen(T_GET_SHF)) == 0) {
        printf("[[worker(t_msg)]] handling get shared file: %s", msg);
        handle_get_shared_file(msg+strlen(T_GET_SHF), request);

    } else if (strncmp(msg, T_MAPOUTPUT, strlen(T_MAPOUTPUT)) == 0) {
        printf("[[worker(t_msg)]] handling mapoutput: %s", msg);
        handle_mapoutput(msg+strlen(T_MAPOUTPUT), request);

        // mp_server_request_done(request, NULL);
    } else if (strncmp(msg, T_DONE, strlen(T_DONE)) == 0) {
        printf("[[worker(t_msg)]] handling task done: %s", msg);
        handle_task_done(msg+strlen(T_DONE), request);

        // mp_server_request_done(request, NULL);
    } else if (strncmp(msg, T_GET_INTF, strlen(T_GET_INTF)) == 0) {
        printf("[[worker(t_msg)]] handling get interm file: %s", msg);
        handle_get_intermediate_file(msg+strlen(T_GET_INTF), request);
    } else {
        err_exit("[worker.c: handle_task_msg()] Unknown task msg type (%s).", msg);
    }
}

void* SIGCHLD_handler_thread(void* arg) {
    sigset_t ss;
    siginfo_t sinfo;

    sigemptyset(&ss);
    sigaddset(&ss, SIGCHLD);
    char pid_str[11];

    while (1) {
        sigwaitinfo(&ss, &sinfo);

        // abnormal exit
        if (sinfo.si_code != CLD_EXITED || sinfo.si_status != EXIT_SUCCESS) {
            sprintf(pid_str, "%d", sinfo.si_pid);

            printf("[[worker]] SIGCHLD (%s)\n", pid_str);

            Task* t = hash_table_get_locked(g_wctx.pid_task_map, pid_str);
            assert(t != NULL);
            Task_failed_event* tfe = task_failed_event_create(t);
            if (tfe == NULL) {
                err_exit("[worker.c: SIGCHLD_handler_thread()] task_failed_event_create() error");
            }
            task_ev_handler_handle(g_wctx.task_event_handler, (Task_event*)tfe);
        }
    }

    return NULL;
}

static void* ep_engine_event_loop_thread(void* arg) {
    EP_engine* engine = g_wctx.engine;
    int err = ep_engine_start_event_loop(engine);
    if (err < 0) {
        err_exit("[worker.c ep_engine_event_loop_thread()] ep_engine_start_event_loop() error");
    }

    return NULL;
}

void worker_context_init(const char* worker_name, const char* worker_ip, 
    // const char* mr_exe_dir, const char* shared_file_list, const char* shared_dir, 
                            const char* shared_dev_file/* , const char* output_dir */) {
    g_wctx.name = worker_name;

    g_wctx.name_sf_map = hash_table_create();
    if (g_wctx.name_sf_map == NULL) {
        err_exit("[worker.c: worker_context_init()] hash_table_create(name_sf_map) error");
    }

    g_wctx.engine = ep_engine_create(false, 0);
    if (g_wctx.engine == NULL) {
        err_exit("[worker.c: worker_context_init()] ep_engine_create() error");
    }

    int err = pthread_create(&g_wctx.ev_loop_thr, NULL, ep_engine_event_loop_thread, NULL);
    if (err) {
        err_exit_errn(err, "[worker.c: main()] pthread_create(ep_engine_event_loop_thread) error");
    }

    g_wctx.job_msg_listener = mp_server_create(WORKER_PORT, g_wctx.engine, 
                                                        handle_job_msg, NULL);
    if (g_wctx.job_msg_listener == NULL)
        err_exit("[worker.c: worker_context_init()] mp_server_create(job_msg_listener) error");

    g_wctx.task_msg_listener = mp_server_create(-1, g_wctx.engine, handle_task_msg, NULL);
    if (g_wctx.task_msg_listener == NULL)
        err_exit("[worker.c: worker_context_init()] mp_server_create(task_msg_listener) error");

    g_wctx.task_event_handler = task_ev_handler_create();
    if (g_wctx.task_event_handler == NULL)
        err_exit("[worker.c: worker_context_init()] task_ev_handler_create() error");

    g_wctx.worker_ip = worker_ip;
    // g_wctx.mr_exe_dir = mr_exe_dir;
    // g_wctx.shared_file_list = shared_file_list;
    // g_wctx.shared_dir = shared_dir;
    g_wctx.shared_dev_file = shared_dev_file;
    // g_wctx.output_dir = output_dir;

    // build worker's output dir
    const char* da_mr_home_env = getenv("DA_MR_HOME");
    if (da_mr_home_env == NULL) {
        err_exit("[worker.c: worker_context_init()] DA_MR_HOME env variable not found.");
    }
    sprintf(g_wctx.mr_exe_dir, "%s/%s", da_mr_home_env, MR_EXEC_DIR);
    sprintf(g_wctx.shared_file_list, "%s/%s", da_mr_home_env, SHARED_FILE_LIST);
    sprintf(g_wctx.shared_dir, "%s/%s", da_mr_home_env, SHARED_DIR);
    sprintf(g_wctx.worker_output_dir, "%s/%s", da_mr_home_env, MR_OUTPUT_DIR);

    g_wctx.master_conn = NULL;

    TAILQ_INIT(&g_wctx.map_tasks);
    TAILQ_INIT(&g_wctx.reduce_tasks);
    g_wctx.tid_task_map = hash_table_create();
    if (g_wctx.tid_task_map == NULL) {
        err_exit("[worker.c: worker_context_init()] hash_table_create(tid_task_map) error");
    }
    g_wctx.pid_task_map = hash_table_create();
    if (g_wctx.pid_task_map == NULL) {
        err_exit("[worker.c: worker_context_init()] hash_table_create(pid_task_map) error");
    }
    
    shared_file_q_init();

    pthread_mutex_init(&g_wctx.lock, NULL);
}

int main(int argc, char* argv[]) {
    // worker <worker name> <worker ip> <mr exe dir> 
    //                  <shared file list> <shared_dir> <shared_dev>
    // if (argc != 7) {
    //     err_exit("[worker.c: main()] "
    //                 "usage: %s <worker name> <worker ip> <mapreduce executable dir> " 
    //                 "<shared file list> <shared_dir> <shared_dev>", argv[0]);
    // }
    // worker <worker name> <worker ip> <shared_dev>
    if (argc != 4) {
        err_exit("[worker.c: main()] "
                    "usage: %s <worker name> <worker ip> <shared_dev>", argv[0]);
    }

    pthread_t sig_thr;
    sigset_t ss;
    sigemptyset(&ss);
    sigaddset(&ss, SIGCHLD);
    pthread_sigmask(SIG_BLOCK, &ss, NULL);

    int err = pthread_create(&sig_thr, NULL, SIGCHLD_handler_thread, NULL);
    if (err) {
        err_exit_errn(err, "[worker.c: main()] pthread_create(SIGCHLD_handler_thread) error");
    }

    const char* worker_name = argv[1];
    const char* worker_ip = argv[2];
    const char* shared_dev_file = argv[3];
    // const char* mr_exe_dir = argv[3];
    // const char* shared_file_list = argv[4];
    // const char* shared_dir = argv[5];
    // const char* shared_dev_file = argv[6];

    worker_context_init(worker_name, worker_ip, /* mr_exe_dir, shared_file_list,
                            shared_dir, */ shared_dev_file/* , output_dir */);

    pthread_join(g_wctx.ev_loop_thr, NULL);
}
