#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "da_mr/mapreduce.h"

void map(const char* input_split_file) {
    FILE *fp = fopen(input_split_file, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char* token = strtok(line, " \t\n\r");
        while (token != NULL) {
            MR_emit_kv(token, "1");
            token = strtok(NULL, " \t\n\r");
        }
        // char *token, *dummy = line;
        // while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
        //     MR_emit_kv(token, "1");
        // }
    }
    free(line);
    fclose(fp);
}

void reduce(const char* key, ValueIterator get_next_val) {
    int count = 0;
    const char *value;
    while ((value = get_next_val(key)) != NULL)
        count++;
    // char result[4096];
    // sprintf(result, "%d, keylen: %ld, partition: %d", count, strlen(key), partition_number);
    char result[11];
    sprintf(result, "%d", count);
    MR_emit_result(key, result);
}

int main(int argc, char* argv[]) {
    MR_run_task(argc, argv, map, reduce);
    // MR_run_local_test(argc, argv, map, reduce);
}