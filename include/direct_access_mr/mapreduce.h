#ifndef MAPREDUCE_H
#define MAPREDUCE_H

typedef void (*Mapper)(const char* input_split_file);

typedef const char* (*ValueIterator)(const char* key/* , int partition_number */);
typedef void (*Reducer)(const char* key, ValueIterator get_next_val/* , int partition_number */);

void MR_emit_kv(char* key, char* val);
void MR_emit_result(const char* key, const char* result);

// void test_MR_run_task(int argc, char* argv[], Mapper map, Reducer reduce);
void MR_run_task(int argc, char* argv[], Mapper map, Reducer reduce);

#endif
