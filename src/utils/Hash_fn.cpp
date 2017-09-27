#include "Hash_fn.h"

size_t Hash_fn::get_partition(string key)
{
    return hash_fn(key) % PARTITIONS;
}

hash<string> Hash_fn::hash_fn;