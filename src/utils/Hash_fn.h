#ifndef MYDB_HASH_FN_H
#define MYDB_HASH_FN_H
#include "../global.h"

class Hash_fn
{
    static hash<string> hash_fn;
public:
    static size_t get_partition(string key);

};


#endif //MYDB_HASH_FN_H
