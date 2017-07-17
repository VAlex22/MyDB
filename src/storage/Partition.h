#ifndef MYDB_PARTITION_H
#define MYDB_PARTITION_H
#include "../global.h"

struct Text
{
    char x[TEXT_SIZE];
    bool operator== (const Text& a) const;

    template <typename Archive>
    void serialize(Archive& ar, const unsigned int version);
};


template <typename t, size_t s>
class Partition {
private:
    unsigned size; // size of values in partition
    unsigned used; // used size
    unsigned rowSize; // size of one row
    unordered_map<string, unsigned> fieldIndexes;
    unordered_map<string, array<t,s>> rowsByKey;

public:
    Partition(string file);
    Partition(unsigned size);
    Partition(unsigned size, unordered_map<string, unsigned> fieldIndexes);
    bool insert(string key, array<t,s> ar);
    unordered_map<string, t> read(string key, vector<string> fields); // read only specified fields
    array<t,s> read(string key); // read whole row
    bool update(string key, unordered_map<string,t> newData);
    bool remove(string key);
    bool serialize(string file);
};

#endif //MYDB_PARTITION_H
