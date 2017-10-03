#ifndef MYDB_PARTITION_H
#define MYDB_PARTITION_H
#include "../global.h"
#include "../utils/TimestampGenerator.h"
/*
 * Transaction support implemented not for all functions!!!
 */

struct Text
{
    char x[TEXT_SIZE+1];
    bool operator== (const Text& a) const;

    template <typename Archive>
    void serialize(Archive& ar, const unsigned int version);
};

template <typename t, size_t s>
struct Row {
    Row(array<t, s> fields, unsigned timestamp);
    array<t, s> fields;
    unsigned timestamp;
    bool lock;
};

template <typename t, size_t s>
struct Tup {
    Tup(array<t, s> fields, unsigned timestamp, string key);
    ~Tup(){};
    array<t, s> fields;
    unsigned timestamp;
    string key;
};

template <typename t, size_t s>
class Partition {

private:
    unordered_map <unsigned, bool> autoCommitBySession;
    unsigned size; // size of values in partition
    unsigned used; // used size
    unsigned rowSize; // size of one row
    unordered_map<string, Row<t, s>> rowsByKey;
    unordered_map<unsigned, map<string, Tup<t, s>>> transactionSets;

public:
    unordered_map<string, unsigned> fieldIndexes;
    unordered_map<unsigned, string> indexFields;
    Partition(string file);
    Partition(unsigned size);
    Partition(unsigned size, unordered_map<string, unsigned> fieldIndexes);
    bool insert(string key, array<t,s> ar);
    unordered_map<string, t> read(string key, vector<string> fields); // read only specified fields

    bool remove(string key);
    bool serialize(string file);
    /*
    * Functions with transaction support!!!
    */
    array<t,s> read(string key, unsigned session); // read whole row
    bool update(string key, unordered_map<string,t> newData, unsigned session);
    void startTransaction(unsigned session);
    unsigned validateTransaction(unsigned session);
    bool writeTransaction(unsigned session, unsigned commitTs);
    void abort(unsigned session);
};

#endif //MYDB_PARTITION_H
