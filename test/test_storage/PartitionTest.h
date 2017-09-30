#ifndef MYDB_PARTITIONTEST_H
#define MYDB_PARTITIONTEST_H

#include "gtest/gtest.h"
#include "Partition.h"

#define NUMBER_OF_FIELDS2 1

class PartitionTest : public ::testing::Test {
public:
    unsigned NUMBER_OF_ROWS = 1000;

protected:

    // You can do set-up work for each test here.
    PartitionTest();

    Text generateText();

    long generateLong();

    string generateKey(unsigned i);

    unsigned partitionSize = 1024;

    Partition<Text, FIELDS> p1 = Partition<Text, FIELDS>(partitionSize);
    Partition<long, NUMBER_OF_FIELDS2> p2 = Partition<long, NUMBER_OF_FIELDS2>(partitionSize);
    unordered_map<string, array<Text, FIELDS>> data1;
    unordered_map<string, array<long, 1>> data2;
};



#endif //MYDB_PARTITIONTEST_H
