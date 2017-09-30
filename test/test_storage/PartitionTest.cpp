#include "PartitionTest.h"

PartitionTest::PartitionTest()
{
    srand( (unsigned int) time(NULL));

    for (unsigned i = 0; i < NUMBER_OF_ROWS; i++)
    {
        array<Text, FIELDS> array1;
        for (int j = 0; j < FIELDS; j++)
        {
            array1[j] = generateText();
        }
        data1.insert({generateKey(i), array1});

        array<long, NUMBER_OF_FIELDS2> array2;
        for (int j = 0; j < NUMBER_OF_FIELDS2; j++)
        {
            array2[j] = generateLong();
        }
        data2.insert({generateKey(i), array2});
    }


    for (auto it : data2)
    {
        p2.insert(it.first, it.second);
    }

    for (auto it : data1)
    {
        p1.insert(it.first, it.second);
    }


}

Text PartitionTest::generateText() {
    Text result = Text();
    for (int i = 0; i < TEXT_SIZE; i++)
    {
        char c = (char) ('a' + rand() % 26);
        result.x[i] = c;
    }
    return  result;
}

long PartitionTest::generateLong() {
    return (long) rand()*(RAND_MAX) + rand();
}

string PartitionTest::generateKey(unsigned i) {
    return "account"+to_string(i);
}

TEST_F(PartitionTest, WriteReadTest)
{
    for (auto it : data1)
    {
        EXPECT_EQ(p1.read(it.first), data1[it.first]);
    }

    for (auto it : data2)
    {
        EXPECT_EQ(p2.read(it.first), data2[it.first]);
    }
}

TEST_F(PartitionTest, UpdateTest)
{
    for (unsigned i = 0; i < 100; i++)
    {
        string key = generateKey(rand() % NUMBER_OF_ROWS);

        int n = rand() % (FIELDS - 1) + 1;
        unordered_map<string, Text> newData;
        for (unsigned k = 0; k < n; k++)
        {
            int index = rand() % FIELDS;
            Text t = generateText();
            newData["f"+to_string(index)] = t;

            data1[key][index] = t;
        }
        p1.update(key, newData);
    }

    for (auto it : data1)
    {
        EXPECT_EQ(p1.read(it.first), data1[it.first]);
    }
}

TEST_F(PartitionTest, ReadSomeFieldsTest)
{
    for (unsigned i = 0; i < NUMBER_OF_ROWS; i++)
    {
        string key = generateKey(i);

        int n = rand() % (FIELDS - 1) + 1;
        vector<string> fields;
        unordered_map<string, Text> expected;
        for (unsigned k = 0; k < n; k++)
        {
            int index = rand() % FIELDS;
            string field = "f"+to_string(index);
            if (find(fields.begin(), fields.end(), field) == fields.end())
            {
                fields.push_back(field);
                expected[field] = data1[key][index];
            }
            EXPECT_EQ(p1.read(key, fields), expected);
        }
    }
}

TEST_F(PartitionTest, RemoveTest)
{
    for (auto it : data1)
    {
        EXPECT_EQ(p1.remove(it.first), true);
    }

    for (auto it : data1)
    {
        EXPECT_THROW(p1.read(it.first), int);
    }
}

TEST_F(PartitionTest, SerializationTest)
{
    p1.serialize("file1");
    Partition<Text, FIELDS> p1_("file1");
    for (auto it : data1)
    {
        EXPECT_EQ(p1_.read(it.first), data1[it.first]);
    }

    p2.serialize("file2");
    Partition<long, NUMBER_OF_FIELDS2> p2_("file2");
    for (auto it : data2)
    {
        EXPECT_EQ(p2_.read(it.first), data2[it.first]);
    }
}