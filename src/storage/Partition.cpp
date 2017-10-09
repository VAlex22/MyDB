#include "Partition.h"

bool Text::operator==(const Text &a) const
{
    return (memcmp(a.x, x, TEXT_SIZE) == 0);
}

template <typename t, size_t s>
Row<t, s>::Row(array<t, s> fields, unsigned timestamp) : fields(fields), timestamp(timestamp), locker(0)
{
}

template <typename t, size_t s>
Tup<t, s>::Tup(array<t, s> fields, unsigned timestamp, string key) : fields(fields), timestamp(timestamp), key(key)
{
}

template <typename Archive>
void Text::serialize(Archive& ar, const unsigned int version)
{
    ar & x;
}

std::ostream& operator<< (std::ostream &o, const Text& t)
{
    for (int i = 0; i < TEXT_SIZE; i++)
        o << t.x[i];
    return o;
}

template <typename t, size_t s>
Partition<t, s>::Partition(unsigned size) : size(size), used(0) {
    rowSize = sizeof(array<t, s>);
    rowsByKey.reserve(size / rowSize);
    for (unsigned i = 0; i < s; i++)
    {
        fieldIndexes.insert({"field"+to_string(i),i});
        indexFields.insert({i,"field"+to_string(i)});
    }
}

template <typename t, size_t s>
Partition<t, s>::Partition(unsigned size, unordered_map<string, unsigned> fieldIndexes, int id)
        : size(size), used(0), fieldIndexes(fieldIndexes), id(id)
{
    rowSize = sizeof(array<t, s>);
    rowsByKey.reserve(size/rowSize);
    for (auto field : fieldIndexes) {
        indexFields.insert({field.second, field.first});
    }
}

template <typename t, size_t s>
Partition<t, s>::Partition(string file) {
    ifstream f(file);
    boost::archive::binary_iarchive archive(f);
    //archive>>rowsByKey;
    archive>>fieldIndexes;
    archive>>indexFields;
    archive>>size;
    archive>>used;
    archive>>rowSize;
    f.close();
}

template <typename t, size_t s>
bool Partition<t, s>::remove(string key) {

    if (rowsByKey.erase(key) == 1) {
        used -= rowSize;
        return true;
    } else {
        return false;
    }
}

template <typename t, size_t s>
bool Partition<t, s>::insert(string key, array<t, s> ar)
{

        Row<t, s> row(ar, TimestampGenerator::currentTimestamp());
        auto p = rowsByKey.insert({key, row});
        if (!p.second) {
            p.first->second = row;
        } else {
            used += rowSize;
        }
        return true;

}

template <typename t, size_t s>
unordered_map<string, t> Partition<t, s>::read(string key, vector<string> fields) {

    auto row = rowsByKey.find(key);
    if (row == rowsByKey.end()) {
        throw NO_SUCH_ENTRY_EXCEPTION;

    } else {

        unordered_map<string, t> result;
        for (string field : fields) {

            auto index = fieldIndexes.find(field);
            if (index == fieldIndexes.end()) {
                throw INVALID_FIELD_EXCEPTION;
            } else {
                result[field] = row->second.fields[index->second];
            }
        }
        return result;
    }
}

template <typename t, size_t s>
bool Partition<t, s>::serialize(string file) {
    ofstream f(file);
    boost::archive::binary_oarchive archive(f);
    //archive<<rowsByKey;
    archive<<fieldIndexes;
    archive<<indexFields;
    archive<<size;
    archive<<used;
    archive<<rowSize;
    f.close();

    return true;
}
/*
 * Functions with transaction support!!!
 */

template <typename t, size_t s>
array<t, s> Partition<t, s>::read(string key, unsigned session, unsigned *locker) {
    auto ac = autoCommitBySession.find(session);
    if (ac == autoCommitBySession.end())
    {
        autoCommitBySession[session] = true;
    }

    auto res = transactionSets[session].find(key);
    if (res == transactionSets[session].end())
    {
        auto result = rowsByKey.find(key);
        if (result != rowsByKey.end()) {
            if (!autoCommitBySession[session]) {
                if (result->second.locker != 0) {
                    *locker = result->second.locker;
                }
                else
                {
                    transactionSets[session].insert(
                            {key, Tup<t, s>(result->second.fields, result->second.timestamp, key)});
                }
            }

            return result->second.fields;
        } else {
            throw NO_SUCH_ENTRY_EXCEPTION;
        }
    } else {
        return res->second.fields;
    }
}

template <typename t, size_t s>
bool Partition<t, s>::update(string key, unordered_map<string, t> newData, unsigned session)
{
    auto ac = autoCommitBySession.find(session);
    if (ac == autoCommitBySession.end())
    {
        autoCommitBySession[session] = true;
    }
    if (autoCommitBySession[session]) {
        auto row = rowsByKey.find(key);
        if (row == rowsByKey.end()) {
            throw NO_SUCH_ENTRY_EXCEPTION;
        } else {
            for (auto field : newData) {
                auto index = fieldIndexes.find(field.first);
                if (index == fieldIndexes.end()) {
                    throw INVALID_FIELD_EXCEPTION;
                } else {
                    row->second.fields[index->second] = field.second;
                }
            }
            return true;
        }
    } else
    {
        auto row  = transactionSets[session].find(key);
        if (row == transactionSets[session].end()) {
            throw NO_SUCH_ENTRY_EXCEPTION;
        } else {
            for (auto field : newData) {
                auto index = fieldIndexes.find(field.first);
                if (index == fieldIndexes.end()) {
                    throw INVALID_FIELD_EXCEPTION;
                } else {
                    row->second.fields[index->second] = field.second;
                }
            }
            return true;
        }
    }
}

template <typename t, size_t s>
void Partition<t, s>::startTransaction(unsigned session) {
    autoCommitBySession[session] = false;
    map<string, Tup<t, s>> transactionSet;
    transactionSets[session] = transactionSet;
}

template <typename t, size_t s>
unsigned Partition<t, s>::lockTransactionSet(unsigned session) {
    unsigned locker = 0;
    for (auto it = transactionSets[session].begin(); it != transactionSets[session].end(); it++)
    {
        if (rowsByKey.at(it->second.key).locker != 0)
        {
            for (auto it_ = transactionSets[session].begin(); it_ != it; it_++)
            {
                rowsByKey.at(it_->second.key).locker = 0;
            }
            locker = rowsByKey.at(it->second.key).locker;
            break;
        }
        else
        {
            rowsByKey.at(it->second.key).locker = session;
        }
    }
    return locker;

};

template <typename t, size_t s>
unsigned Partition<t, s>::computeTransactionTimestamp(unsigned session) {

    unsigned ts = 0;

    for (auto it : transactionSets[session])
    {
        ts = max(ts, it.second.timestamp + 1);
    }

    return ts;
}


template <typename t, size_t s>
unsigned Partition<t, s>::validateTransaction(unsigned session) {

    unsigned status = 1;

    for (auto it : transactionSets[session])
    {
        if (it.second.timestamp != rowsByKey.at(it.second.key).timestamp)
        {
            status = 0;
            break;
        }
    }

    return status;
}

template <typename t, size_t s>
bool Partition<t, s>::writeTransaction(unsigned session, unsigned commitTs) {
    int i=0;
    for (auto it : transactionSets[session])
    {
        i++;
        rowsByKey.at(it.second.key).fields = it.second.fields;
        rowsByKey.at(it.second.key).timestamp = commitTs;
        rowsByKey.at(it.second.key).locker = 0;
        //cout<<it.first<<" unlocked by commit"<<endl;
    }

    string ss = string("unlocked partition ") + to_string(id)+ " session "+ to_string(session) +" " + to_string(i);
    cout<<ss<<endl;
    transactionSets.erase(session);
    autoCommitBySession[session] = true;
    return true;
}

template <typename t, size_t s>
void Partition<t, s>::abort(unsigned session) {
    int i=0;
    for (auto row: transactionSets[session])
    {
        i++;
        rowsByKey.at(row.second.key).locker = 0;
    }

    string ss = string("unlocked ab partition ") + to_string(id)+ " session "+ to_string(session) +" " + to_string(i);
    cout<<ss<<endl;
    transactionSets.erase(session);
    autoCommitBySession[session] = true;
}

template class Tup<long, 1>;
template class Tup<Text, FIELDS>;
template class Row<long, 1>;
template class Row<Text, FIELDS>;
template class Partition<Text, FIELDS>;
template class Partition<long, 1>;

