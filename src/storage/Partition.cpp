#include "Partition.h"

bool Text::operator==(const Text &a) const
{
    return (memcmp(a.x, x, TEXT_SIZE) == 0);
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
        fieldIndexes.insert({"f"+to_string(i),i});
        indexFields.insert({i,"f"+to_string(i)});
    }
}

template <typename t, size_t s>
Partition<t, s>::Partition(unsigned size, unordered_map<string, unsigned> fieldIndexes)
        : size(size), used(0), fieldIndexes(fieldIndexes)
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
    archive>>rowsByKey;
    archive>>fieldIndexes;
    archive>>indexFields;
    archive>>size;
    archive>>used;
    archive>>rowSize;
    f.close();
}

template <typename t, size_t s>
bool Partition<t, s>::update(string key, unordered_map<string, t> newData)
{
    auto row = rowsByKey.find(key);
    if (row == rowsByKey.end())
    {
        throw NO_SUCH_ENTRY_EXCEPTION;
    }
    else
    {
        for (auto field : newData)
        {
            auto index = fieldIndexes.find(field.first);
            if (index == fieldIndexes.end())
            {
                throw INVALID_FIELD_EXCEPTION;
            }
            else
            {
                row->second[index->second] = field.second;
            }
        }
        return true;
    }
}

template <typename t, size_t s>
bool Partition<t, s>::remove(string key) {
    if (rowsByKey.erase(key) == 1)
    {
        used -= rowSize;
        return true;
    } else
    {
        return false;
    }

}

template <typename t, size_t s>
bool Partition<t, s>::insert(string key, array<t, s> ar)
{
    auto p = rowsByKey.insert({key, ar});
    if (!p.second) {
        p.first->second = ar;
    } else
    {
        used += rowSize;
    }
    return true;
}

template <typename t, size_t s>
unordered_map<string, t> Partition<t, s>::read(string key, vector<string> fields) {
    auto row = rowsByKey.find(key);
    if (row == rowsByKey.end())
    {
        throw NO_SUCH_ENTRY_EXCEPTION;

    } else
    {
        unordered_map<string, t> result;
        for (string field : fields)
        {
            auto index = fieldIndexes.find(field);
            if (index == fieldIndexes.end())
            {

                throw INVALID_FIELD_EXCEPTION;

            }
            else
            {
                result[field] = row->second[index->second];
            }
        }
        return result;
    }
}

template <typename t, size_t s>
array<t, s> Partition<t, s>::read(string key) {
    auto result = rowsByKey.find(key);
    if (result != rowsByKey.end())
    {
        return result->second;
    }
    else
    {
        throw NO_SUCH_ENTRY_EXCEPTION;
    }
}

template <typename t, size_t s>
bool Partition<t, s>::serialize(string file) {
    ofstream f(file);
    boost::archive::binary_oarchive archive(f);
    archive<<rowsByKey;
    archive<<fieldIndexes;
    archive<<indexFields;
    archive<<size;
    archive<<used;
    archive<<rowSize;
    f.close();

    return true;
}

template class Partition<Text, 4>;
template class Partition<Text, 10>;
template class Partition<long, 1>;


