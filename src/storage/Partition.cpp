#include "Partition.h"

bool text::operator==(const text &a) const {
    return (memcmp(a.x, x, TEXT_SIZE) == 0);
}

std::ostream& operator<< (std::ostream &o, const text& t)
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
    }
}

template <typename t, size_t s>
Partition<t, s>::Partition(unsigned size, unordered_map<string, unsigned> fieldIndexes)
        : size(size), used(0), fieldIndexes(fieldIndexes)
{
    rowSize = sizeof(array<t, s>);
    rowsByKey.reserve(size/rowSize);
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

template class Partition<text, 10>;
template class Partition<long, 1>;


