#ifndef MYDB_GLOBAL_H
#define MYDB_GLOBAL_H

#include <string>
#include <unordered_map>
#include <vector>
#include <set>
#include <ctime>
#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdio>

#include <boost/array.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

#define TEXT_SIZE 3

using boost::asio::local::stream_protocol;
using namespace std;

static const int NO_SUCH_ENTRY_EXCEPTION = 1;
static const int INVALID_SCHEMA_EXCEPTION = 2;
static const int INVALID_FIELD_EXCEPTION = 3;

#endif //MYDB_GLOBAL_H
