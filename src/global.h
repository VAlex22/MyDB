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
#include <thread>
#include <queue>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <map>

#include <boost/array.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>


#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

#define TEXT_SIZE                           10
#define FIELDS                              10

#define MSG_EXIT_THREAD			            1
#define MSG_DELETE		                    2
#define MSG_INSERT_TEXT                     3
#define MSG_UPDATE_TEXT                     4
#define MSG_READ_FULL_TEXT                  5
#define MSG_READ_PARTIAL_TEXT               6
#define MSG_INSERT_LONG                     7
#define MSG_UPDATE_LONG                     8
#define MSG_READ_LONG                       9
#define MSG_START_TRANSACTION               10
#define MSG_LOCK_TRANSACTION_SET            11
#define MSG_COMPUTE_TRANSACTION_TIMESTAMP   12
#define MSG_VALIDATE_TRANSACTION            13
#define MSG_WRITE_TRANSACTION               14
#define MSG_ABORT_TRANSACTION               15


#define PARTITIONS                          1
#define BUFFER_SIZE                         512

using boost::asio::local::stream_protocol;
using namespace std;

static const int NO_SUCH_ENTRY_EXCEPTION = 1;
static const int INVALID_SCHEMA_EXCEPTION = 2;
static const int INVALID_FIELD_EXCEPTION = 3;
static const int LOCKED_EXCEPTION = 4;

#endif //MYDB_GLOBAL_H
