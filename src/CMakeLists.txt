set(MyDB_SRCS storage/Partition.cpp storage/Partition.h messages/Messages.pb.cc messages/Messages.pb.h
        server/Server.cpp server/Server.h server/Session.cpp server/Session.h global.h server/WorkerThread.cpp
        server/WorkerThread.h utils/AsyncConditionVariable.cpp utils/AsyncConditionVariable.h utils/Hash_fn.cpp utils/Hash_fn.h utils/TimestampGenerator.cpp utils/TimestampGenerator.h)

add_executable(server server/main.cpp)

add_library(libmydb ${MyDB_SRCS})
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.63 COMPONENTS serialization system REQUIRED)
find_package(Protobuf REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})
target_link_libraries(libmydb ${Boost_SERIALIZATION_LIBRARY} ${Boost_SYSTEM_LIBRARY} ${PROTOBUF_LIBRARIES})
target_link_libraries(server libmydb)