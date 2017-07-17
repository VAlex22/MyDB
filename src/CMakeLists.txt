set(MyDB_SRCS storage/Partition.cpp storage/Partition.h global.h)

add_library(libmydb ${MyDB_SRCS})
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.63 COMPONENTS serialization REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
target_link_libraries(libmydb ${Boost_SERIALIZATION_LIBRARY})