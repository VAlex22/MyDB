set(MyDB_SRCS storage/Partition.cpp storage/Partition.h global.h)

add_library(libmydb ${MyDB_SRCS})
