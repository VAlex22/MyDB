set (Test_SRCS test_storage/PartitionTest.cpp test_storage/PartitionTest.h main.cpp)

include_directories(../src/storage)

add_executable(mydbtest ${Test_SRCS})

target_link_libraries(mydbtest
        libmydb
        libgtest
        )
