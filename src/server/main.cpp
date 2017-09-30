#include "../global.h"
#include "Server.h"
#include "WorkerThread.h"

int main(int argc, char* argv[])
{
    try
    {

        boost::asio::io_service io_service;
        /*
        array<WorkerThread<Text, FIELDS>, PARTITIONS> workers = {
                {WorkerThread<Text, FIELDS>("1", 1000), WorkerThread<Text, FIELDS>("2", 1000), WorkerThread<Text, FIELDS>("3", 1000)}
        };


        std::remove("/tmp/mydbsocket");
        Server<Text, FIELDS> s(io_service, "/tmp/mydbsocket", &workers);
        */

        unordered_map<string, unsigned> fieldIndexes;
        fieldIndexes["balance"] = 0;
        array<WorkerThread<long, 1>, PARTITIONS> workers = {
                {WorkerThread<long, 1>("1", 1000, fieldIndexes), WorkerThread<long, 1>("2", 1000, fieldIndexes), WorkerThread<long, 1>("3", 1000, fieldIndexes)}
        };


        std::remove("/tmp/mydbsocket");
        Server<long, 1> s(io_service, "/tmp/mydbsocket", &workers);

        io_service.run();
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}



