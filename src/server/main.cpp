#include "../global.h"
#include "Server.h"
#include "WorkerThread.h"

int main(int argc, char* argv[])
{
    try
    {

        boost::asio::io_service io_service;
        //Partition<Text, 4> *p = new Partition<Text, 4>(1000);
        array<WorkerThread<Text, 4>, PARTITIONS> workers = {
                {WorkerThread<Text, 4>("1", 1000), WorkerThread<Text, 4>("2", 1000), WorkerThread<Text, 4>("3", 1000)}
        };


        std::remove("/tmp/mydbsocket");
        Server<Text, 4> s(io_service, "/tmp/mydbsocket", &workers);

        io_service.run();
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}



