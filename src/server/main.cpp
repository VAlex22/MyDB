#include "../global.h"
#include "Server.h"

int main(int argc, char* argv[])
{
    try
    {

        boost::asio::io_service io_service;
        Partition<Text, 4> *p = new Partition<Text, 4>(1000);

        std::remove("/tmp/mydbsocket");
        Server<Text, 4> s(io_service, "/tmp/mydbsocket", p);

        io_service.run();
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}



