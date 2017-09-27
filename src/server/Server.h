#ifndef MYDB_SERVER_H
#define MYDB_SERVER_H

#include "../global.h"
#include "../messages/Messages.pb.h"
#include "../storage/Partition.h"
#include "Session.h"


template <typename t, size_t s>
using session_ptr = typename boost::shared_ptr<Session<t, s>>;

template <typename t, size_t s>
class Server
{
public:
    Server(boost::asio::io_service &io_service, const std::string &file, array<WorkerThread<t, s>, PARTITIONS> *workers);
    void handle_accept(session_ptr<t, s> new_session, const boost::system::error_code& error);

private:
    array<WorkerThread<t, s>, PARTITIONS> *workers;
    boost::asio::io_service& io_service_;
    stream_protocol::acceptor acceptor_;
};



#endif //MYDB_SERVER_H
