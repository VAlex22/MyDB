#include "Server.h"

template <typename t, size_t s>
Server<t, s>::Server(boost::asio::io_service &io_service, const std::string &file, array<WorkerThread<t, s>, PARTITIONS> *workers)
    :
        io_service_(io_service),
        acceptor_(io_service, stream_protocol::endpoint(file)),
        workers(workers)
{
    session_ptr<t, s> new_session(new Session<t, s>(io_service_, workers));

    acceptor_.async_accept(
            new_session->socket(),
            boost::bind(
                    &Server::handle_accept,
                    this,
                    new_session,
                    boost::asio::placeholders::error
            )
    );
    cout<< "Server started"<< endl;
}

template <typename t, size_t s>
void Server<t, s>::handle_accept(session_ptr<t, s> new_session, const boost::system::error_code& error)
{
    if (!error)
    {
        new_session->start();
    }

    new_session.reset(new Session<t, s>(io_service_, workers));

    acceptor_.async_accept(
            new_session->socket(),
            boost::bind(
                    &Server::handle_accept,
                    this,
                    new_session,
                    boost::asio::placeholders::error
            )
    );
}

template class Server<Text, 4>;

