#ifndef MYDB_SESSION_H
#define MYDB_SESSION_H

#include "WorkerThread.h"
#include "../global.h"
#include "../messages/Messages.pb.h"
#include "../storage/Partition.h"

template <typename t, size_t s>
class Session : public boost::enable_shared_from_this<Session<t,s>>
{
public:
    Session(boost::asio::io_service& io_service, WorkerThread<t, s> *w);
    stream_protocol::socket& socket();
    void start();
    void handle_read(const boost::system::error_code& error, size_t bytes_transferred);
    void handle_write(const boost::system::error_code& error);

private:
    stream_protocol::socket socket_;
    boost::array<char, 1024> input;
    char output[1024];
    WorkerThread<t, s> *w;
    //Partition<t, s> *p;

};

template <size_t s>
class Session<Text, s> : public boost::enable_shared_from_this<Session<Text, s>>
{
public:
    Session(boost::asio::io_service& io_service, WorkerThread<Text, s> *w);
    stream_protocol::socket& socket();
    void start();
    void handle_read(const boost::system::error_code& error, size_t bytes_transferred);
    void handle_write(const boost::system::error_code& error);

private:
    stream_protocol::socket socket_;
    boost::array<char, 1024> input;
    char output[1024];
    WorkerThread<Text, s> *w;
    //Partition<Text, s> *p;

};

template <size_t s>
class Session<long, s> : public boost::enable_shared_from_this<Session<long, s>>
{
public:
    Session(boost::asio::io_service& io_service, WorkerThread<long, s> *w);
    stream_protocol::socket& socket();
    void start();
    void handle_read(const boost::system::error_code& error, size_t bytes_transferred);
    void handle_write(const boost::system::error_code& error);

private:
    stream_protocol::socket socket_;
    boost::array<char, 1024> input;
    char output[1024];
    WorkerThread<long, s> *w;
    //Partition<long, s> *p;

};


#endif //MYDB_SESSION_H
