#ifndef MYDB_SESSION_H
#define MYDB_SESSION_H

#include "../global.h"
#include "../messages/Messages.pb.h"
#include "../storage/Partition.h"

template <typename t, size_t s>
class Session : public boost::enable_shared_from_this<Session<t,s>>
{
public:
    Session(boost::asio::io_service& io_service, Partition<t, s> *p);
    stream_protocol::socket& socket();
    void start();
    void handle_read(const boost::system::error_code& error, size_t bytes_transferred);
    void handle_write(const boost::system::error_code& error);

private:
    stream_protocol::socket socket_;
    boost::array<char, 1024> input;
    char output[1024];
    Partition<t, s> *p;

};

template <size_t s>
class Session<Text, s> : public boost::enable_shared_from_this<Session<Text, s>>
{
public:
    Session(boost::asio::io_service& io_service, Partition<Text, s> *p);
    stream_protocol::socket& socket();
    void start();
    void handle_read(const boost::system::error_code& error, size_t bytes_transferred);
    void handle_write(const boost::system::error_code& error);

private:
    stream_protocol::socket socket_;
    boost::array<char, 1024> input;
    char output[1024];
    Partition<Text, s> *p;

};

template <size_t s>
class Session<long, s> : public boost::enable_shared_from_this<Session<long, s>>
{
public:
    Session(boost::asio::io_service& io_service, Partition<long, s> *p);
    stream_protocol::socket& socket();
    void start();
    void handle_read(const boost::system::error_code& error, size_t bytes_transferred);
    void handle_write(const boost::system::error_code& error);

private:
    stream_protocol::socket socket_;
    boost::array<char, 1024> input;
    char output[1024];
    Partition<long, s> *p;

};


#endif //MYDB_SESSION_H
