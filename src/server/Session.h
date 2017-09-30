#ifndef MYDB_SESSION_H
#define MYDB_SESSION_H

#include "WorkerThread.h"
#include "../global.h"
#include "../messages/Messages.pb.h"
#include "../storage/Partition.h"
#include "../utils/Hash_fn.h"

template <typename t, size_t s>
class Session : public boost::enable_shared_from_this<Session<t,s>>
{
public:
    Session(boost::asio::io_service& io_service, array<WorkerThread<t, s>, PARTITIONS> *workers);
    stream_protocol::socket& socket();
    void start();
    void handle_socket_read(const boost::system::error_code &error, size_t bytes_transferred);
    void handle_socket_write(const boost::system::error_code &error);

private:
    boost::asio::io_service& io_service_;
    stream_protocol::socket socket_;
    boost::array<char, BUFFER_SIZE> input;
    boost::array<char, BUFFER_SIZE> output;
    array<WorkerThread<t, s>, PARTITIONS> *workers;
};

template <size_t s>
class Session<Text, s> : public boost::enable_shared_from_this<Session<Text, s>>
{
public:
    Session(boost::asio::io_service& io_service, array<WorkerThread<Text, s>, PARTITIONS> *workers);
    stream_protocol::socket& socket();
    void start();
    void handle_socket_read(const boost::system::error_code &error, size_t bytes_transferred);
    void handle_socket_write(const boost::system::error_code &error);

private:
    void handle_delete(const boost::system::error_code& error, WorkerRequest *wr);
    void handle_insert(const boost::system::error_code& error, WorkerRequest *wr);
    void handle_full_read(const boost::system::error_code& error, WorkerRequest *wr);
    void handle_partial_read(const boost::system::error_code& error, WorkerRequest *wr);
    void handle_update(const boost::system::error_code& error, WorkerRequest *wr);
    boost::asio::io_service& io_service_;
    stream_protocol::socket socket_;
    boost::array<char, BUFFER_SIZE> input;
    boost::array<char, BUFFER_SIZE> output;
    array<WorkerThread<Text, s>, PARTITIONS> *workers;
};

template <size_t s>
class Session<long, s> : public boost::enable_shared_from_this<Session<long, s>>
{
public:
    Session(boost::asio::io_service& io_service, array<WorkerThread<long, s>, PARTITIONS> *workers);
    stream_protocol::socket& socket();
    void start();
    void handle_socket_read(const boost::system::error_code &error, size_t bytes_transferred);
    void handle_delete(const boost::system::error_code& error, WorkerRequest *wr);
    void handle_insert(const boost::system::error_code& error, WorkerRequest *wr);
    void handle_read(const boost::system::error_code& error, WorkerRequest *wr);
    void handle_update(const boost::system::error_code& error, WorkerRequest *wr);
    void handle_socket_write(const boost::system::error_code &error);

private:
    boost::asio::io_service& io_service_;
    stream_protocol::socket socket_;
    boost::array<char, BUFFER_SIZE> input;
    boost::array<char, BUFFER_SIZE> output;
    array<WorkerThread<long, s>, PARTITIONS> *workers;
};


#endif //MYDB_SESSION_H
