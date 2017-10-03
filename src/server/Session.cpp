#include "Session.h"

template <size_t s>
Session<Text, s>::Session(boost::asio::io_service& io_service, array<WorkerThread<Text, s>, PARTITIONS> *workers) :
        io_service_(io_service), socket_(io_service), workers(workers), sessionId(++sessionCount)
{
    cout<<"New Session "<<sessionId<<endl;
}

template <size_t s>
stream_protocol::socket& Session<Text, s>::socket()
{
    return socket_;
}

template <size_t s>
void Session<Text, s>::start()
{
    socket_.async_read_some(
            boost::asio::buffer(input),
            boost::bind(
                    &Session<Text, s>::handle_socket_read,
                    this->shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred
            )
    );
}

template <size_t s>
Session<long, s>::Session(boost::asio::io_service& io_service, array<WorkerThread<long, s>, PARTITIONS> *workers) :
        io_service_(io_service), socket_(io_service), workers(workers), sessionId(++sessionCount)
{
    cout<<"New Session "<<sessionId<<endl;
}

template <size_t s>
stream_protocol::socket& Session<long, s>::socket()
{
    return socket_;
}

template <size_t s>
void Session<long, s>::start()
{
    socket_.async_read_some(
            boost::asio::buffer(input),
            boost::bind(
                    &Session<long, s>::handle_socket_read,
                    this->shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred
            )
    );
}

template <size_t s>
void Session<Text, s>::handle_socket_write(const boost::system::error_code &error)
{
    if (!error)
    {
        socket_.async_read_some(
                boost::asio::buffer(input),
                boost::bind(
                        &Session<Text, s>::handle_socket_read,
                        this->shared_from_this(),
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred
                )
        );
    }
}

template <size_t s>
void Session<long, s>::handle_socket_write(const boost::system::error_code &error)
{
    if (!error)
    {
        socket_.async_read_some(
                boost::asio::buffer(input),
                boost::bind(
                        &Session<long, s>::handle_socket_read,
                        this->shared_from_this(),
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred
                )
        );
    }
}

template <size_t s>
void Session<Text, s>::handle_socket_read(const boost::system::error_code &error, size_t bytes_transferred)
{
    if (!error)
    {
        mydb::Request request;
        request.ParseFromArray(input.c_array(), (int) bytes_transferred);
        try {
            switch (request.type()) {
                case mydb::Request_REQUEST_TYPE_DELETE : {

                    WorkerRequest *wr = new WorkerRequest(io_service_, MSG_DELETE, sessionId, request.key(), nullptr);

                    size_t partition = Hash_fn::get_partition(request.key());
                    (*workers)[partition].PostMsg(wr);
                    wr->acv.async_wait(boost::bind(&Session<Text, s>::handle_status, this->shared_from_this(), boost::asio::placeholders::error, wr));
                    break;
                }
                case mydb::Request_REQUEST_TYPE_INSERT_TEXT : {
                    array<Text, s> *ar = new array<Text, s>;

                    const google::protobuf::Map<string, string> &m = request.text_row();
                    size_t partition = Hash_fn::get_partition(request.key());
                    for (auto i = m.begin(); i != m.end(); ++i) {
                        Text text = Text();
                        strcpy(text.x, i->second.c_str());
                        (*ar)[(*workers)[partition].p.fieldIndexes[i->first]] = text;
                    }
                    WorkerRequest *wr = new WorkerRequest(io_service_, MSG_INSERT_TEXT, sessionId, request.key(), ar);
                    (*workers)[partition].PostMsg(wr);
                    wr->acv.async_wait(boost::bind(&Session<Text, s>::handle_status, this->shared_from_this(), boost::asio::placeholders::error, wr));
                    break;
                }

                case mydb::Request_REQUEST_TYPE_READ_TEXT : {
                    const google::protobuf::RepeatedPtrField<string> &fields = request.fields();
                    size_t partition = Hash_fn::get_partition(request.key());
                    if (fields.empty()) {
                        WorkerRequest *wr = new WorkerRequest(io_service_, MSG_READ_FULL_TEXT, sessionId, request.key(), nullptr);
                        //unique_lock<mutex> lock(wr->m);
                        //wr->cv.wait(lock);
                        (*workers)[partition].PostMsg(wr);
                        wr->acv.async_wait(boost::bind(&Session<Text, s>::handle_full_read, this->shared_from_this(), boost::asio::placeholders::error, wr));

                    } else {
                        vector<string> *v = new vector<string>;
                        for (auto i : fields) {
                            v->push_back(i);
                        }
                        WorkerRequest *wr = new WorkerRequest(io_service_, MSG_READ_PARTIAL_TEXT, sessionId, request.key(), v);
                        (*workers)[partition].PostMsg(wr);
                        wr->acv.async_wait(boost::bind(&Session<Text, s>::handle_partial_read, this->shared_from_this(), boost::asio::placeholders::error, wr));
                    }

                    break;
                }

                case mydb::Request_REQUEST_TYPE_UPDATE_TEXT : {

                    unordered_map<string, Text> *newData = new unordered_map<string, Text>;
                    for (auto it : request.text_row()) {
                        Text text = Text();
                        strcpy(text.x, it.second.c_str());

                        newData->insert({it.first, text});
                    }
                    WorkerRequest *wr = new WorkerRequest(io_service_, MSG_UPDATE_TEXT, sessionId, request.key(), newData);
                    size_t partition = Hash_fn::get_partition(request.key());
                    (*workers)[partition].PostMsg(wr);
                    wr->acv.async_wait(boost::bind(&Session<Text, s>::handle_status, this->shared_from_this(), boost::asio::placeholders::error, wr));

                    break;
                }
                default : {
                    break;
                }
            }
        } catch (int er)
        {
            switch (er)
                {
                    case INVALID_FIELD_EXCEPTION: {
                        cout<<"Invalid field exception"<<endl;
                        break;
                    }
                    case NO_SUCH_ENTRY_EXCEPTION: {
                        cout<<"No such entry exception"<<endl;
                        break;
                    }
                    default: {
                        cout<<"Unknown exception"<<endl;
                        break;
                    }
                }
            mydb::Response response;
            response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
            response.set_isstatusok(false);

            int size = response.ByteSize();
            response.SerializeToArray(output.c_array(), size);
            boost::asio::async_write(
                    socket_,
                    boost::asio::buffer(output, size),
                    boost::bind(&Session<Text, s>::handle_socket_write, this->shared_from_this(),
                                boost::asio::placeholders::error)
            );
        }
    }
}

template <size_t s>
void Session<Text, s>::handle_status(const boost::system::error_code &error, WorkerRequest *wr) {
    bool status = (bool) wr->response;
    mydb::Response response;
    response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
    response.set_isstatusok(status);
    delete wr;
    int size = response.ByteSize();
    response.SerializeToArray(output.c_array(), size);
    boost::asio::async_write(
            socket_,
            boost::asio::buffer(output, size),
            boost::bind(&Session<Text, s>::handle_socket_write, this->shared_from_this(),
                        boost::asio::placeholders::error)
    );

}

template <size_t s>
void Session<Text, s>::handle_full_read(const boost::system::error_code& error, WorkerRequest *wr) {
    mydb::Response response;
    if (wr->error)
    {
        response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
        response.set_isstatusok(false);
        delete wr;
        int size = response.ByteSize();
        response.SerializeToArray(output.c_array(), size);
        boost::asio::async_write(
                socket_,
                boost::asio::buffer(output, size),
                boost::bind(&Session<Text, s>::handle_socket_write, this->shared_from_this(),
                            boost::asio::placeholders::error)
        );
    } else
    {

        response.set_type(mydb::Response_RESPONSE_TYPE_READ_TEXT);
        array<Text, s> *ar = static_cast<array<Text, s> *>(wr->response);
        for (int i = 0; i < s; i++) {
            response.mutable_text_result()->insert({(*workers)[0].p.indexFields[i], string((*ar)[i].x)});
        }
        delete ar;
        delete wr;
        int size = response.ByteSize();
        response.SerializeToArray(output.c_array(), size);
        boost::asio::async_write(
                socket_,
                boost::asio::buffer(output, size),
                boost::bind(&Session<Text, s>::handle_socket_write, this->shared_from_this(),
                            boost::asio::placeholders::error)
        );


    }
}
template <size_t s>
void Session<Text, s>::handle_partial_read(const boost::system::error_code& error, WorkerRequest *wr) {
    mydb::Response response;
    if (wr->error)
    {
        response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
        response.set_isstatusok(false);
        delete wr;

        int size = response.ByteSize();
        response.SerializeToArray(output.c_array(), size);
        boost::asio::async_write(
                socket_,
                boost::asio::buffer(output, size),
                boost::bind(&Session<Text, s>::handle_socket_write, this->shared_from_this(),
                            boost::asio::placeholders::error)
        );
    } else
    {
        response.set_type(mydb::Response_RESPONSE_TYPE_READ_TEXT);
        unordered_map<string, Text> *res = static_cast<unordered_map<string, Text> *>(wr->response);
        for (auto r : *res) {
            response.mutable_text_result()->insert({r.first, string(r.second.x)});
        }

        int size = response.ByteSize();
        response.SerializeToArray(output.c_array(), size);
        delete res;
        delete wr;
        boost::asio::async_write(
                socket_,
                boost::asio::buffer(output, size),
                boost::bind(&Session<Text, s>::handle_socket_write, this->shared_from_this(),
                            boost::asio::placeholders::error)
        );
    }
}

template <size_t s>
void Session<long, s>::handle_socket_read(const boost::system::error_code &error, size_t bytes_transferred)
{
    if (!error) {
        mydb::Request request;
        request.ParseFromArray(input.c_array(), (int) bytes_transferred);

        switch (request.type()) {
            case mydb::Request_REQUEST_TYPE_DELETE : {
                WorkerRequest *wr = new WorkerRequest(io_service_, MSG_DELETE, sessionId, request.key(), nullptr);
                size_t partition = Hash_fn::get_partition(request.key());
                (*workers)[partition].PostMsg(wr);;
                wr->acv.async_wait(boost::bind(&Session<long, s>::handle_status, this->shared_from_this(),
                                               boost::asio::placeholders::error, wr));
                break;
            }

            case mydb::Request_REQUEST_TYPE_INSERT_LONG : {

                array<long, s> *ar = new array<long, s>;
                (*ar)[0] = request.long_row();
                WorkerRequest *wr = new WorkerRequest(io_service_, MSG_INSERT_LONG, sessionId, request.key(), ar);
                size_t partition = Hash_fn::get_partition(request.key());
                (*workers)[partition].PostMsg(wr);
                wr->acv.async_wait(boost::bind(&Session<long, s>::handle_status, this->shared_from_this(),
                                               boost::asio::placeholders::error, wr));

                break;
            }

            case mydb::Request_REQUEST_TYPE_READ_LONG : {
                //cout<<"read request"<<endl;
                WorkerRequest *wr = new WorkerRequest(io_service_, MSG_READ_LONG, sessionId, request.key(), nullptr);

                size_t partition = Hash_fn::get_partition(request.key());

                (*workers)[partition].PostMsg(wr);
                wr->acv.async_wait(boost::bind(&Session<long, s>::handle_read, this->shared_from_this(),
                                               boost::asio::placeholders::error, wr));
                break;
            }

            case mydb::Request_REQUEST_TYPE_UPDATE_LONG : {
                //cout<<"update request"<<endl;
                unordered_map<string, long> *newData = new unordered_map<string, long>;
                newData->insert({request.long_field(), request.long_row()});
                WorkerRequest *wr = new WorkerRequest(io_service_, MSG_UPDATE_LONG, sessionId, request.key(), newData);
                size_t partition = Hash_fn::get_partition(request.key());

                (*workers)[partition].PostMsg(wr);
                wr->acv.async_wait(boost::bind(&Session<long, s>::handle_status, this->shared_from_this(),
                                               boost::asio::placeholders::error, wr));
                break;
            }
            case mydb::Request_REQUEST_TYPE_START_TRANSACTION : {
                //cout<<"start"<<endl;
                WorkerRequest *wr = new WorkerRequest(io_service_, MSG_START_TRANSACTION, sessionId, PARTITIONS, request.key(), nullptr);
                for (size_t partition = 0; partition < PARTITIONS; partition++)
                {
                    (*workers)[partition].PostMsg(wr);
                }
                wr->acv.async_wait(boost::bind(&Session<long, s>::handle_status, this->shared_from_this(),
                                               boost::asio::placeholders::error, wr));
                break;
            }
            case mydb::Request_REQUEST_TYPE_COMMIT : {
                //cout<<"validate"<<endl;
                WorkerRequest *wr = new WorkerRequest(io_service_, MSG_VALIDATE_TRANSACTION, sessionId, PARTITIONS, request.key(), nullptr);
                for (size_t partition = 0; partition < PARTITIONS; partition++)
                {
                    (*workers)[partition].PostMsg(wr);
                }
                wr->acv.async_wait(boost::bind(&Session<long, s>::handle_validate_transaction, this->shared_from_this(),
                                               boost::asio::placeholders::error, wr));

                break;
            }
            case mydb::Request_REQUEST_TYPE_ABORT : {
                WorkerRequest *wr = new WorkerRequest(io_service_, MSG_ABORT_TRANSACTION, sessionId, PARTITIONS, request.key(), nullptr);
                for (size_t partition = 0; partition < PARTITIONS; partition++)
                {
                    (*workers)[partition].PostMsg(wr);
                }
                wr->acv.async_wait(boost::bind(&Session<long, s>::handle_status, this->shared_from_this(),
                                               boost::asio::placeholders::error, wr));
                break;
                break;
            }
            default : {
                break;
            }
        }

    }
}


template <size_t s>
void Session<long, s>::handle_status(const boost::system::error_code &error, WorkerRequest *wr) {
    bool status = (bool) wr->response;
    mydb::Response response;

    response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
    response.set_isstatusok(status);
    delete wr;
    int size = response.ByteSize();
    response.SerializeToArray(output.c_array(), size);
    boost::asio::async_write(
            socket_,
            boost::asio::buffer(output, size),
            boost::bind(&Session<long, s>::handle_socket_write, this->shared_from_this(),
                        boost::asio::placeholders::error)
    );
}

template <size_t s>
void Session<long, s>::handle_read(const boost::system::error_code& error, WorkerRequest *wr) {
    mydb::Response response;
    if (wr->error) {
        response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
        response.set_isstatusok(false);
        delete wr;
        int size = response.ByteSize();
        response.SerializeToArray(output.c_array(), size);
        boost::asio::async_write(
                socket_,
                boost::asio::buffer(output, size),
                boost::bind(&Session<long, s>::handle_socket_write, this->shared_from_this(),
                            boost::asio::placeholders::error)
        );
    }
    else {
        response.set_type(mydb::Response_RESPONSE_TYPE_READ_LONG);

        array<long, s> *ar = static_cast<array<long, s> *>(wr->response);
        response.set_long_result(to_string((*ar)[0]));

        int size = response.ByteSize();
        response.SerializeToArray(output.c_array(), size);

        delete ar;
        delete wr;
        boost::asio::async_write(
                socket_,
                boost::asio::buffer(output, size),
                boost::bind(&Session<long, s>::handle_socket_write, this->shared_from_this(),
                            boost::asio::placeholders::error)
        );
    }
}

template <size_t s>
void Session<long, s>::handle_validate_transaction(const boost::system::error_code &error, WorkerRequest *wr) {
    //cout<<"write"<<endl;
    bool abort = false;
    unsigned cts = 0;
    for (unsigned i = 0; i < PARTITIONS; i++)
    {
        if (wr->tsar[i] == 0)
        {
            abort = true;
            break;
        }
        else
        {
            cts = max(cts, wr->tsar[i]);
        }
    }


    delete wr;

    if (abort)
    {
        mydb::Response response;
        response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
        response.set_isstatusok(false);
        int size = response.ByteSize();
        response.SerializeToArray(output.c_array(), size);
        boost::asio::async_write(
                socket_,
                boost::asio::buffer(output, size),
                boost::bind(&Session<long, s>::handle_socket_write, this->shared_from_this(),
                            boost::asio::placeholders::error)
        );
    }
    else
    {
        WorkerRequest *writewr = new WorkerRequest(io_service_, MSG_WRITE_TRANSACTION, sessionId, PARTITIONS, "",
                                                   &cts);
        for (size_t partition = 0; partition < PARTITIONS; partition++)
        {
            (*workers)[partition].PostMsg(writewr);
        }
        wr->acv.async_wait(boost::bind(&Session<long, s>::handle_status, this->shared_from_this(),
                                       boost::asio::placeholders::error, writewr));
    }
}

template <size_t s>
unsigned Session<long, s>::sessionCount = 0;

template <size_t s>
unsigned Session<Text, s>::sessionCount = 0;

template class Session<Text, FIELDS>;
template class Session<long, 1>;
