#include "Session.h"

void yield(){}

template <size_t s>
Session<Text, s>::Session(boost::asio::io_service& io_service, WorkerThread<Text, s> *w) : socket_(io_service), w(w)
{
    cout<<"New Session"<<endl;
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
                    &Session<Text, s>::handle_read,
                    this->shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred
            )
    );
}

template <size_t s>
Session<long, s>::Session(boost::asio::io_service& io_service, WorkerThread<long, s> *w) : socket_(io_service), w(w)
{
    cout<<"New Session"<<endl;
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
                    &Session<long, s>::handle_read,
                    this->shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred
            )
    );
}

template <size_t s>
void Session<Text, s>::handle_write(const boost::system::error_code& error)
{
    if (!error)
    {
        socket_.async_read_some(
                boost::asio::buffer(input),
                boost::bind(
                        &Session<Text, s>::handle_read,
                        this->shared_from_this(),
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred
                )
        );
    }
}

template <size_t s>
void Session<long, s>::handle_write(const boost::system::error_code& error)
{
    if (!error)
    {
        socket_.async_read_some(
                boost::asio::buffer(input),
                boost::bind(
                        &Session<long, s>::handle_read,
                        this->shared_from_this(),
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred
                )
        );
    }
}

template <size_t s>
void Session<Text, s>::handle_read(const boost::system::error_code& error, size_t bytes_transferred)
{
    if (!error)
    {
        mydb::Request request;
        request.ParseFromArray(input.c_array(), (int) bytes_transferred);
        try {
            switch (request.type()) {
                case mydb::Request_REQUEST_TYPE_DELETE : {
                    WorkerRequest *wr = new WorkerRequest(MSG_DELETE, &request.key(), nullptr);
                    unique_lock<mutex> lock(wr->m);
                    w->PostMsg(wr);

                    //wr->cv.wait(lock);
                    while(!wr->responseReady)
                    {
                        yield_timer.expires_from_now(boost::posix_time::seconds(1));
                        yield_timer.async_wait(&yield);
                    }
                    bool status = (bool) wr->response;
                    delete wr;
                    //bool status = p->remove(request.key());
                    mydb::Response response;
                    response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
                    response.set_isstatusok(status);

                    int size = response.ByteSize();
                    response.SerializeToArray(output, size);
                    boost::asio::async_write(
                            socket_,
                            boost::asio::buffer(output, size),
                            boost::bind(&Session<Text, s>::handle_write, this->shared_from_this(),
                                        boost::asio::placeholders::error)
                    );

                    break;
                }
                case mydb::Request_REQUEST_TYPE_INSERT_TEXT : {
                    array<Text, s> ar;

                    const google::protobuf::Map<string, string> &m = request.text_row();
                    if (typeid(Text) == typeid(Text)) {
                        for (auto i = m.begin(); i != m.end(); ++i) {
                            Text text = Text();
                            strcpy(text.x, i->second.c_str());
                            ar[w->p.fieldIndexes[i->first]] = text;
                        }
                        WorkerRequest *wr = new WorkerRequest(MSG_INSERT_TEXT, &request.key(), &ar);
                        unique_lock<mutex> lock(wr->m);
                        w->PostMsg(wr);
                        //wr->cv.wait(lock);
                        while(!wr->responseReady)
                        {
                            yield_timer.expires_from_now(boost::posix_time::seconds(1));
                            yield_timer.async_wait(&yield);
                        }
                        bool status = (bool) wr->response;
                        delete wr;
                        //bool status = p->insert(request.key(), ar);
                        mydb::Response response;
                        response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
                        response.set_isstatusok(status);

                        int size = response.ByteSize();
                        response.SerializeToArray(output, size);
                        boost::asio::async_write(
                                socket_,
                                boost::asio::buffer(output, size),
                                boost::bind(&Session<Text, s>::handle_write, this->shared_from_this(),
                                            boost::asio::placeholders::error)
                        );
                    }
                    break;
                }

                case mydb::Request_REQUEST_TYPE_READ_TEXT : {
                    const google::protobuf::RepeatedPtrField<string> &fields = request.fields();

                    mydb::Response response;
                    response.set_type(mydb::Response_RESPONSE_TYPE_READ_TEXT);
                    if (fields.empty()) {
                        WorkerRequest *wr = new WorkerRequest(MSG_READ_FULL_TEXT, &request.key(), nullptr);
                        unique_lock<mutex> lock(wr->m);
                        w->PostMsg(wr);
                        //wr->cv.wait(lock);
                        while(!wr->responseReady)
                        {
                            yield_timer.expires_from_now(boost::posix_time::seconds(1));
                            yield_timer.async_wait(&yield);
                        }
                        array<Text, s> *ar = static_cast<array<Text, s>*>(wr->response);
                        delete wr;
                        //array<Text, s> ar = p->read(request.key());
                        for (int i = 0; i < s; i++) {
                            response.mutable_text_result()->insert({w->p.indexFields[i], string((*ar)[i].x)});
                        }
                    } else {
                        vector<string> v;
                        for (auto i : fields) {
                            v.push_back(i);
                        }
                        WorkerRequest *wr = new WorkerRequest(MSG_READ_PARTIAL_TEXT, &request.key(), &v);
                        unique_lock<mutex> lock(wr->m);
                        w->PostMsg(wr);
                        //wr->cv.wait(lock);
                        while(!wr->responseReady)
                        {
                            yield_timer.expires_from_now(boost::posix_time::seconds(1));
                            yield_timer.async_wait(&yield);
                        }
                        unordered_map<string, Text> *res = static_cast<unordered_map<string, Text>*>(wr->response);
                        delete wr;
                        //unordered_map<string, Text> res = p->read(request.key(), v);
                        for (auto r : *res) {
                            response.mutable_text_result()->insert({r.first, string(r.second.x)});
                        }
                    }

                    int size = response.ByteSize();
                    cout << size << endl;
                    response.SerializeToArray(output, size);
                    boost::asio::async_write(
                            socket_,
                            boost::asio::buffer(output, size),
                            boost::bind(&Session<Text, s>::handle_write, this->shared_from_this(),
                                        boost::asio::placeholders::error)
                    );

                    break;
                }

                case mydb::Request_REQUEST_TYPE_UPDATE_TEXT : {

                    mydb::Response response;
                    response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
                    unordered_map<string, Text> newData;
                    for (auto it : request.text_row()) {
                        Text text = Text();
                        strcpy(text.x, it.second.c_str());

                        newData.insert({it.first, text});
                    }

                    WorkerRequest *wr = new WorkerRequest(MSG_UPDATE_TEXT, &request.key(), &newData);
                    unique_lock<mutex> lock(wr->m);
                    w->PostMsg(wr);
                    //wr->cv.wait(lock);
                    while(!wr->responseReady)
                    {
                        yield_timer.expires_from_now(boost::posix_time::seconds(1));
                        yield_timer.async_wait(&yield);
                    }
                    bool status = (bool) wr->response;
                    delete wr;
                    //bool status = p->update(request.key(), newData);
                    response.set_isstatusok(status);

                    int size = response.ByteSize();
                    cout << size << endl;
                    response.SerializeToArray(output, size);
                    boost::asio::async_write(
                            socket_,
                            boost::asio::buffer(output, size),
                            boost::bind(&Session<Text, s>::handle_write, this->shared_from_this(),
                                        boost::asio::placeholders::error)
                    );

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
            cout << size << endl;
            response.SerializeToArray(output, size);
            boost::asio::async_write(
                    socket_,
                    boost::asio::buffer(output, size),
                    boost::bind(&Session<Text, s>::handle_write, this->shared_from_this(),
                                boost::asio::placeholders::error)
            );
        }
    }
}


template <size_t s>
void Session<long, s>::handle_read(const boost::system::error_code& error, size_t bytes_transferred)
{
    if (!error)
    {
        mydb::Request request;
        request.ParseFromArray(input.c_array(), (int) bytes_transferred);
        try {
            switch (request.type()) {
                case mydb::Request_REQUEST_TYPE_DELETE : {
                    WorkerRequest *wr = new WorkerRequest(MSG_DELETE, &request.key(), nullptr);
                    unique_lock<mutex> lock(wr->m);
                    w->PostMsg(wr);
                    //wr->cv.wait(lock);
                    while(!wr->responseReady)
                    {
                        yield_timer.expires_from_now(boost::posix_time::seconds(1));
                        yield_timer.async_wait(&yield);
                    }
                    bool status = (bool) wr->response;
                    delete wr;
                    //bool status = p->remove(request.key());
                    mydb::Response response;
                    response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
                    response.set_isstatusok(status);

                    int size = response.ByteSize();
                    response.SerializeToArray(output, size);
                    boost::asio::async_write(
                            socket_,
                            boost::asio::buffer(output, size),
                            boost::bind(&Session<long, s>::handle_write, this->shared_from_this(),
                                        boost::asio::placeholders::error)
                    );

                    break;
                }

                case mydb::Request_REQUEST_TYPE_INSERT_LONG : {

                    array<long, s> ar;

                    ar[0] = request.long_row();

                    WorkerRequest *wr = new WorkerRequest(MSG_DELETE, &request.key(), &ar);
                    unique_lock<mutex> lock(wr->m);
                    w->PostMsg(wr);
                    //wr->cv.wait(lock);
                    while(!wr->responseReady)
                    {
                        yield_timer.expires_from_now(boost::posix_time::seconds(1));
                        yield_timer.async_wait(&yield);
                    }
                    bool status = (bool) wr->response;
                    delete wr;
                    //bool status = p->insert(request.key(), ar);
                    mydb::Response response;

                    response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
                    response.set_isstatusok(status);

                    int size = response.ByteSize();
                    response.SerializeToArray(output, size);
                    boost::asio::async_write(
                            socket_,
                            boost::asio::buffer(output, size),
                            boost::bind(&Session<long, s>::handle_write, this->shared_from_this(),
                                        boost::asio::placeholders::error)
                    );

                    break;
                }

                case mydb::Request_REQUEST_TYPE_READ_LONG : {
                    mydb::Response response;
                    response.set_type(mydb::Response_RESPONSE_TYPE_READ_LONG);
                    WorkerRequest *wr = new WorkerRequest(MSG_READ_LONG, &request.key(), nullptr);
                    unique_lock<mutex> lock(wr->m);
                    w->PostMsg(wr);
                    //wr->cv.wait(lock);
                    while(!wr->responseReady)
                    {
                        yield_timer.expires_from_now(boost::posix_time::seconds(1));
                        yield_timer.async_wait(&yield);
                    }
                    array<long, s> *ar = static_cast<array<long, s>*>(wr->response);
                    delete wr;
                    //array<long, s> ar = p->read(request.key());
                    response.set_long_result((*ar)[0]);

                    int size = response.ByteSize();
                    cout << size << endl;
                    response.SerializeToArray(output, size);
                    boost::asio::async_write(
                            socket_,
                            boost::asio::buffer(output, size),
                            boost::bind(&Session<long, s>::handle_write, this->shared_from_this(),
                                        boost::asio::placeholders::error)
                    );

                    break;
                }

                case mydb::Request_REQUEST_TYPE_UPDATE_LONG : {
                    mydb::Response response;
                    response.set_type(mydb::Response_RESPONSE_TYPE_STATUS);
                    unordered_map<string, long> newData;

                    newData.insert({request.long_field(), request.long_row()});

                    WorkerRequest *wr = new WorkerRequest(MSG_UPDATE_LONG, &request.key(), &newData);
                    unique_lock<mutex> lock(wr->m);
                    w->PostMsg(wr);
                    //wr->cv.wait(lock);
                    while(!wr->responseReady)
                    {
                        yield_timer.expires_from_now(boost::posix_time::seconds(1));
                        yield_timer.async_wait(&yield);
                    }
                    bool status = (bool) wr->response;
                    delete wr;
                    //bool status = p->update(request.key(), newData);
                    response.set_isstatusok(status);

                    int size = response.ByteSize();
                    cout << size << endl;
                    response.SerializeToArray(output, size);
                    boost::asio::async_write(
                            socket_,
                            boost::asio::buffer(output, size),
                            boost::bind(&Session<long, s>::handle_write, this->shared_from_this(),
                                        boost::asio::placeholders::error)
                    );

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
            cout << size << endl;
            response.SerializeToArray(output, size);
            boost::asio::async_write(
                    socket_,
                    boost::asio::buffer(output, size),
                    boost::bind(&Session<long, s>::handle_write, this->shared_from_this(),
                                boost::asio::placeholders::error)
            );
        }
    }
}

template class Session<Text, 4>;
template class Session<long, 1>;