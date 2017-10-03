#include "WorkerThread.h"

template <size_t s>
WorkerThread<Text, s>::WorkerThread(unsigned threadId, unsigned partitionSize) :
        m_thread(0), threadId(threadId), msgId(0), p(Partition<Text, s>(partitionSize))
{
    m_thread = new thread(&WorkerThread::Process, this);
}

template <size_t s>
WorkerThread<long, s>::WorkerThread(unsigned threadId, unsigned partitionSize) :
        m_thread(0), threadId(threadId), msgId(0), p(Partition<long, s>(partitionSize))
{
    m_thread = new thread(&WorkerThread::Process, this);
}

template <size_t s>
WorkerThread<long, s>::WorkerThread(unsigned threadId, unsigned partitionSize, unordered_map<string, unsigned> fieldIndexes) :
        m_thread(0), threadId(threadId), msgId(0), p(Partition<long, s>(partitionSize, fieldIndexes))
{
    m_thread = new thread(&WorkerThread::Process, this);
}

template <size_t s>
WorkerThread<Text, s>::~WorkerThread()
{
    if (!m_thread)
        return;

    // Create a new ThreadMsg
    //WorkerRequest* request = new WorkerRequest();

    // Put exit thread message into the queue
    {
        lock_guard<mutex> lock(m_mutex);
      //  m_queue.push(request);
        m_cv.notify_one();
    }

    m_thread->join();
    delete m_thread;
    m_thread = 0;
}

template <size_t s>
WorkerThread<long, s>::~WorkerThread()
{
    if (!m_thread)
        return;

    // Create a new ThreadMsg
    //WorkerRequest* request = new WorkerRequest();

    // Put exit thread message into the queue
    {
        lock_guard<mutex> lock(m_mutex);
        //m_queue.push(request);
        m_cv.notify_one();
    }

    m_thread->join();
    delete m_thread;
    m_thread = 0;
}

template <size_t s>
void WorkerThread<Text, s>::PostMsg(WorkerRequest* data)
{

    // Add user data msg to queue and notify worker thread
    std::unique_lock<std::mutex> lk(m_mutex);
    m_queue.push(data);
    m_cv.notify_one();
    return;
}

template <size_t s>
void WorkerThread<long, s>::PostMsg(LongWorkerRequest* data)
{
    std::unique_lock<std::mutex> lk(m_mutex);
    m_queue.push(data);
    m_cv.notify_one();
    return;
}

template <size_t s>
void WorkerThread<Text, s>::Process()
{
    while (1)
    {
        WorkerRequest* msg;
        {
            // Wait for a message to be added to the queue
            std::unique_lock<std::mutex> lk(m_mutex);
            while (m_queue.empty()) {
                m_cv.wait(lk);
            }
            if (m_queue.empty())
                continue;

            msg = m_queue.front();
            m_queue.pop();
        }
        try {
            switch (msg->type) {

                case MSG_EXIT_THREAD: {
                    delete msg;
                    std::unique_lock<std::mutex> lk(m_mutex);
                    while (!m_queue.empty()) {
                        msg = m_queue.front();
                        m_queue.pop();
                        delete msg;
                    }

                    cout << "Exit thread on " << threadId << endl;
                    return;
                }

                case MSG_DELETE: {
                    bool status = p.remove(msg->key);
                    msg->response = (void *) status;
                    msg->acv.notify();
                    break;
                }

                case MSG_INSERT_TEXT: {
                    array<Text, s> *ar = static_cast<array<Text, s> *>(msg->data);
                    bool status = p.insert(msg->key, *ar);
                    delete ar;
                    msg->response = (void *) status;
                    msg->acv.notify();
                    break;
                }

                case MSG_UPDATE_TEXT: {

                    unordered_map<string, Text> *newData = static_cast<unordered_map<string, Text> *>(msg->data);

                    bool status = p.update(msg->key, *newData, msg->sessionId);
                    delete newData;
                    msg->response = (void *) status;
                    msg->acv.notify();

                    break;
                }

                case MSG_READ_FULL_TEXT: {

                    array<Text, s> *ar = new array<Text,s>;
                    *ar = p.read(msg->key, msg->sessionId);
                    msg->response = (void *) ar;
                    msg->acv.notify();
                    break;
                }

                case MSG_READ_PARTIAL_TEXT: {
                    vector<string> *v = static_cast<vector<string> *>(msg->data);

                    unordered_map<string, Text> *res = new unordered_map<string, Text>;
                            *res = p.read(msg->key, *v);
                    delete v;
                    msg->response = (void *) res;
                    msg->acv.notify();
                    break;
                }

                default:
                    break;
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
            msg->response = (void *) false;
            msg->error = true;
            msg->acv.notify();

        }
    }
}

template <size_t s>
void WorkerThread<long, s>::Process() {
    while (1) {
        LongWorkerRequest *msg;
        {
            // Wait for a message to be added to the queue
            std::unique_lock<std::mutex> lk(m_mutex);
            while (m_queue.empty())
                m_cv.wait(lk);

            if (m_queue.empty())
                continue;

            msg = m_queue.front();
            m_queue.pop();
        }
        try {
            switch (msg->type) {

                case MSG_EXIT_THREAD: {
                    delete msg;
                    std::unique_lock<std::mutex> lk(m_mutex);
                    while (!m_queue.empty()) {
                        msg = m_queue.front();
                        m_queue.pop();
                        delete msg;
                    }

                    cout << "Exit thread on " << threadId << endl;
                    return;
                }

                case MSG_DELETE: {
                    bool status = p.remove(msg->key);
                    msg->error = status;
                    msg->acv.notify();
                    break;
                }

                case MSG_INSERT_LONG: {
                    array<long, s> ar;
                    ar[0] = msg->value;
                    msg->error = p.insert(msg->key, ar);
                    msg->acv.notify();
                    break;
                }

                case MSG_UPDATE_LONG: {
                    unordered_map<string, long> newData;
                    newData["balance"] = msg->value;
                    msg->error = p.update(msg->key, newData, msg->sessionId);
                    msg->acv.notify();
                    break;
                }

                case MSG_READ_LONG: {
                    array<long, s> ar;
                    ar = p.read(msg->key, msg->sessionId);
                    msg->value = ar[0];
                    msg->acv.notify();
                    break;
                }
                case MSG_START_TRANSACTION: {
                    p.startTransaction(msg->sessionId);
                    msg->acv.notify();
                    break;
                }
                case MSG_VALIDATE_TRANSACTION: {
                    unsigned ts = p.validateTransaction(msg->sessionId);
                    msg->tsar[threadId] = ts;
                    msg->acv.notify();
                    break;
                }
                case MSG_WRITE_TRANSACTION: {
                    unsigned cts = (msg->cts);
                    p.writeTransaction(msg->sessionId, cts);
                    msg->acv.notify();
                    break;
                }
                case MSG_ABORT_TRANSACTION: {
                    p.abort(msg->sessionId);
                    msg->acv.notify();
                    break;
                }
                default:
                    break;
            }

        } catch (int er) {
            switch (er) {
                case INVALID_FIELD_EXCEPTION: {
                    cout << "Invalid field exception" << msg->key <<" "<<msg->type<< endl;
                    break;
                }
                case NO_SUCH_ENTRY_EXCEPTION: {
                    cout << "No such entry exception" << msg->key <<" "<<msg->type<< endl;
                    break;
                }
                case LOCKED_EXCEPTION: {
                    cout << "Row locked, trying again, key: " << msg->key <<" "<<msg->type<<" "<< threadId<< endl;
                    PostMsg(msg);
                    break;

                }
                default: {
                    cout << "Unknown exception " << endl;
                    break;
                }
            }
            msg->error = true;
            msg->acv.notify();
        }
    }
}

WorkerRequest::WorkerRequest(boost::asio::io_service & service, int type, unsigned sessionId, string key, void *data) :
        acv(service), type(type), sessionId(sessionId), key(key), data(data), error(false)
{
}

WorkerRequest::WorkerRequest(boost::asio::io_service &service, int type, unsigned sessionId, size_t waiters, string key,
                             void *data) :
        acv(service, waiters), type(type), sessionId(sessionId), key(key), data(data), error(false)
{
}

LongWorkerRequest::LongWorkerRequest(boost::asio::io_service &service, int type, unsigned sessionId, int waiters,
                                     string key, long value) :
        acv(service, waiters), type(type), sessionId(sessionId), key(key), value(value), error(false)
{

}

LongWorkerRequest::LongWorkerRequest(boost::asio::io_service &service, int type, unsigned sessionId, int waiters,
                                     unsigned cts):
        acv(service, waiters), type(type), sessionId(sessionId), cts(cts), error(false)
{

}


template class WorkerThread<Text, FIELDS>;
template class WorkerThread<long, 1>;