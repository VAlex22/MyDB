#include "WorkerThread.h"

template <size_t s>
WorkerThread<Text, s>::WorkerThread(const char* threadName, unsigned partitionSize) :
        m_thread(0), THREAD_NAME(threadName), msgId(0), p(Partition<Text, s>(partitionSize))
{
    m_thread = new thread(&WorkerThread::Process, this);
}

template <size_t s>
WorkerThread<long, s>::WorkerThread(const char* threadName, unsigned partitionSize) :
        m_thread(0), THREAD_NAME(threadName), msgId(0), p(Partition<long, s>(partitionSize))
{
    m_thread = new thread(&WorkerThread::Process, this);
}

template <size_t s>
WorkerThread<long, s>::WorkerThread(const char* threadName, unsigned partitionSize, unordered_map<string, unsigned> fieldIndexes) :
        m_thread(0), THREAD_NAME(threadName), msgId(0), p(Partition<long, s>(partitionSize, fieldIndexes))
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
void WorkerThread<long, s>::PostMsg(WorkerRequest* data)
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

                    cout << "Exit thread on " << THREAD_NAME << endl;
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

                    bool status = p.update(msg->key, *newData);
                    delete newData;
                    msg->response = (void *) status;
                    msg->acv.notify();

                    break;
                }

                case MSG_READ_FULL_TEXT: {

                    array<Text, s> *ar = new array<Text,s>;
                    *ar = p.read(msg->key);
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
        WorkerRequest *msg;
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

                    cout << "Exit thread on " << THREAD_NAME << endl;
                    return;
                }

                case MSG_DELETE: {
                    bool status = p.remove(msg->key);
                    msg->response = (void *) status;
                    msg->acv.notify();
                    break;
                }

                case MSG_INSERT_LONG: {
                    array<long, s> *ar = static_cast<array<long, s> *>(msg->data);
                    bool status = p.insert(msg->key, *ar);
                    delete ar;
                    msg->response = (void *) status;
                    msg->acv.notify();
                    break;
                }

                case MSG_UPDATE_LONG: {
                    unordered_map<string, long> *newData = static_cast<unordered_map<string, long> *>(msg->data);
                    bool status = p.update(msg->key, *newData);
                    delete newData;
                    msg->response = (void *) status;
                    msg->acv.notify();
                    break;
                }

                case MSG_READ_LONG: {
                    array<long, s> *ar = new array<long, s>;
                    *ar = p.read(msg->key);
                    msg->response = (void *) ar;
                    msg->acv.notify();
                    break;
                }

                default:
                    break;
            }

        } catch (int er) {
            switch (er) {
                case INVALID_FIELD_EXCEPTION: {
                    cout << "Invalid field exception" << endl;
                    break;
                }
                case NO_SUCH_ENTRY_EXCEPTION: {
                    cout << "No such entry exception" << endl;
                    break;
                }
                default: {
                    cout << "Unknown exception" << endl;
                    break;
                }
            }
            msg->response = (void *) false;
            msg->error = true;
            msg->acv.notify();
        }
    }
}

WorkerRequest::WorkerRequest(boost::asio::io_service & service, int type, string key, void* data) :
        acv(service), type(type), key(key), data(data), error(false)
{
}

template class WorkerThread<Text, FIELDS>;

template class WorkerThread<long, 1>;
