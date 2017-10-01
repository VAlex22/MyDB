#ifndef MYDB_WORKERTHREAD_H
#define MYDB_WORKERTHREAD_H

#include "../global.h"
#include "../storage/Partition.h"
#include "../utils/AsyncConditionVariable.h"

struct WorkerRequest
{
    WorkerRequest(boost::asio::io_service & service, int type, unsigned sessionId, string key, void* data);
    WorkerRequest(boost::asio::io_service & service, int type, unsigned sessionId, size_t waiters, string key, void* data);
    int type;
    unsigned sessionId;
    string key;
    void *data;
    AsyncConditionVariable acv;
    bool error=false;
    void *response;
};

template <typename t, size_t s>
class WorkerThread
{
public:
    WorkerThread(const char* threadName, unsigned partitionSize);
    ~WorkerThread();
    void PostMsg(WorkerRequest* wr);
    Partition<t, s> p;

private:
    WorkerThread(const WorkerThread&);
    WorkerThread& operator=(const WorkerThread&);
    void Process();

    uint64_t msgId;
    thread* m_thread;
    queue<WorkerRequest*> m_queue;
    mutex m_mutex;
    condition_variable m_cv;
    const char* THREAD_NAME;

};

template <size_t s>
class WorkerThread<Text, s>
{
public:
    WorkerThread(const char* threadName, unsigned partitionSize);
    ~WorkerThread();
    void PostMsg(WorkerRequest* wr);
    Partition<Text, s> p;
    WorkerThread(const WorkerThread&);
    WorkerThread& operator=(const WorkerThread&);

private:

    void Process();

    uint64_t msgId;
    thread* m_thread;
    queue<WorkerRequest*> m_queue;
    mutex m_mutex;
    condition_variable m_cv;
    const char* THREAD_NAME;

};

template <size_t s>
class WorkerThread<long, s>
{
public:
    WorkerThread(const char* threadName, unsigned partitionSize);
    WorkerThread(const char* threadName, unsigned partitionSize, unordered_map<string, unsigned> fieldIndexes);
    ~WorkerThread();
    void PostMsg(WorkerRequest* wr);
    Partition<long, s> p;
    WorkerThread(const WorkerThread&);
    WorkerThread& operator=(const WorkerThread&);

private:
    void Process();

    uint64_t msgId;
    thread* m_thread;
    queue<WorkerRequest*> m_queue;
    mutex m_mutex;
    condition_variable m_cv;
    const char* THREAD_NAME;

};


#endif //MYDB_WORKERTHREAD_H
