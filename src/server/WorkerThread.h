#ifndef MYDB_WORKERTHREAD_H
#define MYDB_WORKERTHREAD_H

#include "../global.h"
#include "../storage/Partition.h"
#include "AsyncConditionVariable.h"

struct WorkerRequest
{
    WorkerRequest(int type, const string *key, const void* data, boost::asio::io_service & service);
    int type;
    const string *key;
    const void *data;
    //condition_variable cv;
    //mutex m;
    AsyncConditionVariable acv;
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
class WorkerThread<long, s>
{
public:
    WorkerThread(const char* threadName, unsigned partitionSize);
    ~WorkerThread();
    void PostMsg(WorkerRequest* wr);
    Partition<long, s> p;

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


#endif //MYDB_WORKERTHREAD_H
