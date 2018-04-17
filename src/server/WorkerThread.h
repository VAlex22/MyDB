#ifndef MYDB_WORKERTHREAD_H
#define MYDB_WORKERTHREAD_H

#include "../global.h"
#include "../storage/Partition.h"
#include "../utils/AsyncConditionVariable.h"

struct WorkerRequest
{
    WorkerRequest(boost::asio::io_service & service, int type, unsigned sessionId, string key, void* data);
    WorkerRequest(boost::asio::io_service & service, int type, unsigned sessionId, size_t waiters, string key, void* data);
    const atomic_int type;
    const atomic_int sessionId;
    string key;
    void *data;
    AsyncConditionVariable acv;
    bool error=false;
    void *response;
    array<unsigned, PARTITIONS> tsar;
};

struct LongWorkerRequest
{
    LongWorkerRequest(boost::asio::io_service & service, int type, unsigned sessionId, int waiters, string key, long value);
    LongWorkerRequest(boost::asio::io_service & service, int type, unsigned sessionId, int waiters, unsigned cts);
    AsyncConditionVariable acv;
    int type;
    unsigned sessionId;
    string key;
    long value;
    unsigned cts;
    bool error=false;
    array<unsigned, PARTITIONS> tsar;
};


template <typename t, size_t s>
class WorkerThread
{
public:
    WorkerThread(unsigned threadId, unsigned partitionSize);
    ~WorkerThread();
    Partition<t, s> p;

private:
    WorkerThread(const WorkerThread&);
    WorkerThread& operator=(const WorkerThread&);
    void Process();

    uint64_t msgId;
    thread* m_thread;
    queue<WorkerRequest*> m_dequeue;
    mutex m_mutex;
    condition_variable m_cv;
    unsigned threadId;
};

template <size_t s>
class WorkerThread<Text, s>
{
public:
    WorkerThread(unsigned threadId, unsigned partitionSize);
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
    unsigned threadId;

};

template <size_t s>
class WorkerThread<long, s>
{
public:
    WorkerThread(unsigned threadId, unsigned partitionSize);
    WorkerThread(unsigned threadId, unsigned partitionSize, unordered_map<string, unsigned> fieldIndexes);
    ~WorkerThread();
    void PostMsg(LongWorkerRequest* wr);
    Partition<long, s> p;
    WorkerThread(const WorkerThread&);
    WorkerThread& operator=(const WorkerThread&);

private:
    void Process();

    uint64_t msgId;
    thread* m_thread;
    deque<WorkerRequest*> m_deque;

    mutex m_mutex;
    condition_variable m_cv;
    void postLockedMessages(unsigned session);
    unordered_map<unsigned, vector<WorkerRequest*>> locked_msg;
    unsigned threadId;

};


#endif //MYDB_WORKERTHREAD_H
