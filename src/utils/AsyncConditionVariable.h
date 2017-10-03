#ifndef MYDB_ASYNCCONDITIONVARIABLE_H
#define MYDB_ASYNCCONDITIONVARIABLE_H

#include "../global.h"

class AsyncConditionVariable {

public:
    explicit
    AsyncConditionVariable(boost::asio::io_service& io_service, int waiters);
    explicit
    AsyncConditionVariable(boost::asio::io_service& io_service);
    template <typename WaitHandler>
    void async_wait(WaitHandler handler) {
        cout<<"wait"<<endl;

        mtx.lock();
        timer_.async_wait(handler);
        cout<<"waiters"<<waiters<<endl;
        if (waiters == 0)
        {
            timer_.cancel_one();
        }
        mtx.unlock();
        cout<<"wait unlock"<<endl;
    };
    void notify();

private:
    mutex mtx;
    int waiters;
    boost::asio::deadline_timer timer_;
};



#endif //MYDB_ASYNCCONDITIONVARIABLE_H
