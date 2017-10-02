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
        timer_.async_wait(handler);
        if (waiters == 0)
        {
            timer_.cancel();
        }
    };
    void notify();

private:
    atomic<int> waiters;
    boost::asio::deadline_timer timer_;
};



#endif //MYDB_ASYNCCONDITIONVARIABLE_H
