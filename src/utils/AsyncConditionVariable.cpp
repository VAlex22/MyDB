#include "AsyncConditionVariable.h"

AsyncConditionVariable::AsyncConditionVariable(boost::asio::io_service& io_service, int waiters)
: timer_(io_service), waiters(waiters)
        {
            timer_.expires_at(boost::posix_time::pos_infin);
        }

AsyncConditionVariable::AsyncConditionVariable(boost::asio::io_service& io_service)
        : timer_(io_service), waiters(1)
{
    timer_.expires_at(boost::posix_time::pos_infin);
}

void AsyncConditionVariable::notify()
{
    waiters--;
    if (waiters == 0)
    {
        timer_.cancel();
    }
}