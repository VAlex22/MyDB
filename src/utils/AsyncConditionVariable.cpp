#include "AsyncConditionVariable.h"

AsyncConditionVariable::AsyncConditionVariable(boost::asio::io_service& io_service)
: timer_(io_service), notified(false)
        {
            timer_.expires_at(boost::posix_time::pos_infin);
        }

void AsyncConditionVariable::notify()
{
        timer_.cancel();
        notified = true;
}