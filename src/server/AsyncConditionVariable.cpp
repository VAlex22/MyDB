#include "AsyncConditionVariable.h"

AsyncConditionVariable::AsyncConditionVariable(boost::asio::io_service& io_service)
: timer_(io_service)
        {
                // Setting expiration to infinity will cause handlers to
                // wait on the timer until cancelled.
                timer_.expires_at(boost::posix_time::pos_infin);
        }

/*template <typename WaitHandler>
void AsyncConditionVariable::async_wait(WaitHandler handler)
{
    // bind is used to adapt the user provided handler to the deadline
    // timer's wait handler type requirement.
    timer_.async_wait(boost::bind(handler));
}*/

void AsyncConditionVariable::notify() { timer_.cancel();     }