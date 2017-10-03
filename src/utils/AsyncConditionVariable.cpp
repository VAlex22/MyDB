#include "AsyncConditionVariable.h"

AsyncConditionVariable::AsyncConditionVariable(boost::asio::io_service& io_service, int waiters)
: timer_(io_service), waiters(waiters)
        {
            //timer_.expires_at(boost::posix_time::pos_infin);
            timer_.expires_from_now(boost::posix_time::seconds(5));
        }

AsyncConditionVariable::AsyncConditionVariable(boost::asio::io_service& io_service)
        : timer_(io_service), waiters(1)
{
    //timer_.expires_at(boost::posix_time::pos_infin);
    timer_.expires_from_now(boost::posix_time::seconds(5));
}

void AsyncConditionVariable::notify()
{
    cout<<"notified"<<endl;
    mtx.lock();
    waiters--;
    cout<<"waiters "<<waiters<<endl;
    if (waiters == 0)
    {
        timer_.cancel_one();
    }
    mtx.unlock();
    cout<<"notified unlock"<<endl;
}