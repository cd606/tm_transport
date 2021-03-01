#ifndef TM_KIT_TRANSPORT_RABBITMQ_RABBITMQ_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_RABBITMQ_RABBITMQ_COMPONENT_HPP_

#include <memory>
#include <functional>
#include <optional>
#include <exception>
#include <unordered_map>
#include <thread>

#include <tm_kit/infra/WithTimeData.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/ConnectionLocator.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace rabbitmq {
    
    class RabbitMQComponentImpl;

    class RabbitMQComponentException : public std::runtime_error {
    public:
        RabbitMQComponentException(std::string const &info) : std::runtime_error(info) {}
    };

    class RabbitMQComponent {
    private:
        std::unique_ptr<RabbitMQComponentImpl> impl_;
    public:
        enum ExceptionPolicy {
            Throw
            , IgnoreForWriteAndThrowForRead
            , IgnoreForWriteAndStopForRead
        };
        RabbitMQComponent(ExceptionPolicy exceptionPolicy=ExceptionPolicy::Throw);
        RabbitMQComponent(RabbitMQComponent &&);
        RabbitMQComponent &operator=(RabbitMQComponent &&);
        ~RabbitMQComponent();

        //The bool parameters in the std::function signatures are the
        //final flags

        //for exchanges, the identifier in locator is the exchange name
        //for subscription, the topic can be in rabbitmq wildcard format
        uint32_t rabbitmq_addExchangeSubscriptionClient(ConnectionLocator const &locator,
                        std::string const &topic,
                        std::function<void(basic::ByteDataWithTopic &&)> client,
                        std::optional<WireToUserHook> wireToUserHook = std::nullopt);
        void rabbitmq_removeExchangeSubscriptionClient(uint32_t);
        std::function<void(basic::ByteDataWithTopic &&)> rabbitmq_getExchangePublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
        //for RPC queues, the identifier in locator is the queue name
        std::function<void(basic::ByteDataWithID &&)> rabbitmq_setRPCQueueClient(ConnectionLocator const &locator,
                        std::function<void(bool, basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the requester
        void rabbitmq_removeRPCQueueClient(ConnectionLocator const &locator);
        std::function<void(bool, basic::ByteDataWithID &&)> rabbitmq_setRPCQueueServer(ConnectionLocator const &locator,
                        std::function<void(basic::ByteDataWithID &&)> server,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the replier, where bool means whether it is the final reply
        std::unordered_map<ConnectionLocator, std::thread::native_handle_type> rabbitmq_threadHandles();
    };

} } } } }

#endif