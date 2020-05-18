#ifndef TM_KIT_TRANSPORT_RABBITMQ_RABBITMQ_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_RABBITMQ_RABBITMQ_COMPONENT_HPP_

#include <memory>
#include <functional>
#include <optional>
#include <exception>

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
        RabbitMQComponent();
        ~RabbitMQComponent();

        //The std::string const & parameters in the callbacks are content encoding schemes

        //for exchanges, the identifier in locator is the exchange name
        //for subscription, the topic can be in rabbitmq wildcard format
        void rabbitmq_addExchangeSubscriptionClient(ConnectionLocator const &locator,
                        std::string const &topic,
                        std::function<void(std::string const &, basic::ByteDataWithTopic &&)> client,
                        std::optional<WireToUserHook> wireToUserHook = std::nullopt);
        std::function<void(std::string const &, basic::ByteDataWithTopic &&)> rabbitmq_getExchangePublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
        //for RPC queues, the identifier in locator is the queue name
        std::function<void(std::string const &, basic::ByteDataWithID &&)> rabbitmq_setRPCQueueClient(ConnectionLocator const &locator,
                        std::function<void(std::string const &, basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the requester
        std::function<void(bool, std::string const &, basic::ByteDataWithID &&)> rabbitmq_setRPCQueueServer(ConnectionLocator const &locator,
                        std::function<void(std::string const &, basic::ByteDataWithID &&)> server,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the replier, where bool means whether it is the final reply
    };

} } } } }

#endif