#ifndef TM_KIT_TRANSPORT_REDIS_REDIS_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_REDIS_REDIS_COMPONENT_HPP_

#include <memory>
#include <functional>
#include <optional>
#include <regex>
#include <variant>
#include <string>

#include <tm_kit/infra/WithTimeData.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/ConnectionLocator.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace redis {
    
    class RedisComponentImpl;

    class RedisComponentException : public std::runtime_error {
    public:
        RedisComponentException(std::string const &info) : std::runtime_error(info) {}
    };

    class RedisComponent {
    private:
        std::unique_ptr<RedisComponentImpl> impl_;
    public:
        RedisComponent();
        ~RedisComponent();
        RedisComponent(RedisComponent const &) = delete;
        RedisComponent(RedisComponent &&);
        RedisComponent &operator=(RedisComponent const &) = delete;
        RedisComponent &operator=(RedisComponent &&);
        //host and port are needed in the locator
        struct NoTopicSelection {};
        void redis_addSubscriptionClient(ConnectionLocator const &locator,
                        std::string const &topic,
                        std::function<void(basic::ByteDataWithTopic &&)> client,
                        std::optional<WireToUserHook> wireToUserHook = std::nullopt);
        std::function<void(basic::ByteDataWithTopic &&)> redis_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
        //for RPC, host, queue and an RPC channel name (as identifier) are needed in the locator
        std::function<void(basic::ByteDataWithID &&)> redis_setRPCClient(ConnectionLocator const &locator,
                        std::function<std::string()> clientCommunicationIDCreator,
                        std::function<void(basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the requester
        std::function<void(bool, basic::ByteDataWithID &&)> redis_setRPCServer(ConnectionLocator const &locator,
                        std::function<void(basic::ByteDataWithID &&)> server,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the replier, where bool means whether it is the final reply

    };

} } } } }


#endif