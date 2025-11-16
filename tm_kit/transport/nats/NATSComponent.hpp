#ifndef TM_KIT_TRANSPORT_NATS_NATS_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_NATS_NATS_COMPONENT_HPP_

#include <memory>
#include <functional>
#include <optional>
#include <regex>
#include <variant>
#include <string>
#include <unordered_map>
#include <thread>

#include <tm_kit/infra/WithTimeData.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/ConnectionLocator.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace nats {
    
    class NATSComponentImpl;

    class NATSComponentException : public std::runtime_error {
    public:
        NATSComponentException(std::string const &info) : std::runtime_error(info) {}
    };

    class NATSComponent {
    private:
        std::unique_ptr<NATSComponentImpl> impl_;
    public:
        NATSComponent();
        virtual ~NATSComponent();
        NATSComponent(NATSComponent const &) = delete;
        NATSComponent(NATSComponent &&);
        NATSComponent &operator=(NATSComponent const &) = delete;
        NATSComponent &operator=(NATSComponent &&);
        //host and port are needed in the locator
        struct NoTopicSelection {};
        uint32_t nats_addSubscriptionClient(ConnectionLocator const &locator,
                        std::string const &topic,
                        std::function<void(basic::ByteDataWithTopic &&)> client,
                        std::optional<WireToUserHook> wireToUserHook = std::nullopt);
        void nats_removeSubscriptionClient(uint32_t id);
        std::function<void(basic::ByteDataWithTopic &&)> nats_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
        //for RPC, host, queue and an RPC channel name (as identifier) are needed in the locator
        std::function<void(basic::ByteDataWithID &&)> nats_setRPCClient(ConnectionLocator const &locator,                        
                        std::function<void(bool, basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt,
                        uint32_t *clientNumberOutput = nullptr); //the return value is the requester
        void nats_removeRPCClient(ConnectionLocator const &locator, uint32_t clientNumber);
        std::function<void(bool, basic::ByteDataWithID &&)> nats_setRPCServer(ConnectionLocator const &locator,
                        std::function<void(basic::ByteDataWithID &&)> server,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the replier, where bool means whether it is the final reply
        std::unordered_map<ConnectionLocator, std::thread::native_handle_type> nats_threadHandles();
    };

} } } } }


#endif