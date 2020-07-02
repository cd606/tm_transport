#ifndef TM_KIT_TRANSPORT_ZEROMQ_ZEROMQ_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_ZEROMQ_ZEROMQ_COMPONENT_HPP_

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

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace zeromq {
    
    class ZeroMQComponentImpl;

    class ZeroMQComponent {
    private:
        std::unique_ptr<ZeroMQComponentImpl> impl_;
    public:
        ZeroMQComponent();
        ~ZeroMQComponent();
        //only host and port are needed in the locators
        struct NoTopicSelection {};
        uint32_t zeroMQ_addSubscriptionClient(ConnectionLocator const &locator,
                        std::variant<NoTopicSelection, std::string, std::regex> const &topic,
                        std::function<void(basic::ByteDataWithTopic &&)> client,
                        std::optional<WireToUserHook> wireToUserHook = std::nullopt);
        void zeroMQ_removeSubscriptionClient(uint32_t id);
        std::function<void(basic::ByteDataWithTopic &&)> zeroMQ_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
    };

} } } } }


#endif