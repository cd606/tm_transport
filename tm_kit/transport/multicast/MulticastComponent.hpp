#ifndef TM_KIT_TRANSPORT_MULTICAST_MULTICAST_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_MULTICAST_MULTICAST_COMPONENT_HPP_

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

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace multicast {
    
    class MulticastComponentImpl;

    class MulticastComponent {
    private:
        std::unique_ptr<MulticastComponentImpl> impl_;
    public:
        MulticastComponent();
        ~MulticastComponent();
        //only host and port are needed in the locators
        struct NoTopicSelection {};
        void multicast_addSubscriptionClient(ConnectionLocator const &locator,
                        std::variant<NoTopicSelection, std::string, std::regex> const &topic,
                        std::function<void(basic::ByteDataWithTopic &&)> client,
                        std::optional<WireToUserHook> wireToUserHook = std::nullopt);
        //the "int" parameter is the ttl
        std::function<void(basic::ByteDataWithTopic &&, int)> multicast_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
    };

} } } } }


#endif