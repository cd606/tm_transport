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

    enum class MulticastComponentTopicEncodingChoice {
        CBOR
        , BinaryAdHoc
    };

    class MulticastComponent {
    private:
        std::unique_ptr<MulticastComponentImpl> impl_;
    public:
        MulticastComponent(MulticastComponentTopicEncodingChoice choice = MulticastComponentTopicEncodingChoice::CBOR);
        MulticastComponent(MulticastComponent &&);
        MulticastComponent &operator=(MulticastComponent &&);
        ~MulticastComponent();
        //only host and port are needed in the locators
        struct NoTopicSelection {};
        uint32_t multicast_addSubscriptionClient(ConnectionLocator const &locator,
                        std::variant<NoTopicSelection, std::string, std::regex> const &topic,
                        std::function<void(basic::ByteDataWithTopic &&)> client,
                        std::optional<WireToUserHook> wireToUserHook = std::nullopt);
        void multicast_removeSubscriptionClient(uint32_t id);
        //the "int" parameter is the ttl
        std::function<void(basic::ByteDataWithTopic &&, int)> multicast_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
    };

} } } } }


#endif