#ifndef TM_KIT_TRANSPORT_SHARED_MEMORY_BROADCAST_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_SHARED_MEMORY_BROADCAST_COMPONENT_HPP_

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

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace shared_memory_broadcast {
    
    class SharedMemoryBroadcastComponentImpl;

    class SharedMemoryBroadcastComponent {
    private:
        std::unique_ptr<SharedMemoryBroadcastComponentImpl> impl_;
    public:
        SharedMemoryBroadcastComponent();
        ~SharedMemoryBroadcastComponent();
        //only host and port are needed in the locators
        struct NoTopicSelection {};
        uint32_t shared_memory_broadcast_addSubscriptionClient(ConnectionLocator const &locator,
                        std::variant<NoTopicSelection, std::string, std::regex> const &topic,
                        std::function<void(basic::ByteDataWithTopic &&)> client,
                        std::optional<WireToUserHook> wireToUserHook = std::nullopt);
        void shared_memory_broadcast_removeSubscriptionClient(uint32_t id);
        std::function<void(basic::ByteDataWithTopic &&)> shared_memory_broadcast_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
    };

} } } } }


#endif