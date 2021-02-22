#ifndef TM_KIT_TRANSPORT_NNG_NNG_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_NNG_NNG_COMPONENT_HPP_

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

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace nng {
    
    class NNGComponentImpl;

    class NNGComponent {
    private:
        std::unique_ptr<NNGComponentImpl> impl_;
    public:
        NNGComponent();
        ~NNGComponent();
        //only host and port are needed in the locators
        struct NoTopicSelection {};
        uint32_t nng_addSubscriptionClient(ConnectionLocator const &locator,
                        std::variant<NoTopicSelection, std::string, std::regex> const &topic,
                        std::function<void(basic::ByteDataWithTopic &&)> client,
                        std::optional<WireToUserHook> wireToUserHook = std::nullopt);
        void nng_removeSubscriptionClient(uint32_t id);
        std::function<void(basic::ByteDataWithTopic &&)> nng_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
        std::unordered_map<ConnectionLocator, std::thread::native_handle_type> nng_threadHandles();
    };

} } } } }


#endif