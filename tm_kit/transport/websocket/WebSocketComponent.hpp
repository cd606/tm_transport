#ifndef TM_KIT_TRANSPORT_WEB_SOCKET_WEB_SOCKET_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_WEB_SOCKET_WEB_SOCKET_COMPONENT_HPP_

#include <tm_kit/transport/websocket/WebSocketComponentException.hpp>
#include <tm_kit/transport/ConnectionLocator.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace web_socket {

    class WebSocketComponentImpl;

    class WebSocketComponent {
    private:
        std::unique_ptr<WebSocketComponentImpl> impl_;
    public:
        WebSocketComponent();
        WebSocketComponent(WebSocketComponent const &) = delete;
        WebSocketComponent &operator=(WebSocketComponent const &) = delete;
        WebSocketComponent(WebSocketComponent &&);
        WebSocketComponent &operator=(WebSocketComponent &&);
        virtual ~WebSocketComponent();

        std::function<void(basic::ByteDataWithTopic &&, int)> websocket_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
        void finalizeEnvironment();
    };

} } } } }

#endif