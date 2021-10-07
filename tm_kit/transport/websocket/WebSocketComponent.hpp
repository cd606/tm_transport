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

        std::function<void(basic::ByteDataWithTopic &&)> websocket_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt);
        std::function<void(bool, basic::ByteDataWithID &&)> websocket_setRPCServer(ConnectionLocator const &locator,
                        std::function<void(basic::ByteDataWithID &&)> server,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the replier, where bool means whether it is the final reply
        void finalizeEnvironment();
    };

} } } } }

#endif