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

        struct NoTopicSelection {};
        uint32_t websocket_addSubscriptionClient(ConnectionLocator const &locator,
                        std::variant<NoTopicSelection, std::string, std::regex> const &topic,
                        std::function<void(basic::ByteDataWithTopic &&)> client,
                        std::optional<WireToUserHook> wireToUserHook = std::nullopt,
                        std::vector<basic::ByteData> &&initialMessages = {},
                        std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> const &protocolReactor = {},
                        std::function<void()> const &protocolRestartReactor = {});
        void websocket_removeSubscriptionClient(uint32_t id);
        std::function<void(basic::ByteDataWithTopic &&)> websocket_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook = std::nullopt, std::function<std::function<std::optional<basic::ByteData>(basic::ByteDataView const &, std::atomic<bool> &)>()> const &protocolReactorFactory = {});
        std::function<void(basic::ByteDataWithID &&)> websocket_setRPCClient(ConnectionLocator const &locator,
                        std::function<void(bool, basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt,
                        uint32_t *clientNumberOutput = nullptr); //the return value is the requester
        void websocket_removeRPCClient(ConnectionLocator const &locator, uint32_t clientNumber);
        std::function<void(bool, basic::ByteDataWithID &&)> websocket_setRPCServer(ConnectionLocator const &locator,
                        std::function<void(basic::ByteDataWithID &&)> server,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the replier, where bool means whether it is the final reply
        void finalizeEnvironment();
        std::unordered_map<ConnectionLocator, std::thread::native_handle_type> websocket_threadHandles();
    };

} } } } }

#endif
