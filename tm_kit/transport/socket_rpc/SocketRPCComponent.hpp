#ifndef TM_KIT_TRANSPORT_SOCKET_RPC_SOCKET_RPC_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_SOCKET_RPC_SOCKET_RPC_COMPONENT_HPP_

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

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace socket_rpc {
    
    class SocketRPCComponentImpl;

    class SocketRPCComponentException : public std::runtime_error {
    public:
        SocketRPCComponentException(std::string const &info) : std::runtime_error(info) {}
    };

    class SocketRPCComponent {
    private:
        std::unique_ptr<SocketRPCComponentImpl> impl_;
    public:
        SocketRPCComponent();
        ~SocketRPCComponent();
        SocketRPCComponent(SocketRPCComponent &&);
        SocketRPCComponent &operator=(SocketRPCComponent &&);
        //for RPC client, only host and port are required in the locator
        std::function<void(basic::ByteDataWithID &&)> socket_rpc_setRPCClient(ConnectionLocator const &locator,
                        std::function<void(bool, basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the requester
        void socket_rpc_removeRPCClient(ConnectionLocator const &locator);
        //for RPC server, only port is required in the locator
        std::function<void(bool, basic::ByteDataWithID &&)> socket_rpc_setRPCServer(ConnectionLocator const &locator,
                        std::function<void(basic::ByteDataWithID &&)> server,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt); //the return value is the replier, where bool means whether it is the final reply
        std::thread::native_handle_type socket_rpc_threadHandle();
    };

} } } } }

#endif