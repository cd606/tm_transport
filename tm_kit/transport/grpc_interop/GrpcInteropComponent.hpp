#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_INTEROP_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_INTEROP_COMPONENT_HPP_

#include <memory>

#include <tm_kit/transport/grpc_interop/GrpcInteropComponentException.hpp>
#include <tm_kit/transport/ConnectionLocator.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>

namespace grpc {
    class Channel;
    class ServerBuilder;
    class Service;
}

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {

    class GrpcInteropComponentImpl;

    class GrpcInteropComponent {
    private:
        std::unique_ptr<GrpcInteropComponentImpl> impl_;
    public:
        GrpcInteropComponent();
        GrpcInteropComponent(GrpcInteropComponent const &) = delete;
        GrpcInteropComponent &operator=(GrpcInteropComponent const &) = delete;
        GrpcInteropComponent(GrpcInteropComponent &&);
        GrpcInteropComponent &operator=(GrpcInteropComponent &&);
        ~GrpcInteropComponent();
        std::shared_ptr<grpc::Channel> grpc_interop_getChannel(ConnectionLocator const &locator);
        void grpc_interop_registerService(ConnectionLocator const &locator, grpc::Service *);
        //The setRPCClient calls are NOT the preferred way to create 
        //RPC client facilities for grpc_interop. They are provided 
        //for compatibility with MultiTransportRemoteFacility
        std::function<void(basic::ByteDataWithID &&)> grpc_interop_setRPCClient(ConnectionLocator const &locator,
                        std::function<void(bool, std::optional<basic::ByteDataWithID> &&)> client,
                        std::optional<ByteDataHookPair> hookPair = std::nullopt);
        void grpc_interop_removeRPCClient(ConnectionLocator const &locator);
        void finalizeEnvironment();
    };
}}}}}

#endif