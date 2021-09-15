#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_CONNECTION_LOCATOR_UTILS_HPP_
#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_CONNECTION_LOCATOR_UTILS_HPP_

#include <tm_kit/transport/grpc_interop/GrpcServiceInfo.hpp>
#include <tm_kit/transport/ConnectionLocator.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {

    namespace connection_locator_utils {
        extern GrpcServiceInfo parseServiceInfo(ConnectionLocator const &l);
    }

} } } } }

#endif