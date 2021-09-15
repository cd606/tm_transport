#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_INTEROP_COMPONENT_EXCEPTION_HPP_
#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_INTEROP_COMPONENT_EXCEPTION_HPP_

#include <exception>
#include <stdexcept>
#include <string>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {

    class GrpcInteropComponentException : public std::runtime_error {
    public:
        GrpcInteropComponentException(std::string const &info) : std::runtime_error(info) {}
    };

} } } } }

#endif