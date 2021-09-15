#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVICE_INFO_HPP_
#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVICE_INFO_HPP_

#include <tm_kit/basic/SerializationHelperMacros.hpp>

#include <iostream>
#include <sstream>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {

    #define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVICE_INFO_FIELDS \
        ((std::string, namespaceName)) \
        ((std::string, serviceName)) \
        ((std::string, methodName)) \
        ((bool, isSingleRpcCall))

    TM_BASIC_CBOR_CAPABLE_STRUCT(GrpcServiceInfo, TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVICE_INFO_FIELDS);

    inline std::string grpcServiceInfoAsEndPointString(GrpcServiceInfo const &info) {
        std::ostringstream oss;
        oss << '/' << info.namespaceName << '.' << info.serviceName << '/' << info.methodName;
        return oss.str();
    }

}}}}}

TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE_NO_FIELD_NAMES(dev::cd606::tm::transport::grpc_interop::GrpcServiceInfo, TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVICE_INFO_FIELDS);

#undef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVICE_INFO_FIELDS

#endif