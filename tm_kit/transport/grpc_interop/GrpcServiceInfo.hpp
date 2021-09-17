#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVICE_INFO_HPP_
#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVICE_INFO_HPP_

#include <tm_kit/basic/SerializationHelperMacros.hpp>

#include <iostream>
#include <sstream>
#include <variant>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {

    #define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SSL_CLIENT_INFO_FIELDS \
        ((std::string, caCertificateFile)) \
        ((std::string, clientCertificateFile)) \
        ((std::string, clientKeyFile))
    #define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SSL_SERVER_INFO_FIELDS \
        ((std::string, serverCertificateFile)) \
        ((std::string, serverKeyFile))

    TM_BASIC_CBOR_CAPABLE_STRUCT(GrpcSSLClientInfo, TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SSL_CLIENT_INFO_FIELDS);
    TM_BASIC_CBOR_CAPABLE_STRUCT(GrpcSSLServerInfo, TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SSL_SERVER_INFO_FIELDS);

    using GrpcSSLInfo = std::variant<
        std::monostate 
        , GrpcSSLClientInfo
        , GrpcSSLServerInfo
    >;

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

TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE_NO_FIELD_NAMES(dev::cd606::tm::transport::grpc_interop::GrpcSSLClientInfo, TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SSL_CLIENT_INFO_FIELDS);
TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE_NO_FIELD_NAMES(dev::cd606::tm::transport::grpc_interop::GrpcSSLServerInfo, TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SSL_SERVER_INFO_FIELDS);
TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE_NO_FIELD_NAMES(dev::cd606::tm::transport::grpc_interop::GrpcServiceInfo, TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVICE_INFO_FIELDS);

#undef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SSL_CLIENT_INFO_FIELDS
#undef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SSL_SERVER_INFO_FIELDS
#undef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVICE_INFO_FIELDS

#endif