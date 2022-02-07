#ifndef TM_KIT_TRANSPORT_TLS_CONFIGURATION_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_TLS_CONFIGURATION_COMPONENT_HPP_

#include <unordered_map>
#include <string>

#include <tm_kit/infra/Environments.hpp>

#include <tm_kit/basic/SerializationHelperMacros.hpp>
#include <tm_kit/basic/StructFieldInfoUtils.hpp>
#include <tm_kit/basic/ConfigurationMapComponent.hpp>

namespace dev { namespace cd606 {namespace tm {namespace transport {

    #define TM_KIT_TRANSPORT_TLS_CLIENT_INFO_KEY_FIELDS \
        ((std::string, host)) \
        ((int, port)) 
    #define TM_KIT_TRANSPORT_TLS_SERVER_INFO_KEY_FIELDS \
        ((int, port)) 
    #define TM_KIT_TRANSPORT_TLS_CLIENT_INFO_FIELDS \
        ((std::string, caCertificateFile)) \
        ((std::string, clientCertificateFile)) \
        ((std::string, clientKeyFile))
    #define TM_KIT_TRANSPORT_TLS_SERVER_INFO_FIELDS \
        ((std::string, serverCertificateFile)) \
        ((std::string, serverKeyFile)) \
        ((std::vector<std::string>, rootCertificateFiles))

    TM_BASIC_CBOR_CAPABLE_STRUCT(TLSClientInfoKey, TM_KIT_TRANSPORT_TLS_CLIENT_INFO_KEY_FIELDS);
    TM_BASIC_CBOR_CAPABLE_STRUCT(TLSServerInfoKey, TM_KIT_TRANSPORT_TLS_SERVER_INFO_KEY_FIELDS);
    TM_BASIC_CBOR_CAPABLE_STRUCT(TLSClientInfo, TM_KIT_TRANSPORT_TLS_CLIENT_INFO_FIELDS);
    TM_BASIC_CBOR_CAPABLE_STRUCT(TLSServerInfo, TM_KIT_TRANSPORT_TLS_SERVER_INFO_FIELDS);

    using TLSClientConfigurationComponent = basic::ConfigurationMapComponent<
        TLSClientInfoKey, TLSClientInfo, basic::struct_field_info_utils::StructFieldInfoBasedHash<TLSClientInfoKey>
    >;
    using TLSServerConfigurationComponent = basic::ConfigurationMapComponent<
        TLSServerInfoKey, TLSServerInfo, basic::struct_field_info_utils::StructFieldInfoBasedHash<TLSServerInfoKey>
    >;
    using TLSConfigurationComponent = infra::Environment<
        TLSClientConfigurationComponent
        , TLSServerConfigurationComponent
    >;

    namespace tls {
        template <class Env, typename=std::enable_if_t<std::is_convertible_v<Env *, TLSClientConfigurationComponent *>>>
        inline void markConnectionAsUsingTLS(Env *env, std::string const &host, int port=443
            , std::string const &caCertFile="", std::string const &clientCertFile="", std::string const &clientKeyFile=""
        ) {
            static_cast<TLSClientConfigurationComponent *>(env)->setConfigurationItem(
                TLSClientInfoKey {host, port}
                , TLSClientInfo {caCertFile, clientCertFile, clientKeyFile}
            );
        }
    }

}}}}

TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::TLSClientInfoKey, TM_KIT_TRANSPORT_TLS_CLIENT_INFO_KEY_FIELDS);
TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::TLSServerInfoKey, TM_KIT_TRANSPORT_TLS_SERVER_INFO_KEY_FIELDS);
TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::TLSClientInfo, TM_KIT_TRANSPORT_TLS_CLIENT_INFO_FIELDS);
TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::TLSServerInfo, TM_KIT_TRANSPORT_TLS_SERVER_INFO_FIELDS);

#undef TM_KIT_TRANSPORT_TLS_CLIENT_INFO_KEY_FIELDS
#undef TM_KIT_TRANSPORT_TLS_SERVER_INFO_KEY_FIELDS
#undef TM_KIT_TRANSPORT_TLS_CLIENT_INFO_FIELDS
#undef TM_KIT_TRANSPORT_TLS_SERVER_INFO_FIELDS


#endif