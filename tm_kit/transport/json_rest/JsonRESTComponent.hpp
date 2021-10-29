#ifndef TM_KIT_TRANSPORT_JSON_REST_JSON_REST_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_JSON_REST_COMPONENT_HPP_

#include <tm_kit/transport/json_rest/JsonRESTComponentException.hpp>
#include <tm_kit/transport/ConnectionLocator.hpp>

#include <tm_kit/basic/SerializationHelperMacros.hpp>

#include <filesystem>

#define JSON_REST_COMPONENT_TOKEN_AUTHENTICATION_REQUEST_FIELDS \
    ((std::string, username)) \
    ((std::string, password))

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    TM_BASIC_CBOR_CAPABLE_STRUCT(TokenAuthenticationRequest, JSON_REST_COMPONENT_TOKEN_AUTHENTICATION_REQUEST_FIELDS);
    
    class JsonRESTComponentImpl;

    class JsonRESTComponent {
    private:
        std::unique_ptr<JsonRESTComponentImpl> impl_;
    public:
        JsonRESTComponent();
        JsonRESTComponent(JsonRESTComponent const &) = delete;
        JsonRESTComponent &operator=(JsonRESTComponent const &) = delete;
        JsonRESTComponent(JsonRESTComponent &&);
        JsonRESTComponent &operator=(JsonRESTComponent &&);
        virtual ~JsonRESTComponent();

        void addJsonRESTClient(ConnectionLocator const &locator, std::string &&urlQueryPart, std::string &&request, std::function<
            void(std::string &&)
        > const &clientCallback);
        void registerHandler(ConnectionLocator const &locator, std::function<
            bool(std::string const &login, std::string const &data, std::unordered_map<std::string, std::vector<std::string>> const &queryMap, std::function<void(std::string const &)> const &callback)
        > const &handler);
        //if password is std::nullopt, this login will be accepted with any password
        void addBasicAuthentication(int port, std::string const &login, std::optional<std::string> const &password);
        //for this API, the password is already salted and hashed
        void addBasicAuthentication_salted(int port, std::string const &login, std::string const &saltedPassword);
        void addTokenAuthentication(int port, std::string const &login, std::string const &password);
        void addTokenAuthentication_salted(int port, std::string const &login, std::string const &saltedPassword);
        void setDocRoot(int port, std::filesystem::path const &docRoot);
        void finalizeEnvironment();
        std::unordered_map<ConnectionLocator, std::thread::native_handle_type> json_rest_threadHandles();

        static const std::string TOKEN_AUTHENTICATION_REQUEST;
    };

} } } } }

TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::json_rest::TokenAuthenticationRequest, JSON_REST_COMPONENT_TOKEN_AUTHENTICATION_REQUEST_FIELDS);

#undef JSON_REST_COMPONENT_TOKEN_AUTHENTICATION_REQUEST_FIELDS

#endif