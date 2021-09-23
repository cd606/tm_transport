#ifndef TM_KIT_TRANSPORT_JSON_REST_JSON_REST_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_JSON_REST_COMPONENT_HPP_

#include <tm_kit/transport/json_rest/JsonRESTComponentException.hpp>
#include <tm_kit/transport/ConnectionLocator.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

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

        void registerHandler(ConnectionLocator const &locator, std::function<
            bool(std::string const &data, std::function<void(std::string const &)> const &callback)
        > const &handler);
        void finalizeEnvironment();
    };

} } } } }

#endif