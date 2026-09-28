#ifndef TM_KIT_TRANSPORT_JSON_REST_JSON_REST_SSE_PUBLISHER_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_JSON_REST_SSE_PUBLISHER_HPP_

#include <tm_kit/transport/json_rest/JsonRESTComponent.hpp>
#include <tm_kit/infra/RealTimeApp.hpp>

#include <type_traits>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    // The sink accepts an already encoded payload, such as a JSON string.
    template <class R>
    class JsonRESTSSEPublisher {
    public:
        using M = typename R::AppType;
        using Env = typename R::EnvironmentType;

        static auto create(R &r, std::string const &name, ConnectionLocator const &locator,
                           std::string const &eventName="message") -> typename R::template Sink<std::string> {
            static_assert(std::is_convertible_v<Env *, JsonRESTComponent *>,
                "JsonRESTSSEPublisher requires JsonRESTComponent in the environment");
            auto *component = static_cast<JsonRESTComponent *>(r.environment());
            component->registerSSEPublisher(locator);
            auto exporter = M::template pureExporter<std::string>(
                [component, locator, eventName](std::string &&payload) {
                    component->publishSSE(locator, payload, eventName);
                }
            );
            r.registerExporter(name, exporter);
            return r.exporterAsSink(exporter);
        }
    };

} } } } }

#endif
