#ifndef TM_KIT_TRANSPORT_JSON_REST_JSON_REST_SSE_EXPORTER_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_JSON_REST_SSE_EXPORTER_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/TraceNodesComponent.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/NlohmannJsonInterop.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>
#include <tm_kit/transport/json_rest/JsonRESTComponent.hpp>

#include <optional>
#include <type_traits>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    // The locator's port and identifier select the shared HTTP listener and SSE path.
    // The listener's bind address is configured on JsonRESTComponent per port.
    template <class Env, std::enable_if_t<std::is_base_of_v<JsonRESTComponent, Env>, int> = 0>
    class JsonRESTSSEExporter {
    public:
        using M = infra::RealTimeApp<Env>;

        static std::shared_ptr<typename M::template Exporter<basic::ByteDataWithTopic>> createExporter(
            ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt
        ) {
            class LocalE final : public M::template AbstractExporter<basic::ByteDataWithTopic> {
                ConnectionLocator locator_;
                std::optional<UserToWireHook> hook_;
                Env *env_ = nullptr;
            public:
                LocalE(ConnectionLocator const &locator, std::optional<UserToWireHook> hook)
                    : locator_(locator), hook_(std::move(hook)) {}
                void start(Env *env) override final {
                    env_ = env;
                    env_->registerSSEPublisher(locator_);
                }
                void handle(typename M::template InnerData<basic::ByteDataWithTopic> &&data) override final {
                    if (!env_) { return; }
                    TM_INFRA_EXPORTER_TRACER(env_);
                    auto value = std::move(data.timedData.value);
                    basic::ByteData payload {std::move(value.content)};
                    if (hook_) { payload = hook_->hook(std::move(payload)); }
                    auto event = locator_.query("event", value.topic.empty() ? "message" : value.topic);
                    env_->publishSSE(locator_, payload.content, event);
                }
            };
            return M::exporter(new LocalE(locator, std::move(hook)));
        }

        template <class T>
        static std::shared_ptr<typename M::template Exporter<basic::TypedDataWithTopic<T>>> createTypedExporter(
            ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt
        ) {
            static_assert(basic::nlohmann_json_interop::JsonWrappable<T>::value,
                "JsonRESTSSEExporter requires a JSON-wrappable payload type");
            class LocalE final : public M::template AbstractExporter<basic::TypedDataWithTopic<T>> {
                ConnectionLocator locator_;
                std::optional<UserToWireHook> hook_;
                Env *env_ = nullptr;
            public:
                LocalE(ConnectionLocator const &locator, std::optional<UserToWireHook> hook)
                    : locator_(locator), hook_(std::move(hook)) {}
                void start(Env *env) override final {
                    env_ = env;
                    env_->registerSSEPublisher(locator_);
                }
                void handle(typename M::template InnerData<basic::TypedDataWithTopic<T>> &&data) override final {
                    if (!env_) { return; }
                    TM_INFRA_EXPORTER_TRACER(env_);
                    auto &value = data.timedData.value;
                    basic::nlohmann_json_interop::Json<T const *> wrapper(&value.content);
                    nlohmann::json json;
                    wrapper.toNlohmannJson(json);
                    basic::ByteData payload {json.dump()};
                    if (hook_) { payload = hook_->hook(std::move(payload)); }
                    auto event = locator_.query("event", value.topic.empty() ? "message" : value.topic);
                    env_->publishSSE(locator_, payload.content, event);
                }
            };
            return M::exporter(new LocalE(locator, std::move(hook)));
        }
    };

} } } } }

#endif
