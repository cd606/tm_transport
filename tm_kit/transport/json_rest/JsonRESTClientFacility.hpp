#ifndef TM_KIT_TRANSPORT_JSON_REST_JSON_REST_CLIENT_FACILITY_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_JSON_REST_CLIENT_FACILITY_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/AppClassifier.hpp>

#include <tm_kit/transport/json_rest/JsonRESTComponent.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>

#include <type_traits>
#include <memory>
#include <iostream>
#include <sstream>
#include <mutex>
#include <condition_variable>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    template <class M, typename=std::enable_if_t<
        infra::app_classification_v<M> == infra::AppClassification::RealTime
        &&
        std::is_convertible_v<
            typename M::EnvironmentType *
            , JsonRESTComponent *
        >
    >>
    class JsonRESTClientFacilityFactory {
    private:
        using Env = typename M::EnvironmentType;
    public:
        template <class Req, class Resp>
        static auto createClientFacility(
            ConnectionLocator const &locator
        )
            -> std::shared_ptr<
                typename M::template OnOrderFacility<
                    Req, Resp
                >
            >
        {
            if constexpr (
                basic::nlohmann_json_interop::JsonWrappable<Req>::value
                &&
                basic::nlohmann_json_interop::JsonWrappable<Resp>::value
            ) {
                class LocalF : 
                    public M::IExternalComponent
                    , public M::template AbstractOnOrderFacility<Req, Resp> 
                    , public infra::RealTimeAppComponents<Env>::template ThreadedHandler<
                        typename M::template Key<Req>
                        , LocalF 
                    >
                {
                private:
                    ConnectionLocator locator_;
                public:
                    LocalF(ConnectionLocator const &locator)
                        : M::IExternalComponent()
                        , M::template AbstractOnOrderFacility<Req, Resp>()
                        , infra::RealTimeAppComponents<Env>::template ThreadedHandler<
                            typename M::template Key<Req>
                            , LocalF 
                        >()
                        , locator_(locator)
                    {
                        this->startThread();
                    }
                    virtual void start(Env *env) override final {
                    }
                    void actuallyHandle(typename M::template InnerData<typename M::template Key<Req>> &&req) {
                        auto id = req.timedData.value.id();
                        
                        nlohmann::json sendData;
                        basic::nlohmann_json_interop::JsonEncoder<Req>::write(sendData, "request", req.timedData.value.key());

                        auto *env = req.environment;
                        env->JsonRESTComponent::addJsonRESTClient(
                            locator_
                            , sendData.dump()
                            , [this,env,id=std::move(id)](std::string &&response) mutable {
                                nlohmann::json x = nlohmann::json::parse(response);
                                Resp resp;
                                basic::nlohmann_json_interop::Json<Resp *> r(&resp);
                                if (r.fromNlohmannJson(x["response"])) {
                                    this->publish(
                                        env 
                                        , typename M::template Key<Resp> {
                                            std::move(id)
                                            , std::move(resp)
                                        }
                                        , true
                                    );
                                }
                            }
                        );
                    }
                    void idleWork() {}
                    void waitForStart() {}
                };
                return M::template fromAbstractOnOrderFacility<Req,Resp>(new LocalF(locator));
            } else {
                throw JsonRESTComponentException("json rest facility only works when the data types are json-encodable");
            }
        }

        template <class Req, class Resp>
        static std::future<Resp> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, Req &&request) {
            if constexpr (
                basic::nlohmann_json_interop::JsonWrappable<Req>::value
                &&
                basic::nlohmann_json_interop::JsonWrappable<Resp>::value
            ) {
                if constexpr(DetermineClientSideIdentityForRequest<Env, Req>::HasIdentity) {
                    if constexpr (!std::is_same_v<typename DetermineClientSideIdentityForRequest<Env, Req>::IdentityType, std::string>) {
                        throw JsonRESTComponentException("json rest typed one shot remote call does not work when the request has non-string identity");
                    }
                } 
                auto ret = std::make_shared<std::promise<Resp>>();
                nlohmann::json sendData;
                basic::nlohmann_json_interop::JsonEncoder<Req>::write(sendData, "request", request);
                env->JsonRESTComponent::addJsonRESTClient(
                    rpcQueueLocator
                    , sendData.dump()
                    , [ret](std::string &&response) mutable {
                        nlohmann::json x = nlohmann::json::parse(response);
                        Resp resp;
                        basic::nlohmann_json_interop::Json<Resp *> r(&resp);
                        if (r.fromNlohmannJson(x["response"])) {
                            ret->set_value(std::move(resp));
                        }
                    }
                );
                return ret->get_future();
            } else {
                throw JsonRESTComponentException("json rest typed one shot remote call only works when the data types are json-encodable");
            }
        }
    };

}}}}}

#endif