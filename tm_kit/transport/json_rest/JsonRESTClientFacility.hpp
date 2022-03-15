#ifndef TM_KIT_TRANSPORT_JSON_REST_JSON_REST_CLIENT_FACILITY_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_JSON_REST_CLIENT_FACILITY_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/AppClassifier.hpp>

#include <tm_kit/basic/StructFieldInfoBasedCsvUtils.hpp>

#include <tm_kit/transport/json_rest/JsonRESTComponent.hpp>
#include <tm_kit/transport/json_rest/RawString.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>

#include <type_traits>
#include <memory>
#include <iostream>
#include <sstream>
#include <mutex>
#include <condition_variable>
#include <iomanip>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    class JsonRESTClientFacilityFactoryUtils {
    private:
        static std::array<bool, 128> createUnreservedCharactersArray() {
            std::array<bool, 128> ret;
            for (int ii=0; ii<128; ++ii) {
                char c = (char) ii;
                ret[ii] = std::isalnum(c) || (c == '-') || (c == '.') || (c == '_') || (c == '~');
            }
            return ret;
        }
    public:
        static void urlEscape(std::ostream &os, std::string const &input) {
            static std::array<bool,128> unreservedCharacters = createUnreservedCharactersArray();
            for (char c : input) {
                if (c == ' ') {
                    os << '+';
                } else if ((int) c < 0 || (int) c >= 128) {
                    os << '%' << std::setw(2) << std::setfill('0') << std::hex << std::uppercase << (unsigned) c;
                } else if (!unreservedCharacters[(int) c]) {
                    os << '%' << std::setw(2) << std::setfill('0') << std::hex << std::uppercase << (unsigned) c;
                } else {
                    os << c;
                }
            }
        }
    };

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
                    bool useGet_;
                    bool simplePost_;
                    bool noRequestResponseWrap_;
                public:
                    LocalF(ConnectionLocator const &locator)
                        : M::IExternalComponent()
                        , M::template AbstractOnOrderFacility<Req, Resp>()
                        , infra::RealTimeAppComponents<Env>::template ThreadedHandler<
                            typename M::template Key<Req>
                            , LocalF 
                        >()
                        , locator_(locator)
                        , useGet_(
                            (locator.query("use_get", "false") == "true")
                            && 
                            basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<Req>
                        )
                        , simplePost_(
                            (locator.query("simple_post", "false") == "true")
                            && 
                            basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<Req>
                        )
                        , noRequestResponseWrap_(
                            locator.query("no_wrap", "false") == "true"
                        )
                    {
                        this->startThread();
                    }
                    virtual void start(Env *env) override final {
                    }
                    void actuallyHandle(typename M::template InnerData<typename M::template Key<Req>> &&req) {
                        auto id = req.timedData.value.id();

                        nlohmann::json sendData;
                        std::ostringstream oss;
                        if constexpr (basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<Req> || std::is_empty_v<Req>) {
                            if (useGet_ || simplePost_) {
                                if constexpr (!std::is_empty_v<Req>) {
                                    bool start = true;
                                    basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvOutput<Req>
                                        ::outputNameValuePairs(
                                            req.timedData.value.key()
                                            , [&start,&oss](std::string const &name, std::string const &value) {
                                                if (!start) {
                                                    oss << '&';
                                                }
                                                JsonRESTClientFacilityFactoryUtils::urlEscape(oss, name);
                                                oss << '=';
                                                JsonRESTClientFacilityFactoryUtils::urlEscape(oss, value);
                                                start = false;
                                            }
                                        );
                                }
                            } else {
                                basic::nlohmann_json_interop::JsonEncoder<Req>::write(sendData, (noRequestResponseWrap_?std::nullopt:std::optional<std::string> {"request"}), req.timedData.value.key());
                            }
                        } else {
                            basic::nlohmann_json_interop::JsonEncoder<Req>::write(sendData, (noRequestResponseWrap_?std::nullopt:std::optional<std::string> {"request"}), req.timedData.value.key());
                        }

                        auto *env = req.environment;
                        env->JsonRESTComponent::addJsonRESTClient(
                            locator_
                            , (useGet_?oss.str():"")
                            , (useGet_?"":(simplePost_?oss.str():sendData.dump()))
                            , [this,env,id=std::move(id)](std::string &&response) mutable {
                                if constexpr (std::is_same_v<Resp, RawString> || std::is_same_v<Resp, basic::ByteData>) {
                                    this->publish(
                                        env 
                                        , typename M::template Key<Resp> {
                                            std::move(id)
                                            , {std::move(response)}
                                        }
                                        , true
                                    );
                                } else {
                                    try {
                                        nlohmann::json x = nlohmann::json::parse(response);
                                        Resp resp;
                                        basic::nlohmann_json_interop::Json<Resp *> r(&resp);
                                        if (r.fromNlohmannJson(noRequestResponseWrap_?x:x["response"])) {
                                            this->publish(
                                                env 
                                                , typename M::template Key<Resp> {
                                                    std::move(id)
                                                    , std::move(resp)
                                                }
                                                , true
                                            );
                                        }
                                    } catch (...) {
                                        env->log(infra::LogLevel::Error, "Cannot parse reply string '"+response+"' as json");
                                    }
                                }
                            }
                            , (simplePost_?"x-www-form-urlencoded":"application/json")
                            , useGet_
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
                bool useGet = (
                    (rpcQueueLocator.query("use_get", "false") == "true")
                    && 
                    basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<Req>
                );
                bool simplePost = (
                    (rpcQueueLocator.query("simple_post", "false") == "true")
                    && 
                    basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<Req>
                );
                bool noRequestResponseWrap = (
                    rpcQueueLocator.query("no_wrap", "false") == "true"
                );
                auto ret = std::make_shared<std::promise<Resp>>();
                nlohmann::json sendData;
                std::ostringstream oss;
                if constexpr (basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<Req> || std::is_empty_v<Req>) {
                    if (useGet || simplePost) {
                        if constexpr (!std::is_empty_v<Req>) {
                            bool start = true;
                            basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvOutput<Req>
                                ::outputNameValuePairs(
                                    request
                                    , [&start,&oss](std::string const &name, std::string const &value) {
                                        if (!start) {
                                            oss << '&';
                                        }
                                        JsonRESTClientFacilityFactoryUtils::urlEscape(oss, name);
                                        oss << '=';
                                        JsonRESTClientFacilityFactoryUtils::urlEscape(oss, value);
                                        start = false;
                                    }
                                );
                        }
                    } else {
                        basic::nlohmann_json_interop::JsonEncoder<Req>::write(sendData, (noRequestResponseWrap?std::nullopt:std::optional<std::string> {"request"}), request);
                    }
                } else {
                    basic::nlohmann_json_interop::JsonEncoder<Req>::write(sendData, (noRequestResponseWrap?std::nullopt:std::optional<std::string> {"request"}), request);
                }
                env->JsonRESTComponent::addJsonRESTClient(
                    rpcQueueLocator
                    , (useGet?oss.str():"")
                    , (useGet?"":(simplePost?oss.str():sendData.dump()))
                    , [ret,env,noRequestResponseWrap](std::string &&response) mutable {
                        if constexpr (std::is_same_v<Resp, RawString> || std::is_same_v<Resp, basic::ByteData>) {
                            ret->set_value({std::move(response)});
                        } else {
                            try {
                                nlohmann::json x = nlohmann::json::parse(response);
                                Resp resp;
                                basic::nlohmann_json_interop::Json<Resp *> r(&resp);
                                if (r.fromNlohmannJson(noRequestResponseWrap?x:x["response"])) {
                                    ret->set_value(std::move(resp));
                                }
                            } catch (...) {
                                env->log(infra::LogLevel::Error, "Cannot parse reply string '"+response+"' as json");
                            }
                        }
                    }
                    , (simplePost?"x-www-form-urlencoded":"application/json")
                    , useGet
                );
                return ret->get_future();
            } else {
                throw JsonRESTComponentException("json rest typed one shot remote call only works when the data types are json-encodable");
            }
        }
    };

}}}}}

#endif