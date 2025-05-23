#ifndef TM_KIT_TRANSPORT_JSON_REST_JSON_REST_CLIENT_FACILITY_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_JSON_REST_CLIENT_FACILITY_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/AppClassifier.hpp>

#include <tm_kit/basic/StructFieldInfoBasedCsvUtils.hpp>

#include <tm_kit/transport/json_rest/JsonRESTComponent.hpp>
#include <tm_kit/transport/json_rest/RawString.hpp>
#include <tm_kit/transport/json_rest/ComplexInput.hpp>
#include <tm_kit/transport/json_rest/JsonRESTClientFacilityUtils.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>

#include <type_traits>
#include <memory>
#include <iostream>
#include <sstream>
#include <mutex>
#include <condition_variable>
#include <iomanip>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    namespace helper {
        template <class X>
        class IsEmpty {
        public:
            static constexpr bool value = std::is_empty_v<X>;
        };
        template <class X>
        class IsEmpty<basic::SingleLayerWrapper<X>> {
        public:
            static constexpr bool value = IsEmpty<X>::value;
        }; 
        template <int32_t ID, class X>
        class IsEmpty<basic::SingleLayerWrapperWithID<ID, X>> {
        public:
            static constexpr bool value = IsEmpty<X>::value;
        };
        template <class Mark, class X>
        class IsEmpty<basic::SingleLayerWrapperWithTypeMark<Mark, X>> {
        public:
            static constexpr bool value = IsEmpty<X>::value;
        };
        template <class X>
        class IsEmpty<basic::nlohmann_json_interop::Json<X>> {
        public:
            static constexpr bool value = IsEmpty<X>::value;
        };
        template <class X>
        class IsEmpty<basic::nlohmann_json_interop::Json<X *>> {
        public:
            static constexpr bool value = IsEmpty<X>::value;
        };
        template <class X>
        class IsEmpty<basic::nlohmann_json_interop::Json<X const *>> {
        public:
            static constexpr bool value = IsEmpty<X>::value;
        };
        template <class X>
        inline constexpr bool IsEmptyV = IsEmpty<X>::value;

        template <class X>
        class IsCsv {
        public:
            static constexpr bool value = basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<X>;
            using RealType = X;
            static RealType const &access(X const &x) {
                return x;
            }
        };
        template <class X>
        class IsCsv<basic::SingleLayerWrapper<X>> {
        public:
            static constexpr bool value = IsCsv<X>::value;
            using RealType = typename IsCsv<X>::RealType;
            static RealType const &access(basic::SingleLayerWrapper<X> const &x) {
                return IsCsv<X>::access(x.value);
            }
        }; 
        template <int32_t ID, class X>
        class IsCsv<basic::SingleLayerWrapperWithID<ID, X>> {
        public:
            static constexpr bool value = IsCsv<X>::value;
            using RealType = typename IsCsv<X>::RealType;
            static RealType const &access(basic::SingleLayerWrapperWithID<ID, X> const &x) {
                return IsCsv<X>::access(x.value);
            }
        };
        template <class Mark, class X>
        class IsCsv<basic::SingleLayerWrapperWithTypeMark<Mark, X>> {
        public:
            static constexpr bool value = IsCsv<X>::value;
            using RealType = typename IsCsv<X>::RealType;
            static RealType const &access(basic::SingleLayerWrapperWithTypeMark<Mark, X> const &x) {
                return IsCsv<X>::access(x.value);
            }
        };
        template <class X>
        class IsCsv<basic::nlohmann_json_interop::Json<X>> {
        public:
            static constexpr bool value = IsCsv<X>::value;
            using RealType = typename IsCsv<X>::RealType;
            static RealType const &access(basic::nlohmann_json_interop::Json<X> const &x) {
                return IsCsv<X>::access(x.value());
            }
        };
        template <class X>
        class IsCsv<basic::nlohmann_json_interop::Json<X *>> {
        public:
            static constexpr bool value = IsCsv<X>::value;
            using RealType = typename IsCsv<X>::RealType;
            static RealType const &access(basic::nlohmann_json_interop::Json<X *> const &x) {
                return IsCsv<X>::access(x.value());
            }
        };
        template <class X>
        class IsCsv<basic::nlohmann_json_interop::Json<X const *>> {
        public:
            static constexpr bool value = IsCsv<X>::value;
            using RealType = typename IsCsv<X>::RealType;
            static RealType const &access(basic::nlohmann_json_interop::Json<X const *> const &x) {
                return IsCsv<X>::access(x.value());
            }
        };
        template <class X>
        inline constexpr bool IsCsvV = IsCsv<X>::value;
    }
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
        static auto createComplexInputClientFacility(
            ConnectionLocator const &locator
        )
            -> std::shared_ptr<
                typename M::template OnOrderFacility<
                    json_rest::ComplexInput, json_rest::RawStringWithStatus
                >
            >
        {
            class LocalF : 
                public M::IExternalComponent
                , public M::template AbstractOnOrderFacility<json_rest::ComplexInput, json_rest::RawStringWithStatus> 
                , public infra::RealTimeAppComponents<Env>::template ThreadedHandler<
                    typename M::template Key<json_rest::ComplexInput>
                    , LocalF 
                >
            {
            private:
                ConnectionLocator locator_;
            public:
                LocalF(ConnectionLocator const &locator)
                    : M::IExternalComponent()
                    , M::template AbstractOnOrderFacility<json_rest::ComplexInput, json_rest::RawStringWithStatus>()
                    , infra::RealTimeAppComponents<Env>::template ThreadedHandler<
                        typename M::template Key<json_rest::ComplexInput>
                        , LocalF 
                    >()
                    , locator_(locator.addProperty("parse_header", "true"))
                {
                    this->startThread();
                }
                virtual void start(Env *env) override final {
                }
                void actuallyHandle(typename M::template InnerData<typename M::template Key<json_rest::ComplexInput>> &&req) {
                    auto id = req.timedData.value.id();

                    std::ostringstream oss;
                    auto const &input = req.timedData.value.key();
                    
                    auto *env = req.environment;
                    ConnectionLocator updatedLocator = locator_.modifyIdentifier(input.path);
                    std::unordered_map<std::string, std::string> locatorHeaderProps;
                    if (!input.headers.empty()) {
                        for (auto const &item : input.headers) {
                            locatorHeaderProps.insert(std::make_pair("header/"+item.first, item.second));
                        }
                    }
                    if (input.auth_token) {
                        locatorHeaderProps.insert(std::make_pair("auth_token", *(input.auth_token)));
                    }
                    if (!locatorHeaderProps.empty()) {
                        updatedLocator = updatedLocator.addProperties(locatorHeaderProps);
                    }
                    
                    env->JsonRESTComponent::addJsonRESTClient(
                        updatedLocator
                        , std::string {input.query}
                        , std::string {input.body}
                        , [this,env,id=std::move(id)](unsigned status, std::string &&response, std::unordered_map<std::string,std::string> &&headerFields) mutable {
                            this->publish(
                                env 
                                , typename M::template Key<json_rest::RawStringWithStatus> {
                                    std::move(id)
                                    , {status, std::move(headerFields), std::move(response)}
                                }
                                , true
                            );
                        }
                        , input.contentType
                        , input.method
                    );
                }
                void idleWork() {}
                void waitForStart() {}
            };
            return M::template fromAbstractOnOrderFacility<json_rest::ComplexInput, json_rest::RawStringWithStatus>(new LocalF(locator));
        }
        template <class T>
        static auto createComplexInputWithDataClientFacility(
            ConnectionLocator const &locator
        )
            -> std::shared_ptr<
                typename M::template OnOrderFacility<
                    json_rest::ComplexInputWithData<T>, json_rest::RawStringWithStatus
                >
            >
        {
            class LocalF : 
                public M::IExternalComponent
                , public M::template AbstractOnOrderFacility<json_rest::ComplexInputWithData<T>, json_rest::RawStringWithStatus> 
                , public infra::RealTimeAppComponents<Env>::template ThreadedHandler<
                    typename M::template Key<json_rest::ComplexInputWithData<T>>
                    , LocalF 
                >
            {
            private:
                ConnectionLocator locator_;
            public:
                LocalF(ConnectionLocator const &locator)
                    : M::IExternalComponent()
                    , M::template AbstractOnOrderFacility<json_rest::ComplexInputWithData<T>, json_rest::RawStringWithStatus>()
                    , infra::RealTimeAppComponents<Env>::template ThreadedHandler<
                        typename M::template Key<json_rest::ComplexInputWithData<T>>
                        , LocalF 
                    >()
                    , locator_(locator.addProperty("parse_header", "true"))
                {
                    this->startThread();
                }
                virtual void start(Env *env) override final {
                }
                void actuallyHandle(typename M::template InnerData<typename M::template Key<json_rest::ComplexInputWithData<T>>> &&req) {
                    auto id = req.timedData.value.id();

                    std::ostringstream oss;
                    auto const &input = req.timedData.value.key();
                    
                    auto *env = req.environment;
                    ConnectionLocator updatedLocator = locator_.modifyIdentifier(input.path);
                    std::unordered_map<std::string, std::string> locatorHeaderProps;
                    if (!input.headers.empty()) {
                        for (auto const &item : input.headers) {
                            locatorHeaderProps.insert(std::make_pair("header/"+item.first, item.second));
                        }
                    }
                    if (input.auth_token) {
                        locatorHeaderProps.insert(std::make_pair("auth_token", *(input.auth_token)));
                    }
                    if (!locatorHeaderProps.empty()) {
                        updatedLocator = updatedLocator.addProperties(locatorHeaderProps);
                    }
                    
                    env->JsonRESTComponent::addJsonRESTClient(
                        updatedLocator
                        , std::string {input.query}
                        , std::string {input.body}
                        , [this,env,id=std::move(id)](unsigned status, std::string &&response, std::unordered_map<std::string,std::string> &&headerFields) mutable {
                            this->publish(
                                env 
                                , typename M::template Key<json_rest::RawStringWithStatus> {
                                    std::move(id)
                                    , {status, std::move(headerFields), std::move(response)}
                                }
                                , true
                            );
                        }
                        , input.contentType
                        , input.method
                    );
                }
                void idleWork() {}
                void waitForStart() {}
            };
            return M::template fromAbstractOnOrderFacility<json_rest::ComplexInputWithData<T>, json_rest::RawStringWithStatus>(new LocalF(locator));
        }
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
                std::is_same_v<Req, json_rest::ComplexInput>
                &&
                std::is_same_v<Resp, json_rest::RawStringWithStatus>
            ) {
                return createComplexInputClientFacility(locator);
            } else if constexpr (
                json_rest::IsComplexInputWithData<Req>::value
                &&
                std::is_same_v<Resp, json_rest::RawStringWithStatus>
            ) {
                return createComplexInputWithDataClientFacility<
                    typename json_rest::IsComplexInputWithData<Req>::DataType
                >(locator);
            } else if constexpr (
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
                    std::string httpMethod_;
                    bool useGet_;
                    bool simplePost_;
                    bool otherwiseEncodeInUrl_;
                    bool noRequestResponseWrap_;
                    std::string useThisPathForResponse_;
                public:
                    LocalF(ConnectionLocator const &locator)
                        : M::IExternalComponent()
                        , M::template AbstractOnOrderFacility<Req, Resp>()
                        , infra::RealTimeAppComponents<Env>::template ThreadedHandler<
                            typename M::template Key<Req>
                            , LocalF 
                        >()
                        , locator_(std::is_same_v<Resp, json_rest::RawStringWithStatus>?locator.addProperty("parse_header", "true"):locator)
                        , httpMethod_(locator.query("http_method", ""))
                        , useGet_(
                            (locator.query("use_get", "false") == "true")
                            && 
                            (helper::IsCsvV<Req> || helper::IsEmptyV<Req>)
                        )
                        , simplePost_(
                            (locator.query("simple_post", "false") == "true")
                            && 
                            (helper::IsCsvV<Req> || helper::IsEmptyV<Req>)
                        )
                        , otherwiseEncodeInUrl_(
                            (locator.query("no_query_body", "false") == "true")
                            && 
                            (helper::IsCsvV<Req> || helper::IsEmptyV<Req>)
                        )
                        , noRequestResponseWrap_(
                            locator.query("no_wrap", "false") == "true"
                        )
                        , useThisPathForResponse_(
                            locator.query("response_path", "")
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
                        if constexpr (helper::IsCsvV<Req> || helper::IsEmptyV<Req>) {
                            if (useGet_ || simplePost_ || otherwiseEncodeInUrl_) {
                                if constexpr (!helper::IsEmptyV<Req>) {
                                    bool start = true;
                                    basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvOutput<typename helper::IsCsv<Req>::RealType>
                                        ::outputNameValuePairs(
                                            helper::IsCsv<Req>::access(req.timedData.value.key())
                                            , [&start,&oss](std::string const &name, std::string const &value) {
                                                if (!start) {
                                                    oss << '&';
                                                }
                                                JsonRESTClientFacilityFactoryUtils::urlEscape(oss, name);
                                                oss << '=';
                                                JsonRESTClientFacilityFactoryUtils::urlEscape(oss, value);
                                                start = false;
                                            }
                                            , true
                                        );
                                }
                            } else {
                                basic::nlohmann_json_interop::JsonEncoder<Req>::write(sendData, (noRequestResponseWrap_?std::nullopt:std::optional<std::string> {"request"}), req.timedData.value.key());
                            }
                        } else {
                            basic::nlohmann_json_interop::JsonEncoder<Req>::write(sendData, (noRequestResponseWrap_?std::nullopt:std::optional<std::string> {"request"}), req.timedData.value.key());
                        }

                        std::optional<std::string> methodParam = std::nullopt;
                        if (httpMethod_ != "") {
                            methodParam = httpMethod_;
                        } else if (useGet_) {
                            methodParam = "GET";
                        }

                        auto *env = req.environment;
                        env->JsonRESTComponent::addJsonRESTClient(
                            locator_
                            , ((useGet_||otherwiseEncodeInUrl_)?oss.str():"")
                            , ((useGet_||otherwiseEncodeInUrl_)?"":(simplePost_?oss.str():sendData.dump()))
                            , [this,env,id=std::move(id)](unsigned status, std::string &&response, std::unordered_map<std::string,std::string> &&headerFields) mutable {
                                if constexpr (std::is_same_v<Resp, RawString> || std::is_same_v<Resp, basic::ByteData>) {
                                    this->publish(
                                        env 
                                        , typename M::template Key<Resp> {
                                            std::move(id)
                                            , {std::move(response)}
                                        }
                                        , true
                                    );
                                } else if constexpr (std::is_same_v<Resp, RawStringWithStatus>) {
                                    this->publish(
                                        env 
                                        , typename M::template Key<Resp> {
                                            std::move(id)
                                            , {status, std::move(headerFields), std::move(response)}
                                        }
                                        , true
                                    );
                                } else {
                                    try {
                                        nlohmann::json x = nlohmann::json::parse(response);
                                        Resp resp;
                                        basic::nlohmann_json_interop::Json<Resp *> r(&resp);
                                        if (useThisPathForResponse_ != "") {
                                            std::vector<std::string> parts;
                                            boost::split(parts, useThisPathForResponse_, boost::is_any_of("/"));
                                            nlohmann::json x1 = x;
                                            for (auto const &p : parts) {
                                                if (p != "") {
                                                    x1 = x1[p];
                                                }
                                            }
                                            if (r.fromNlohmannJson(x1)) {
                                                this->publish(
                                                    env 
                                                    , typename M::template Key<Resp> {
                                                        std::move(id)
                                                        , std::move(resp)
                                                    }
                                                    , true
                                                );
                                            }
                                        } else {
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
                                        }
                                    } catch (...) {
                                        env->log(infra::LogLevel::Error, "Cannot parse reply string '"+response+"' as json");
                                    }
                                }
                            }
                            , (simplePost_?"application/x-www-form-urlencoded":"application/json")
                            , methodParam
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

        template <class ComplexInputKindRequest>
        static std::future<json_rest::RawStringWithStatus> typedOneShotComplexInputRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, ComplexInputKindRequest &&request) {            
            ConnectionLocator updatedLocator = rpcQueueLocator.modifyIdentifier(request.path);
            std::unordered_map<std::string, std::string> locatorHeaderProps;
            if (!request.headers.empty()) {
                for (auto const &item : request.headers) {
                    locatorHeaderProps.insert(std::make_pair("header/"+item.first, item.second));
                }
            }
            if (request.auth_token) {
                locatorHeaderProps.insert(std::make_pair("auth_token", *(request.auth_token)));
            }
            if (!locatorHeaderProps.empty()) {
                updatedLocator = updatedLocator.addProperties(locatorHeaderProps);
            }

            auto ret = std::make_shared<std::promise<json_rest::RawStringWithStatus>>(); 
            env->JsonRESTComponent::addJsonRESTClient(
                updatedLocator
                , std::string {request.query}
                , std::string {request.body}
                , [ret,env](unsigned status, std::string &&response, std::unordered_map<std::string,std::string> &&headerFields) mutable {
                    ret->set_value({status, std::move(headerFields), std::move(response)});
                }
                , request.contentType
                , request.method
            );
            return ret->get_future();
        }

        template <class Req, class Resp>
        static std::future<Resp> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, Req &&request) {
            if constexpr (
                std::is_same_v<Req, json_rest::ComplexInput>
                &&
                std::is_same_v<Resp, json_rest::RawStringWithStatus>
            ) {
                return typedOneShotComplexInputRemoteCall<Req>(env, rpcQueueLocator, std::move(request));
            } else if constexpr (
                json_rest::IsComplexInputWithData<Req>::value
                &&
                std::is_same_v<Resp, json_rest::RawStringWithStatus>
            ) {
                return typedOneShotComplexInputRemoteCall<Req>(env, rpcQueueLocator, std::move(request));
            }

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
                std::string httpMethod = rpcQueueLocator.query("http_method", "");
                bool useGet = (
                    (rpcQueueLocator.query("use_get", "false") == "true")
                    && 
                    (helper::IsCsvV<Req> || helper::IsEmptyV<Req>)
                );
                bool simplePost = (
                    (rpcQueueLocator.query("simple_post", "false") == "true")
                    && 
                    (helper::IsCsvV<Req> || helper::IsEmptyV<Req>)
                );
                bool otherwiseEncodeInUrl = (
                    (rpcQueueLocator.query("no_query_body", "false") == "true")
                    && 
                    (helper::IsCsvV<Req> || helper::IsEmptyV<Req>)
                );
                bool noRequestResponseWrap = (
                    rpcQueueLocator.query("no_wrap", "false") == "true"
                );
                std::string useThisPathForResponse = 
                    rpcQueueLocator.query("response_path", "");
                
                auto ret = std::make_shared<std::promise<Resp>>();
                nlohmann::json sendData;
                std::ostringstream oss;
                if constexpr (helper::IsCsvV<Req> || helper::IsEmptyV<Req>) {
                    if (useGet || simplePost || otherwiseEncodeInUrl) {
                        if constexpr (!helper::IsEmptyV<Req>) {
                            bool start = true;
                            basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvOutput<typename helper::IsCsv<Req>::RealType>
                                ::outputNameValuePairs(
                                    helper::IsCsv<Req>::access(request)
                                    , [&start,&oss](std::string const &name, std::string const &value) {
                                        if (!start) {
                                            oss << '&';
                                        }
                                        JsonRESTClientFacilityFactoryUtils::urlEscape(oss, name);
                                        oss << '=';
                                        JsonRESTClientFacilityFactoryUtils::urlEscape(oss, value);
                                        start = false;
                                    }
                                    , true
                                );
                        }
                    } else {
                        basic::nlohmann_json_interop::JsonEncoder<Req>::write(sendData, (noRequestResponseWrap?std::nullopt:std::optional<std::string> {"request"}), request);
                    }
                } else {
                    basic::nlohmann_json_interop::JsonEncoder<Req>::write(sendData, (noRequestResponseWrap?std::nullopt:std::optional<std::string> {"request"}), request);
                }
                std::optional<std::string> methodParam = std::nullopt;
                if (httpMethod != "") {
                    methodParam = httpMethod;
                } else if (useGet) {
                    methodParam = "GET";
                }
                env->JsonRESTComponent::addJsonRESTClient(
                    (std::is_same_v<Resp, RawStringWithStatus>?rpcQueueLocator.addProperty("parse_header", "true"):rpcQueueLocator)
                    , ((useGet||otherwiseEncodeInUrl)?oss.str():"")
                    , ((useGet||otherwiseEncodeInUrl)?"":(simplePost?oss.str():sendData.dump()))
                    , [ret,env,noRequestResponseWrap,useThisPathForResponse](unsigned status, std::string &&response, std::unordered_map<std::string,std::string> &&headerFields) mutable {
                        if constexpr (std::is_same_v<Resp, RawString> || std::is_same_v<Resp, basic::ByteData>) {
                            ret->set_value({std::move(response)});
                        } else if constexpr (std::is_same_v<Resp, RawStringWithStatus>) {
                            ret->set_value({status, std::move(headerFields), std::move(response)});
                        } else {
                            if (status < 200 || status >= 300) {
                                ret->set_exception(std::make_exception_ptr(std::runtime_error("JSON request failed with status "+std::to_string(status))));
                            } else {
                                try {
                                    nlohmann::json x = nlohmann::json::parse(response);
                                    Resp resp;
                                    basic::nlohmann_json_interop::Json<Resp *> r(&resp);
                                    if (useThisPathForResponse != "") {
                                        std::vector<std::string> parts;
                                        boost::split(parts, useThisPathForResponse, boost::is_any_of("/"));
                                        nlohmann::json x1 = x;
                                        for (auto const &p : parts) {
                                            if (p != "") {
                                                x1 = x1[p];
                                            }
                                        }
                                        if (r.fromNlohmannJson(x1)) {
                                            ret->set_value(std::move(resp));
                                        }
                                    } else {
                                        if (r.fromNlohmannJson(noRequestResponseWrap?x:x["response"])) {
                                            ret->set_value(std::move(resp));
                                        }
                                    }
                                } catch (...) {
                                    env->log(infra::LogLevel::Error, "Cannot parse reply string '"+response+"' as json");
                                    ret->set_exception(std::current_exception());
                                }
                            }
                        }
                    }
                    , (simplePost?"application/x-www-form-urlencoded":"application/json")
                    , methodParam
                );
                return ret->get_future();
            } else {
                throw JsonRESTComponentException("json rest typed one shot remote call only works when the data types are json-encodable");
            }
        }
    };

}}}}}

#endif
