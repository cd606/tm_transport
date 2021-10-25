#ifndef TM_KIT_TRANSPORT_JSON_REST_JSON_REST_FACILITY_WRAPPER_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_JSON_REST_FACILITY_WRAPPER_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/AppClassifier.hpp>

#include <tm_kit/basic/NlohmannJsonInterop.hpp>
#include <tm_kit/basic/StructFieldInfoBasedCsvUtils.hpp>

#include <tm_kit/transport/json_rest/JsonRESTComponent.hpp>
#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {
    
    template <class M, typename=std::enable_if_t<
        infra::app_classification_v<M> == infra::AppClassification::RealTime
        &&
        std::is_convertible_v<
            typename M::EnvironmentType *
            , JsonRESTComponent *
        >
    >>
    class JsonRESTFacilityWrapper {
    private:
        using Env = typename M::EnvironmentType;
        using R = infra::AppRunner<M>;
        template <class Req, class Resp, typename=std::enable_if_t<
            (
                basic::nlohmann_json_interop::JsonWrappable<Req>::value
                &&
                basic::nlohmann_json_interop::JsonWrappable<Resp>::value
            )
            , void
        >>
        static void wrapFacilitioidConnector_internal(
            R &r
            , std::optional<std::string> const &registeredNameForFacility
            , typename infra::AppRunner<M>::template FacilitioidConnector<Req,Resp> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        )
        {
            auto triggerImporterPair = M::template triggerImporter<typename M::template Key<Req>>();
            auto importer = std::get<0>(triggerImporterPair);
            r.registerImporter(wrapperItemsNamePrefix+"/importer", importer);

            class LocalHandler {
            private:
                Env *env_;
                std::function<void(typename M::template Key<Req> &&)> triggerFunc_;
                std::unordered_map<
                    typename Env::IDType
                    , std::function<void(std::string const &)>
                    , typename Env::IDHash
                > cbMap_;
                std::mutex mutex_;
            public:
                LocalHandler(Env *env, std::function<void(typename M::template Key<Req> &&)> const &triggerFunc) 
                    : env_(env), triggerFunc_(triggerFunc), cbMap_(), mutex_() {}
                bool handleRequest(
                    std::string const &login
                    , std::string const &requestBody
                    , std::unordered_map<std::string, std::vector<std::string>> const &queryMap
                    , std::function<void(std::string const &)> const &cb
                ) {
                    if (requestBody != "") {
                        try {
                            auto incomingJson = nlohmann::json::parse(requestBody);
                            Req req;
                            basic::nlohmann_json_interop::Json<Req *> j(&req);
                            if (j.fromNlohmannJson(incomingJson["request"])) {
                                if constexpr (basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<Req>) {
                                    for (auto const &item : queryMap) {
                                        if (item.second.size() == 1) {
                                            basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvInput<Req>
                                                ::readOneNameValuePair(req, item.first, item.second[0]);
                                        } else {
                                            for (std::size_t ii=0; ii<item.second.size(); ++ii) {
                                                basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvInput<Req>
                                                    ::readOneNameValuePair(req, item.first+"["+std::to_string(ii)+"]", item.second[ii]);
                                            }
                                        }
                                    }
                                }
                                typename Env::IDType id = env_->new_id();
                                std::lock_guard<std::mutex> _(mutex_);
                                cbMap_[id] = cb;
                                triggerFunc_(typename M::template Key<Req> {
                                    id, std::move(req)
                                });
                                return true;
                            } else {
                                return false;
                            }
                        } catch (std::exception const &ex) {
                            std::ostringstream oss;
                            oss << "[JsonRESTFacilityWrapper::LocalHandler::handleRequest] Parsing error for '" << requestBody << "'";
                            env_->log(infra::LogLevel::Warning, oss.str());
                            return false;
                        }
                    } else {
                        if constexpr (basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<Req>) {
                            Req req;
                            basic::struct_field_info_utils::StructFieldInfoBasedInitializer<Req>::initialize(req);
                            for (auto const &item : queryMap) {
                                if (item.second.size() == 1) {
                                    basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvInput<Req>
                                        ::readOneNameValuePair(req, item.first, item.second[0]);
                                } else {
                                    for (std::size_t ii=0; ii<item.second.size(); ++ii) {
                                        basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvInput<Req>
                                            ::readOneNameValuePair(req, item.first+"["+std::to_string(ii)+"]", item.second[ii]);
                                    }
                                }
                            }
                            typename Env::IDType id = env_->new_id();
                            std::lock_guard<std::mutex> _(mutex_);
                            cbMap_[id] = cb;
                            triggerFunc_(typename M::template Key<Req> {
                                id, std::move(req)
                            });
                            return true;
                        } else {
                            std::ostringstream oss;
                            oss << "[JsonRESTFacilityWrapper::LocalHandler::handleRequest] No support for url parameter for type " << typeid(Req).name();
                            env_->log(infra::LogLevel::Warning, oss.str());
                            return false;
                        }
                    }
                }
                void handleResponse(
                    typename M::template KeyedData<Req,Resp> &&resp
                ) {
                    std::lock_guard<std::mutex> _(mutex_);
                    auto iter = cbMap_.find(resp.key.id());
                    if (iter == cbMap_.end()) {
                        return;
                    }
                    basic::nlohmann_json_interop::Json<Resp *> j(&(resp.data));
                    nlohmann::json respObj;
                    j.toNlohmannJson(respObj["response"]);
                    (iter->second)(respObj.dump());
                    cbMap_.erase(iter);
                }
            };

            auto handler = std::make_shared<LocalHandler>(
                r.environment(), std::get<1>(triggerImporterPair)
            );
            r.preservePointer(handler);
            auto exporter = M::template pureExporter<
                typename M::template KeyedData<Req, Resp>
            >([handler](typename M::template KeyedData<Req, Resp> &&resp) {
                handler->handleResponse(std::move(resp));
            });
            r.registerExporter(wrapperItemsNamePrefix+"/exporter", exporter);
            toBeWrapped(r, r.importItem(importer), r.exporterAsSink(exporter));
            static_cast<JsonRESTComponent *>(r.environment())->registerHandler(
                locator, [handler](std::string const &login, std::string const &reqBody, std::unordered_map<std::string, std::vector<std::string>> const &queryMap, std::function<void(std::string const &)> const &cb) {
                    return handler->handleRequest(login, reqBody, queryMap, cb);
                }
            );

            if (registeredNameForFacility) {
                if constexpr (std::is_convertible_v<
                    Env *
                    , HeartbeatAndAlertComponent *
                >) {
                    static_cast<HeartbeatAndAlertComponent *>(r.environment())->addFacilityChannel(
                        *registeredNameForFacility
                        , std::string("json_rest://")+locator.toSerializationFormat()
                    );
                }
            }
        }
        template <class Req, class Resp, typename=std::enable_if_t<
            (
                basic::nlohmann_json_interop::JsonWrappable<Req>::value
                &&
                basic::nlohmann_json_interop::JsonWrappable<Resp>::value
            )
            , void
        >>
        static void wrapFacilitioidConnectorWithStringIdentity_internal(
            R &r
            , std::optional<std::string> const &registeredNameForFacility
            , typename infra::AppRunner<M>::template FacilitioidConnector<std::tuple<std::string,Req>,Resp> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        )
        {
            auto triggerImporterPair = M::template triggerImporter<typename M::template Key<std::tuple<std::string,Req>>>();
            auto importer = std::get<0>(triggerImporterPair);
            r.registerImporter(wrapperItemsNamePrefix+"/importer", importer);

            class LocalHandler {
            private:
                Env *env_;
                std::function<void(typename M::template Key<std::tuple<std::string,Req>> &&)> triggerFunc_;
                std::unordered_map<
                    typename Env::IDType
                    , std::function<void(std::string const &)>
                    , typename Env::IDHash
                > cbMap_;
                std::mutex mutex_;
            public:
                LocalHandler(Env *env, std::function<void(typename M::template Key<std::tuple<std::string,Req>> &&)> const &triggerFunc) 
                    : env_(env), triggerFunc_(triggerFunc), cbMap_(), mutex_() {}
                bool handleRequest(
                    std::string const &login
                    , std::string const &requestBody
                    , std::unordered_map<std::string, std::vector<std::string>> const &queryMap
                    , std::function<void(std::string const &)> const &cb
                ) {
                    if (requestBody != "") {
                        try {
                            auto incomingJson = nlohmann::json::parse(requestBody);
                            Req req;
                            basic::nlohmann_json_interop::Json<Req *> j(&req);
                            if (j.fromNlohmannJson(incomingJson["request"])) {
                                if constexpr (basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<Req>) {
                                    for (auto const &item : queryMap) {
                                        if (item.second.size() == 1) {
                                            basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvInput<Req>
                                                ::readOneNameValuePair(req, item.first, item.second[0]);
                                        } else {
                                            for (std::size_t ii=0; ii<item.second.size(); ++ii) {
                                                basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvInput<Req>
                                                    ::readOneNameValuePair(req, item.first+"["+std::to_string(ii)+"]", item.second[ii]);
                                            }
                                        }
                                    }
                                }
                                typename Env::IDType id = env_->new_id();
                                std::lock_guard<std::mutex> _(mutex_);
                                cbMap_[id] = cb;
                                triggerFunc_(typename M::template Key<std::tuple<std::string,Req>> {
                                    id, {login, std::move(req)}
                                });
                                return true;
                            } else {
                                return false;
                            }
                        } catch (std::exception const &ex) {
                            std::ostringstream oss;
                            oss << "[JsonRESTFacilityWrapper::LocalHandler (with identity)::handleRequest] Parsing error for '" << requestBody << "'";
                            env_->log(infra::LogLevel::Warning, oss.str());
                            return false;
                        }
                    } else {
                        if constexpr (basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<Req>) {
                            Req req;
                            basic::struct_field_info_utils::StructFieldInfoBasedInitializer<Req>::initialize(req);
                            for (auto const &item : queryMap) {
                                if (item.second.size() == 1) {
                                    basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvInput<Req>
                                        ::readOneNameValuePair(req, item.first, item.second[0]);
                                } else {
                                    for (std::size_t ii=0; ii<item.second.size(); ++ii) {
                                        basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvInput<Req>
                                            ::readOneNameValuePair(req, item.first+"["+std::to_string(ii)+"]", item.second[ii]);
                                    }
                                }
                            }
                            typename Env::IDType id = env_->new_id();
                            std::lock_guard<std::mutex> _(mutex_);
                            cbMap_[id] = cb;
                            triggerFunc_(typename M::template Key<std::tuple<std::string,Req>> {
                                id, {login, std::move(req)}
                            });
                            return true;
                        } else {
                            std::ostringstream oss;
                            oss << "[JsonRESTFacilityWrapper::LocalHandler (with identity)::handleRequest] No support for url parameter for type " << typeid(Req).name();
                            env_->log(infra::LogLevel::Warning, oss.str());
                            return false;
                        }
                    }
                }
                void handleResponse(
                    typename M::template KeyedData<std::tuple<std::string,Req>,Resp> &&resp
                ) {
                    std::lock_guard<std::mutex> _(mutex_);
                    auto iter = cbMap_.find(resp.key.id());
                    if (iter == cbMap_.end()) {
                        return;
                    }
                    basic::nlohmann_json_interop::Json<Resp *> j(&(resp.data));
                    nlohmann::json respObj;
                    j.toNlohmannJson(respObj["response"]);
                    (iter->second)(respObj.dump());
                    cbMap_.erase(iter);
                }
            };

            auto handler = std::make_shared<LocalHandler>(
                r.environment(), std::get<1>(triggerImporterPair)
            );
            r.preservePointer(handler);
            auto exporter = M::template pureExporter<
                typename M::template KeyedData<std::tuple<std::string,Req>, Resp>
            >([handler](typename M::template KeyedData<std::tuple<std::string,Req>, Resp> &&resp) {
                handler->handleResponse(std::move(resp));
            });
            r.registerExporter(wrapperItemsNamePrefix+"/exporter", exporter);
            toBeWrapped(r, r.importItem(importer), r.exporterAsSink(exporter));
            static_cast<JsonRESTComponent *>(r.environment())->registerHandler(
                locator, [handler](std::string const &login, std::string const &reqBody, std::unordered_map<std::string, std::vector<std::string>> const &queryMap, std::function<void(std::string const &)> const &cb) {
                    return handler->handleRequest(login, reqBody, queryMap, cb);
                }
            );

            if (registeredNameForFacility) {
                if constexpr (std::is_convertible_v<
                    Env *
                    , HeartbeatAndAlertComponent *
                >) {
                    static_cast<HeartbeatAndAlertComponent *>(r.environment())->addFacilityChannel(
                        *registeredNameForFacility
                        , std::string("json_rest://")+locator.toSerializationFormat()
                    );
                }
            }
        }
    public:
        template <class Req, class Resp>
        static void wrapFacilitioidConnector(
            R &r
            , std::optional<std::string> const &registeredNameForFacility
            , typename infra::AppRunner<M>::template FacilitioidConnector<Req,Resp> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        )
        {
            if constexpr (
                basic::nlohmann_json_interop::JsonWrappable<Req>::value
                &&
                basic::nlohmann_json_interop::JsonWrappable<Resp>::value
            ) {
                wrapFacilitioidConnector_internal<Req,Resp>(
                    r, registeredNameForFacility, toBeWrapped, locator, wrapperItemsNamePrefix
                );
            } else {
                throw JsonRESTComponentException("Json REST server facility wrapper only works when the data types are json-encodable");
            }
        }
        template <class Req, class Resp>
        static void wrapOnOrderFacility(
            R &r
            , std::shared_ptr<typename M::template OnOrderFacility<Req,Resp>> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        ) {
            wrapFacilitioidConnector<Req,Resp>(
                r
                , r.getRegisteredName(toBeWrapped)
                , r.facilityConnector(toBeWrapped)
                , locator
                , wrapperItemsNamePrefix
            );
        }
        template <class Req, class Resp, class C>
        static void wrapLocalOnOrderFacility(
            R &r
            , std::shared_ptr<typename M::template LocalOnOrderFacility<Req,Resp,C>> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        ) {
            wrapFacilitioidConnector<Req,Resp>(
                r
                , r.getRegisteredName(toBeWrapped)
                , r.facilityConnector(toBeWrapped)
                , locator
                , wrapperItemsNamePrefix
            );
        }
        template <class Req, class Resp, class C>
        static void wrapOnOrderFacilityWithExternalEffects(
            R &r
            , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<Req,Resp,C>> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        ) {
            wrapFacilitioidConnector<Req,Resp>(
                r
                , r.getRegisteredName(toBeWrapped)
                , r.facilityConnector(toBeWrapped)
                , locator
                , wrapperItemsNamePrefix
            );
        }
        template <class Req, class Resp, class C, class D>
        static void wrapVIEOnOrderFacility(
            R &r
            , std::shared_ptr<typename M::template VIEOnOrderFacility<Req,Resp,C,D>> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        ) {
            wrapFacilitioidConnector<Req,Resp>(
                r
                , r.getRegisteredName(toBeWrapped)
                , r.facilityConnector(toBeWrapped)
                , locator
                , wrapperItemsNamePrefix
            );
        }
        template <class Req, class Resp>
        static void wrapFacilitioidConnectorWithStringIdentity(
            R &r
            , std::optional<std::string> const &registeredNameForFacility
            , typename infra::AppRunner<M>::template FacilitioidConnector<std::tuple<std::string,Req>,Resp> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        )
        {
            if constexpr (
                basic::nlohmann_json_interop::JsonWrappable<Req>::value
                &&
                basic::nlohmann_json_interop::JsonWrappable<Resp>::value
            ) {
                wrapFacilitioidConnectorWithStringIdentity_internal<Req,Resp>(
                    r, registeredNameForFacility, toBeWrapped, locator, wrapperItemsNamePrefix
                );
            } else {
                throw JsonRESTComponentException("Json REST server facility wrapper only works when the data types are json-encodable");
            }
        }
        template <class Req, class Resp>
        static void wrapOnOrderFacilityWithStringIdentity(
            R &r
            , std::shared_ptr<typename M::template OnOrderFacility<std::tuple<std::string,Req>,Resp>> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        ) {
            wrapFacilitioidConnectorWithStringIdentity<Req,Resp>(
                r
                , r.getRegisteredName(toBeWrapped)
                , r.facilityConnector(toBeWrapped)
                , locator
                , wrapperItemsNamePrefix
            );
        }
        template <class Req, class Resp, class C>
        static void wrapLocalOnOrderFacilityWithStringIdentity(
            R &r
            , std::shared_ptr<typename M::template LocalOnOrderFacility<std::tuple<std::string,Req>,Resp,C>> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        ) {
            wrapFacilitioidConnectorWithStringIdentity<Req,Resp>(
                r
                , r.getRegisteredName(toBeWrapped)
                , r.facilityConnector(toBeWrapped)
                , locator
                , wrapperItemsNamePrefix
            );
        }
        template <class Req, class Resp, class C>
        static void wrapOnOrderFacilityWithExternalEffectsWithStringIdentity(
            R &r
            , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<std::tuple<std::string,Req>,Resp,C>> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        ) {
            wrapFacilitioidConnectorWithStringIdentity<Req,Resp>(
                r
                , r.getRegisteredName(toBeWrapped)
                , r.facilityConnector(toBeWrapped)
                , locator
                , wrapperItemsNamePrefix
            );
        }
        template <class Req, class Resp, class C, class D>
        static void wrapVIEOnOrderFacilityWithStringIdentity(
            R &r
            , std::shared_ptr<typename M::template VIEOnOrderFacility<std::tuple<std::string,Req>,Resp,C,D>> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        ) {
            wrapFacilitioidConnectorWithStringIdentity<Req,Resp>(
                r
                , r.getRegisteredName(toBeWrapped)
                , r.facilityConnector(toBeWrapped)
                , locator
                , wrapperItemsNamePrefix
            );
        }
    };

} } } } }

#endif