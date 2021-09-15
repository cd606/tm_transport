#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVER_FACILITY_HPP_
#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERVER_FACILITY_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/AppClassifier.hpp>

#include <tm_kit/transport/grpc_interop/GrpcInteropComponent.hpp>
#include <tm_kit/transport/grpc_interop/GrpcServiceInfo.hpp>
#include <tm_kit/transport/grpc_interop/GrpcSerializationHelper.hpp>
#include <tm_kit/transport/grpc_interop/GrpcConnectionLocatorUtils.hpp>
#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>

#include <grpcpp/impl/codegen/server_callback.h>
#include <grpcpp/impl/codegen/server_callback_handlers.h>
#include <grpcpp/impl/codegen/proto_utils.h>
#include <grpcpp/impl/codegen/method_handler.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {
    
    template <class M, typename=std::enable_if_t<
        infra::app_classification_v<M> == infra::AppClassification::RealTime
        &&
        std::is_convertible_v<
            typename M::EnvironmentType *
            , GrpcInteropComponent *
        >
    >>
    class GrpcServerFacilityWrapper {
    private:
        using Env = typename M::EnvironmentType;
        using R = infra::AppRunner<M>;
        template <class Req, class Resp, typename=std::enable_if_t<
            (
                basic::bytedata_utils::ProtobufStyleSerializableChecker<Req>::IsProtobufStyleSerializable()
                &&
                basic::bytedata_utils::ProtobufStyleSerializableChecker<Resp>::IsProtobufStyleSerializable()
            )
            , void
        >>
        static void wrapFacilitioidConnector_internal(
            R &r
            , std::string const &registeredNameForFacility
            , typename infra::AppRunner<M>::template FacilitioidConnector<Req,Resp> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        )
        {
            auto serviceInfo = connection_locator_utils::parseServiceInfo(locator);

            auto triggerImporterPair = M::template triggerImporter<typename M::template Key<Req>>();
            auto importer = std::get<0>(triggerImporterPair);
            r.registerImporter(wrapperItemsNamePrefix+"/importer", importer);
            
            class LocalService : public grpc::Service {
            private:
                class LocalReactor : public grpc::ServerWriteReactor<Resp> {
                private:
                    LocalService *parent_;
                    typename Env::IDType id_;
                    std::deque<std::tuple<bool, std::unique_ptr<Resp>>> resps_;
                    std::mutex mutex_;
                public:
                    LocalReactor(LocalService *parent, typename Env::IDType const &id) : grpc::ServerWriteReactor<Resp>(), parent_(parent), id_(id), resps_(), mutex_() {}
                    ~LocalReactor() {}
                    void pushReply(Resp &&resp, bool final) {
                        std::lock_guard<std::mutex> _(mutex_);
                        resps_.push_back({final, std::make_unique<Resp>(std::move(resp))});
                        if (resps_.size() == 1) {
                            if (final) {
                                grpc::ServerWriteReactor<Resp>::StartWriteAndFinish(
                                    std::get<1>(resps_.back()).get()
                                    , grpc::WriteOptions {}
                                    , grpc::Status::OK
                                );
                            } else {
                                grpc::ServerWriteReactor<Resp>::StartWrite(
                                    std::get<1>(resps_.back()).get()
                                );
                            }
                        }
                    }
                    virtual void OnWriteDone(bool) override final {
                        std::lock_guard<std::mutex> _(mutex_);
                        resps_.pop_front();
                        if (!resps_.empty()) {
                            if (std::get<0>(resps_.front())) {
                                grpc::ServerWriteReactor<Resp>::StartWriteAndFinish(
                                    std::get<1>(resps_.front()).get()
                                    , grpc::WriteOptions {}
                                    , grpc::Status::OK
                                );
                            } else {
                                grpc::ServerWriteReactor<Resp>::StartWrite(
                                    std::get<1>(resps_.front()).get()
                                );
                            }
                        }
                    }
                    virtual void OnDone() override {
                        parent_->endHandling(id_);
                    }
                };
                Env *env_;
                std::function<void(typename M::template Key<Req> &&)> triggerFunc_;
                std::string serviceInfoStr_;
                std::unordered_map<typename Env::IDType, std::unique_ptr<LocalReactor>, typename Env::IDHash> reactors_;
                std::mutex mutex_;
            public:
                LocalService(Env *env, GrpcServiceInfo const &serviceInfo, std::function<void(typename M::template Key<Req> &&)> const &triggerFunc) 
                    : grpc::Service(), env_(env), triggerFunc_(triggerFunc) 
                    , serviceInfoStr_(grpcServiceInfoAsEndPointString(serviceInfo))
                    , reactors_(), mutex_()
                {
                    AddMethod(new grpc::internal::RpcServiceMethod(
                        serviceInfoStr_.c_str()
                        , grpc::internal::RpcMethod::SERVER_STREAMING
                        , new grpc::internal::ServerStreamingHandler<LocalService, Req, Resp>(
                            [](LocalService *p, grpc::ServerContext *ctx, const Req *req, grpc::ServerWriter<Resp> *writer) {
                                return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "");
                            }
                            , this
                        )
                    ));
                    experimental().MarkMethodCallback(
                        0
                        , new grpc::internal::CallbackServerStreamingHandler<Req, Resp>(
                            [this](grpc::CallbackServerContext *ctx, const Req *req) {
                                return this->serviceFunc(ctx, req);
                            }
                        )
                    );
                }
                grpc::ServerWriteReactor<Resp> *serviceFunc(grpc::CallbackServerContext *ctx, const Req *req) {
                    std::lock_guard<std::mutex> _(mutex_);
                    typename Env::IDType id = env_->new_id();
                    auto iter = reactors_.insert({
                        id
                        , std::make_unique<LocalReactor>(
                            this, id
                        )
                    }).first;
                    triggerFunc_(typename M::template Key<Req> {
                        id
                        , *req
                    });
                    return iter->second.get();
                }
                void handleResp(typename M::template InnerData<typename M::template KeyedData<Req, Resp>> &&resp) {
                    std::lock_guard<std::mutex> _(mutex_);
                    auto iter = reactors_.find(resp.timedData.value.key.id());
                    if (iter != reactors_.end()) {
                        iter->second->pushReply(
                            std::move(resp.timedData.value.data)
                            , resp.timedData.finalFlag
                        );
                    }
                }
                //this signature forces copying to avoid memory issue
                void endHandling(typename Env::IDType id) {
                    std::lock_guard<std::mutex> _(mutex_);
                    reactors_.erase(id);
                }
            };
            auto service = std::make_shared<LocalService>(
                r.environment(), serviceInfo, std::get<1>(triggerImporterPair)
            );
            r.preservePointer(service);
            auto exporter = M::template simpleExporter<
                typename M::template KeyedData<Req, Resp>
            >([service](typename M::template InnerData<typename M::template KeyedData<Req, Resp>> &&resp) {
                service->handleResp(std::move(resp));
            });
            r.registerExporter(wrapperItemsNamePrefix+"/exporter", exporter);
            toBeWrapped(r, r.importItem(importer), r.exporterAsSink(exporter));
            static_cast<GrpcInteropComponent *>(r.environment())->grpc_interop_registerService(locator, service.get());

            if constexpr (std::is_convertible_v<
                Env *
                , HeartbeatAndAlertComponent *
            >) {
                static_cast<HeartbeatAndAlertComponent *>(r.environment())->addFacilityChannel(
                    registeredNameForFacility
                    , std::string("grpc_interop://")+locator.toSerializationFormat()
                );
            }
        }
    public:
        template <class Req, class Resp>
        static void wrapFacilitioidConnector(
            R &r
            , std::string const &registeredNameForFacility
            , typename infra::AppRunner<M>::template FacilitioidConnector<Req,Resp> const &toBeWrapped
            , ConnectionLocator const &locator
            , std::string const &wrapperItemsNamePrefix
        )
        {
            if constexpr (
                basic::bytedata_utils::ProtobufStyleSerializableChecker<Req>::IsProtobufStyleSerializable()
                &&
                basic::bytedata_utils::ProtobufStyleSerializableChecker<Resp>::IsProtobufStyleSerializable()
            ) {
                wrapFacilitioidConnector_internal<Req,Resp>(
                    r, registeredNameForFacility, toBeWrapped, locator, wrapperItemsNamePrefix
                );
            } else {
                throw GrpcInteropComponentException("grpc server facility wrapper only works when the data types are protobuf-encodable");
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
    };

} } } } }

#endif