#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_CLIENT_FACILITY_HPP_
#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_CLIENT_FACILITY_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/AppClassifier.hpp>

#include <tm_kit/transport/grpc_interop/GrpcInteropComponent.hpp>
#include <tm_kit/transport/grpc_interop/GrpcServiceInfo.hpp>
#include <tm_kit/transport/grpc_interop/GrpcSerializationHelper.hpp>
#include <tm_kit/transport/grpc_interop/GrpcConnectionLocatorUtils.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>

#include <type_traits>
#include <memory>
#include <iostream>
#include <sstream>
#include <mutex>
#include <condition_variable>

#include <grpcpp/channel.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/proto_utils.h>
#include <grpcpp/impl/codegen/sync_stream.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/client_callback.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {

    template <class M, typename=std::enable_if_t<
        infra::app_classification_v<M> == infra::AppClassification::RealTime
        &&
        std::is_convertible_v<
            typename M::EnvironmentType *
            , GrpcInteropComponent *
        >
    >>
    class GrpcClientFacilityFactory {
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
                basic::bytedata_utils::ProtobufStyleSerializableChecker<Req>::IsProtobufStyleSerializable()
                &&
                basic::bytedata_utils::ProtobufStyleSerializableChecker<Resp>::IsProtobufStyleSerializable()
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
                    bool isSingleCallback_;
                    std::string serviceInfoStr_;
                    ConnectionLocator locator_;
                    std::shared_ptr<grpc::Channel> channel_;
                    std::mutex startMutex_;
                    std::condition_variable startCond_;
                    class OneCallReactor : public grpc::ClientReadReactor<Resp> {
                    private:
                        LocalF *parent_;
                        Env *env_;
                        typename Env::IDType id_;
                        Req req_;
                        Resp resp_;
                        grpc::ClientContext ctx_;
                        bool singleCallback_;
                    public:
                        OneCallReactor(LocalF *parent, Env *env, typename Env::IDType const &id, Req &&req, bool singleCallback)
                            : parent_(parent), env_(env), id_(id), req_(std::move(req)), resp_(), ctx_(), singleCallback_(singleCallback)
                        {}
                        ~OneCallReactor() = default;
                        void start() {
                            grpc::ClientReadReactor<Resp>::StartCall();
                            grpc::ClientReadReactor<Resp>::StartRead(&resp_);
                        }
                        virtual void OnReadDone(bool good) override final {
                            if (good) {
                                parent_->publish(
                                    env_
                                    , typename M::template Key<Resp> {
                                        id_ 
                                        , std::move(resp_)
                                    }
                                    , singleCallback_
                                );
                                if (!singleCallback_) {
                                    grpc::ClientReadReactor<Resp>::StartRead(&resp_);
                                }
                            }
                        }
                        virtual void OnDone(grpc::Status const &) {
                            parent_->eraseReactor(id_);
                        }
                        grpc::ClientContext *context() {
                            return &ctx_;
                        }
                        Req const *req() const {
                            return &req_;
                        }
                    };
                    std::unordered_map<typename Env::IDType, std::unique_ptr<OneCallReactor>, typename Env::IDHash> reactorMap_;
                    std::mutex reactorMapMutex_;
                public:
                    LocalF(ConnectionLocator const &locator)
                        : M::IExternalComponent()
                        , M::template AbstractOnOrderFacility<Req, Resp>()
                        , infra::RealTimeAppComponents<Env>::template ThreadedHandler<
                            typename M::template Key<Req>
                            , LocalF 
                        >()
                        , isSingleCallback_(false)
                        , serviceInfoStr_()
                        , locator_(locator)
                        , channel_()
                        , startMutex_()
                        , startCond_()
                        , reactorMap_()
                        , reactorMapMutex_()
                    {
                        GrpcServiceInfo serviceInfo = connection_locator_utils::parseServiceInfo(locator);
                        serviceInfoStr_ = grpcServiceInfoAsEndPointString(serviceInfo);
                        isSingleCallback_ = serviceInfo.isSingleRpcCall;
                        this->startThread();
                    }
                    virtual ~LocalF() {}
                    virtual void start(Env *env) override final {
                        {
                            std::lock_guard<std::mutex> _(startMutex_);
                            channel_ = static_cast<GrpcInteropComponent *>(env)->grpc_interop_getChannel(locator_);
                        }
                        startCond_.notify_one();
                    }
                    void actuallyHandle(typename M::template InnerData<typename M::template Key<Req>> &&req) {
                        std::lock_guard<std::mutex> _(reactorMapMutex_);
                        auto iter = reactorMap_.find(req.timedData.value.id());
                        if (iter != reactorMap_.end()) {
                            //Ignore client side duplicate request
                            return;
                        }
                        iter = reactorMap_.insert({
                            req.timedData.value.id()
                            , std::make_unique<OneCallReactor>(
                                this 
                                , req.environment 
                                , req.timedData.value.id()
                                , std::move(req.timedData.value.key())
                                , isSingleCallback_
                            )
                        }).first;
                        grpc::internal::ClientCallbackReaderFactory<Resp>::Create(
                            channel_.get()
                            , grpc::internal::RpcMethod {
                                serviceInfoStr_.c_str()
                                , (
                                    isSingleCallback_
                                    ?
                                    grpc::internal::RpcMethod::RpcType::NORMAL_RPC
                                    :
                                    grpc::internal::RpcMethod::RpcType::SERVER_STREAMING
                                )
                            }
                            , iter->second->context()
                            , iter->second->req()
                            , iter->second.get()
                        );
                        iter->second->start();
                    }
                    //the signature requires a copy, because the ID 
                    //was passed from a struct that will be erased
                    void eraseReactor(typename Env::IDType id) {
                        this->markEndHandlingRequest(id);
                        {
                            std::lock_guard<std::mutex> _(reactorMapMutex_);
                            reactorMap_.erase(id);
                        }
                    }
                    void idleWork() {}
                    void waitForStart() {
                        while (true) {
                            std::unique_lock<std::mutex> lock(startMutex_);
                            startCond_.wait_for(lock, std::chrono::milliseconds(1));
                            if (channel_) {
                                lock.unlock();
                                break;
                            } else {
                                lock.unlock();
                            }
                        }
                    }
                };
                return M::template fromAbstractOnOrderFacility<Req,Resp>(new LocalF(locator));
            } else {
                throw GrpcInteropComponentException("grpc client facility only works when the data types are protobuf-encodable");
            }
        }
        template <class Req, class Resp>
        static std::vector<Resp> runSyncClient(
            Env *env
            , ConnectionLocator const &locator
            , Req const &req
        )
        {
            if constexpr (
                basic::bytedata_utils::ProtobufStyleSerializableChecker<Req>::IsProtobufStyleSerializable()
                &&
                basic::bytedata_utils::ProtobufStyleSerializableChecker<Resp>::IsProtobufStyleSerializable()
            ) {
                auto serviceInfo = connection_locator_utils::parseServiceInfo(locator);
                auto serviceInfoStr = grpcServiceInfoAsEndPointString(serviceInfo);
                auto channel = static_cast<grpc_interop::GrpcInteropComponent *>(env)->grpc_interop_getChannel(locator);
                
                std::vector<Resp> ret;
                grpc::ClientContext ctx;
                
                if (serviceInfo.isSingleRpcCall) {
                    Resp resp;
                    if (grpc::internal::BlockingUnaryCall<Req, Resp>(
                        channel.get()
                        , grpc::internal::RpcMethod {
                            serviceInfoStr.c_str()
                            , grpc::internal::RpcMethod::RpcType::NORMAL_RPC
                        }
                        , &ctx 
                        , req 
                        , &resp
                    ).ok()) {
                        ret.push_back(resp);
                    }
                } else {
                    std::unique_ptr<grpc::ClientReader<Resp>> reader {
                        grpc::internal::ClientReaderFactory<Resp>::Create(
                            channel.get() 
                            , grpc::internal::RpcMethod {
                                serviceInfoStr.c_str()
                                , grpc::internal::RpcMethod::RpcType::SERVER_STREAMING
                            }
                            , &ctx 
                            , req
                        )
                    };
                    Resp resp;
                    while (reader->Read(&resp)) {
                        ret.push_back(std::move(resp));
                    }
                    reader->Finish();
                }
                return ret;
            } else {
                throw GrpcInteropComponentException("grpc sync client only works when the data types are protobuf-encodable");
            }
        }
        template <class Req, class Resp>
        static std::future<Resp> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, Req &&request) {
            if constexpr (
                basic::bytedata_utils::ProtobufStyleSerializableChecker<Req>::IsProtobufStyleSerializable()
                &&
                basic::bytedata_utils::ProtobufStyleSerializableChecker<Resp>::IsProtobufStyleSerializable()
            ) {
                if constexpr(DetermineClientSideIdentityForRequest<Env, Req>::HasIdentity) {
                    if constexpr (std::is_same_v<typename DetermineClientSideIdentityForRequest<Env, Req>::IdentityType, std::string>) {
                        auto ret = std::make_shared<std::promise<Resp>>();
                        std::thread th([env,rpcQueueLocator,request=std::move(request),ret]() mutable {
                            auto callRes = runSyncClient<Req,Resp>(env, rpcQueueLocator, std::move(request));
                            if (!callRes.empty()) {
                                ret->set_value_at_thread_exit(callRes[0]);
                            }
                        });
                        th.detach();
                        return ret->get_future();
                    } else {
                        throw GrpcInteropComponentException("grpc typed one shot remote call does not work when the request has identity");
                    }
                } else {
                    auto ret = std::make_shared<std::promise<Resp>>();
                    std::thread th([env,rpcQueueLocator,request=std::move(request),ret]() mutable {
                        auto callRes = runSyncClient<Req,Resp>(env, rpcQueueLocator, std::move(request));
                        if (!callRes.empty()) {
                            ret->set_value_at_thread_exit(callRes[0]);
                        }
                    });
                    th.detach();
                    return ret->get_future();
                }
            } else {
                throw GrpcInteropComponentException("grpc typed one shot remote call only works when the data types are protobuf-encodable");
            }
        }
    };

}}}}}

#endif