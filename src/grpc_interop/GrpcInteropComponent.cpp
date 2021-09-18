#include <tm_kit/transport/grpc_interop/GrpcInteropComponent.hpp>
#include <tm_kit/transport/grpc_interop/GrpcServiceInfo.hpp>
#include <tm_kit/transport/grpc_interop/GrpcConnectionLocatorUtils.hpp>
#include <tm_kit/transport/grpc_interop/GrpcSerializationHelper.hpp>
#include <tm_kit/transport/TLSConfigurationComponent.hpp>

#include <memory>
#include <unordered_map>
#include <mutex>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iterator>
#include <thread>

#include <grpcpp/channel.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/impl/codegen/sync_stream.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/client_callback.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {

    class GrpcInteropComponentImpl {
    private:
        std::unordered_map<ConnectionLocator, std::shared_ptr<grpc::Channel>> channels_;
        std::mutex channelsMutex_;
        std::unordered_map<ConnectionLocator, std::unique_ptr<grpc::ServerBuilder>> serverBuilders_;
        std::mutex serverBuildersMutex_;
        ConnectionLocator simplifyLocatorForChannel(ConnectionLocator const &l) {
            return ConnectionLocator(l.host(), l.port());
        }
        ConnectionLocator simplifyLocatorForServerBuilder(ConnectionLocator const &l) {
            return ConnectionLocator("", l.port());
        }
        class RpcSender {
        private:
            class LocalReactor : public grpc::ClientReadReactor<basic::ByteData> {
            private:
                RpcSender *parent_;
                std::string id_;
                basic::ByteData req_;
                basic::ByteData resp_;
                grpc::ClientContext ctx_;
                bool singleCallback_;
                std::atomic<bool> stopped_;
            public:
                LocalReactor(RpcSender *parent, std::string const &id, basic::ByteData &&req, bool singleCallback)
                    : parent_(parent), id_(id), req_(std::move(req)), resp_(), ctx_(), singleCallback_(singleCallback), stopped_(false)
                {}
                ~LocalReactor() = default;
                void start() {
                    grpc::ClientReadReactor<basic::ByteData>::StartCall();
                    grpc::ClientReadReactor<basic::ByteData>::StartRead(&resp_);
                }
                virtual void OnReadDone(bool good) override final {
                    if (good) {
                        if (!stopped_) {
                            parent_->callback(id_, std::move(resp_));
                        }
                        if (!singleCallback_) {
                            grpc::ClientReadReactor<basic::ByteData>::StartRead(&resp_);
                        }
                    }
                }
                virtual void OnDone(grpc::Status const &) override final {
                    if (stopped_) {
                        return;
                    }
                    parent_->finalCallback(id_);
                    parent_->removeReactor(id_);
                }
                grpc::ClientContext *context() {
                    return &ctx_;
                }
                basic::ByteData const *req() const {
                    return &req_;
                }
                void stop() {
                    stopped_ = true;
                }
            };

            GrpcServiceInfo serviceInfo_;
            std::string serviceInfoStr_;
            std::shared_ptr<grpc::Channel> channel_;
            std::unordered_map<std::string, std::unique_ptr<LocalReactor>> reactorMap_;
            std::mutex mutex_;

            std::optional<WireToUserHook> wireToUserHook_;
            std::function<void(bool, std::string const &, std::optional<std::string> &&)> cb_;
        public:
            RpcSender(GrpcInteropComponentImpl *parent, ConnectionLocator const &locator, std::optional<WireToUserHook> wireToUserHook, std::function<void(bool, std::string const &, std::optional<std::string> &&)> const &cb, TLSClientConfigurationComponent const *config)
                : serviceInfo_(connection_locator_utils::parseServiceInfo(locator))
                , serviceInfoStr_(grpcServiceInfoAsEndPointString(serviceInfo_))
                , channel_(parent->grpc_interop_getChannel(locator, config))
                , reactorMap_(), mutex_()
                , wireToUserHook_(wireToUserHook), cb_(cb)
            {
            }
            ~RpcSender() {
                stopAll();
            }
            void sendRequest(basic::ByteDataWithID &&req) {
                std::lock_guard<std::mutex> _(mutex_);
                auto iter = reactorMap_.find(req.id);
                if (iter != reactorMap_.end()) {
                    //Ignore client side duplicate request
                    return;
                }
                iter = reactorMap_.insert({
                    req.id
                    , std::make_unique<LocalReactor>(
                        this 
                        , req.id
                        , basic::ByteData { std::move(req.content) }
                        , serviceInfo_.isSingleRpcCall
                    )
                }).first;
                grpc::internal::ClientCallbackReaderFactory<basic::ByteData>::Create(
                    channel_.get()
                    , grpc::internal::RpcMethod {
                        serviceInfoStr_.c_str()
                        , (
                            serviceInfo_.isSingleRpcCall
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
            void stopAll() {
                std::lock_guard<std::mutex> _(mutex_);
                for (auto const &item : reactorMap_) {
                    item.second->stop();
                }
            }
            //signature forces copy        
            void removeReactor(std::string id) {
                std::lock_guard<std::mutex> _(mutex_);
                reactorMap_.erase(id);
            }
            void callback(std::string const &id, basic::ByteData &&data) {
                if (wireToUserHook_) {
                    auto d = (wireToUserHook_->hook)(basic::byteDataView(data));
                    if (d) {
                        cb_(false, id, std::move(d->content));
                    }
                } else {
                    cb_(false, id, std::move(data.content));
                }
            }
            void finalCallback(std::string const &id) {
                cb_(true, id, std::nullopt);
            }
        };
        //Please note that the RpcSenders are deliberately leaked
        //this is to avoid race conditions when removing rpc clients
        std::unordered_map<ConnectionLocator, RpcSender *> rpcClients_;
        std::mutex rpcClientsMutex_;
    public:
        GrpcInteropComponentImpl() : channels_(), channelsMutex_(), serverBuilders_(), serverBuildersMutex_(), rpcClients_(), rpcClientsMutex_() {}
        std::shared_ptr<grpc::Channel> grpc_interop_getChannel(ConnectionLocator const &locator, TLSClientConfigurationComponent const *config) {
            auto key = simplifyLocatorForChannel(locator);
            std::lock_guard<std::mutex> _(channelsMutex_);
            auto iter = channels_.find(key);
            if (iter == channels_.end()) {
                std::ostringstream oss;
                oss << key.host() << ":" << key.port();
                auto channelStr = oss.str();

                auto sslInfo = config?(config->getConfigurationItem(
                    TLSClientInfoKey {
                        key.host(), key.port()
                    }
                )):std::nullopt;
                std::shared_ptr<grpc::Channel> channel;
                if (sslInfo) {
                    grpc::SslCredentialsOptions options;
                    {
                        std::ifstream ifs(sslInfo->caCertificateFile.c_str());
                        options.pem_root_certs = std::string(
                            std::istreambuf_iterator<char>{ifs}, {}
                        );
                        ifs.close();
                    }
                    {
                        std::ifstream ifs(sslInfo->clientCertificateFile.c_str());
                        options.pem_cert_chain = std::string(
                            std::istreambuf_iterator<char>{ifs}, {}
                        );
                        ifs.close();
                    }
                    {
                        std::ifstream ifs(sslInfo->clientKeyFile.c_str());
                        options.pem_private_key = std::string(
                            std::istreambuf_iterator<char>{ifs}, {}
                        );
                        ifs.close();
                    }
                            
                    channel = grpc::CreateChannel(channelStr, grpc::SslCredentials(
                        options
                    ));
                } else {
                    channel = grpc::CreateChannel(channelStr, grpc::InsecureChannelCredentials());
                }

                iter = channels_.insert({
                    key
                    , channel
                }).first;
            }
            return iter->second;
        }
        void grpc_interop_registerService(ConnectionLocator const &locator, grpc::Service *service, TLSServerConfigurationComponent const *config) {
            auto key = simplifyLocatorForServerBuilder(locator);
            std::lock_guard<std::mutex> _(serverBuildersMutex_);
            auto iter = serverBuilders_.find(key);
            if (iter == serverBuilders_.end()) {
                iter = serverBuilders_.insert({
                    key
                    , std::make_unique<grpc::ServerBuilder>()
                }).first;
                std::ostringstream oss;
                oss << "[::]:" << key.port();
                std::string bindStr = oss.str();

                auto sslInfo = config?(config->getConfigurationItem(
                    TLSServerInfoKey {
                        key.port()
                    }
                )):std::nullopt;
                if (sslInfo) {
                    grpc::SslServerCredentialsOptions options;
                    options.pem_key_cert_pairs.push_back(
                        grpc::SslServerCredentialsOptions::PemKeyCertPair()
                    );
                    {
                        std::ifstream ifs(sslInfo->serverCertificateFile.c_str());
                        options.pem_key_cert_pairs.back().cert_chain = std::string(
                            std::istreambuf_iterator<char>{ifs}, {}
                        );
                        ifs.close();
                    }
                    {
                        std::ifstream ifs(sslInfo->serverKeyFile.c_str());
                        options.pem_key_cert_pairs.back().private_key = std::string(
                            std::istreambuf_iterator<char>{ifs}, {}
                        );
                        ifs.close();
                    }
                    iter->second->AddListeningPort(bindStr, grpc::SslServerCredentials(options));
                } else {
                    iter->second->AddListeningPort(bindStr, grpc::InsecureServerCredentials());;
                }
            }
            iter->second->RegisterService(service);
        }
        void finalizeEnvironment() {
            std::lock_guard<std::mutex> _(serverBuildersMutex_);
            for (auto const &item : serverBuilders_) {
                auto *p = item.second.get();
                std::thread th([p]() {
                    p->BuildAndStart()->Wait();
                });
                th.detach();
            }
        }
        std::function<void(basic::ByteDataWithID &&)> grpc_interop_setRPCClient(ConnectionLocator const &locator,
                        std::function<void(bool, std::string const &, std::optional<std::string> &&)> client,
                        std::optional<ByteDataHookPair> hookPair,
                        TLSClientConfigurationComponent const *config)
        {
            std::lock_guard<std::mutex> _(rpcClientsMutex_);
            auto iter = rpcClients_.find(locator);
            if (iter == rpcClients_.end()) {
                std::optional<WireToUserHook> wireToUserHook;
                if (hookPair) {
                    wireToUserHook = hookPair->wireToUser;
                } else {
                    wireToUserHook = std::nullopt;
                }
                iter = rpcClients_.insert({locator, new RpcSender(
                    this, locator, wireToUserHook, client, config
                )}).first;
            }
            auto *p = iter->second;
            if (hookPair && hookPair->userToWire) {
                auto hook = hookPair->userToWire->hook;
                return [p,hook](basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    p->sendRequest({data.id, std::move(x.content)});
                };
            } else {
                return [p](basic::ByteDataWithID &&data) {
                    p->sendRequest(std::move(data));
                };
            }
        }
        void grpc_interop_removeRPCClient(ConnectionLocator const &locator)
        {
            std::lock_guard<std::mutex> _(rpcClientsMutex_);
            auto iter = rpcClients_.find(locator);
            if (iter != rpcClients_.end()) {
                iter->second->stopAll();
                rpcClients_.erase(iter);
            }
        }
    };

    GrpcInteropComponent::GrpcInteropComponent() : impl_(std::make_unique<GrpcInteropComponentImpl>()) {}
    GrpcInteropComponent::GrpcInteropComponent(GrpcInteropComponent &&) = default;
    GrpcInteropComponent &GrpcInteropComponent::operator=(GrpcInteropComponent &&) = default;
    GrpcInteropComponent::~GrpcInteropComponent() = default;
    std::shared_ptr<grpc::Channel> GrpcInteropComponent::grpc_interop_getChannel(ConnectionLocator const &locator) {
        return impl_->grpc_interop_getChannel(locator, dynamic_cast<TLSClientConfigurationComponent const *>(this));
    }
    void GrpcInteropComponent::grpc_interop_registerService(ConnectionLocator const &locator, grpc::Service *service) {
        impl_->grpc_interop_registerService(locator, service, dynamic_cast<TLSServerConfigurationComponent const *>(this));
    }
    std::function<void(basic::ByteDataWithID &&)> GrpcInteropComponent::grpc_interop_setRPCClient(ConnectionLocator const &locator,
        std::function<void(bool, std::string const &, std::optional<std::string> &&)> client,
        std::optional<ByteDataHookPair> hookPair)
    {
        if (hookPair) {
            throw GrpcInteropComponentException("GrpcInteropComponent::grpc_interop_setRPCClient: hook is not supported");
        }
        return impl_->grpc_interop_setRPCClient(locator, client, hookPair, dynamic_cast<TLSClientConfigurationComponent const *>(this));
    }
    void GrpcInteropComponent::grpc_interop_removeRPCClient(ConnectionLocator const &locator) {
        impl_->grpc_interop_removeRPCClient(locator);
    }
    void GrpcInteropComponent::finalizeEnvironment() {
        impl_->finalizeEnvironment();
    }
}}}}}