#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_INTEROP_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_INTEROP_COMPONENT_HPP_

#include <memory>
#include <unordered_map>
#include <mutex>
#include <iostream>
#include <sstream>
#include <thread>

#include <grpcpp/channel.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {
    class GrpcInteropComponent {
    private:
        std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channels_;
        std::mutex channelsMutex_;
        std::unordered_map<int, std::unique_ptr<grpc::ServerBuilder>> serverBuilders_;
        std::mutex serverBuildersMutex_;
    public:
        GrpcInteropComponent() : channels_(), channelsMutex_(), serverBuilders_(), serverBuildersMutex_() {}
        std::shared_ptr<grpc::Channel> grpc_interop_getChannel(std::string const &channelAddr) {
            std::lock_guard<std::mutex> _(channelsMutex_);
            auto iter = channels_.find(channelAddr);
            if (iter == channels_.end()) {
                iter = channels_.insert({
                    channelAddr.c_str()
                    , grpc::CreateChannel(channelAddr, grpc::InsecureChannelCredentials())
                }).first;
            }
            return iter->second;
        }
        std::shared_ptr<grpc::Channel> grpc_interop_getChannel(std::string const &host, int port) {
            std::ostringstream oss;
            oss << host << ":" <<  port;
            return grpc_interop_getChannel(oss.str());
        }
        grpc::ServerBuilder *grpc_interop_getServerBuilder(int port) {
            std::lock_guard<std::mutex> _(serverBuildersMutex_);
            auto iter = serverBuilders_.find(port);
            if (iter == serverBuilders_.end()) {
                iter = serverBuilders_.insert({
                    port
                    , std::make_unique<grpc::ServerBuilder>()
                }).first;
                std::ostringstream oss;
                oss << "[::]:" << port;
                iter->second->AddListeningPort(oss.str(), grpc::InsecureServerCredentials());
            }
            return iter->second.get();
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
    };
}}}}}

#endif