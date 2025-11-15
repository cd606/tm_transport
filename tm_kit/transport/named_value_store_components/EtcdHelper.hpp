#ifndef TM_KIT_TRANSPORT_ETCD_NAMED_VALUE_STORE_ETCD_HELPER_HPP_
#define TM_KIT_TRANSPORT_ETCD_NAMED_VALUE_STORE_ETCD_HELPER_HPP_

#include <grpcpp/grpcpp.h>
#ifdef _MSC_VER
#undef DELETE
#endif
#include <libetcd/rpc.grpc.pb.h>
#include <libetcd/kv.pb.h>

#include <optional>
#include <cstdlib>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace named_value_store_components {
    namespace etcd {
        class EtcdHelper {
        public:
            static std::shared_ptr<grpc::ChannelInterface> localInsecureChannel() {
                return grpc::CreateChannel("127.0.0.1:2379", grpc::InsecureChannelCredentials());
            }
            static std::shared_ptr<grpc::ChannelInterface> localSecureChannelFromEnvironmentVariables() {
                char *p = std::getenv("ETCDCTL_ENDPOINTS");
                if (!p) {
                    return localInsecureChannel();
                }
                std::string endPoint {p};
                if (endPoint == "") {
                    return localInsecureChannel();
                }
                if (endPoint.starts_with("http://")) {
                    return grpc::CreateChannel(endPoint.substr(7), grpc::InsecureChannelCredentials());
                }
                if (!endPoint.starts_with("https://")) {
                    throw std::runtime_error("[EtcdHelper::localSecureChannelFromEnvironmentVariables] end point does not start with https://");
                }
                p = std::getenv("ETCDCTL_CACERT");
                if (!p) {
                    throw std::runtime_error("[EtcdHelper::localSecureChannelFromEnvironmentVariables] No CA cert file");
                }
                std::string caCertFile {p};
                p = std::getenv("ETCDCTL_CERT");
                if (!p) {
                    throw std::runtime_error("[EtcdHelper::localSecureChannelFromEnvironmentVariables] No client cert file");
                }
                std::string clientCertFile {p};
                p = std::getenv("ETCDCTL_KEY");
                if (!p) {
                    throw std::runtime_error("[EtcdHelper::localSecureChannelFromEnvironmentVariables] No client key file");
                }
                std::string clientKeyFile {p};
                grpc::SslCredentialsOptions options;
                {
                    
                    std::ifstream ifs(caCertFile.c_str());
                    options.pem_root_certs = std::string(
                        std::istreambuf_iterator<char>{ifs}, {}
                    );
                    ifs.close();
                }
                {
                    std::ifstream ifs(clientCertFile.c_str());
                    options.pem_cert_chain = std::string(
                        std::istreambuf_iterator<char>{ifs}, {}
                    );
                    ifs.close();
                }
                {
                    std::ifstream ifs(clientKeyFile.c_str());
                    options.pem_private_key = std::string(
                        std::istreambuf_iterator<char>{ifs}, {}
                    );
                    ifs.close();
                }
                        
                return grpc::CreateChannel(endPoint.substr(8), grpc::SslCredentials(
                    options
                ));
            }
            static std::optional<std::string> getLocal(std::string const &key) {
                etcdserverpb::RangeRequest range;
                range.set_key(key);

                etcdserverpb::RangeResponse response;
                grpc::Status status;

                auto channel = localSecureChannelFromEnvironmentVariables();
                auto stub = etcdserverpb::KV::NewStub(channel);
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                stub->Range(&ctx, range, &response);

                if (response.kvs_size() > 0) {
                    return response.kvs()[0].value();
                } else {
                    return std::nullopt;
                }
            }
            static void setLocal(std::string const &key, std::string const &value) {
                etcdserverpb::TxnRequest txn;
                auto *action = txn.add_success();
                auto *put = action->mutable_request_put();
                put->set_key(key);
                put->set_value(value);

                etcdserverpb::TxnResponse txnResp;

                auto channel = localSecureChannelFromEnvironmentVariables();
                auto stub = etcdserverpb::KV::NewStub(channel);
                
                grpc::ClientContext txnCtx;
                txnCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                stub->Txn(&txnCtx, txn, &txnResp);
            }
            static void putLocal(std::string const &key, std::string const &value) {
                setLocal(key, value);
            }
            static void deleteLocal(std::string const &key) {
                etcdserverpb::TxnRequest txn;
                auto *action = txn.add_success();
                auto *del = action->mutable_request_delete_range();
                del->set_key(key);

                etcdserverpb::TxnResponse txnResp;

                auto channel = localSecureChannelFromEnvironmentVariables();
                auto stub = etcdserverpb::KV::NewStub(channel);
                
                grpc::ClientContext txnCtx;
                txnCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                stub->Txn(&txnCtx, txn, &txnResp);
            }
        };
    }
} } } } }

#endif
