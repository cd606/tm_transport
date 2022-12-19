#ifndef TM_KIT_TRANSPORT_ETCD_NAMED_VALUE_STORE_ETCD_LOCAL_IMPORTER_HPP_
#define TM_KIT_TRANSPORT_ETCD_NAMED_VALUE_STORE_ETCD_LOCAL_IMPORTER_HPP_

#include <grpcpp/grpcpp.h>
#ifdef _MSC_VER
#undef DELETE
#endif
#include <libetcd/rpc.grpc.pb.h>
#include <libetcd/kv.pb.h>

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/AppClassifier.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace named_value_store_components {
    namespace etcd {
        template <class M, typename=std::enable_if_t<infra::AppClassifier<M>::TheClassification == infra::AppClassification::RealTime>>
        class EtcdLocalImporter {
        public:
            static std::shared_ptr<typename M::template Importer<std::string>> singleKeyImporter(std::string const &key) {
                return M::template simpleImporter<std::string>(
                    [key](typename M::template PublisherCall<std::string> const &pub) {
                        etcdserverpb::RangeRequest range;
                        range.set_key(key);

                        etcdserverpb::RangeResponse initResponse;
                        grpc::Status initStatus;

                        etcdserverpb::WatchRequest req;
                        auto *r = req.mutable_create_request();
                        r->set_key(key); 
                
                        etcdserverpb::WatchResponse watchResponse;

                        grpc::CompletionQueue queue;
                        void *tag;
                        bool ok; 

                        bool wasShutdown = false;

                        auto channel = grpc::CreateChannel("127.0.0.1:2379", grpc::InsecureChannelCredentials());
                        auto initStub = etcdserverpb::KV::NewStub(channel);
                        grpc::ClientContext initCtx;
                        //All the requests in this app will die out after 24 hours
                        //, this is to avoid too many dead requests in server.
                        //(Please notice that if the deadline is set too early, then
                        //the watch stream will also be cut off at that time point)
                        initCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                        std::shared_ptr<
                                grpc::ClientAsyncResponseReader<etcdserverpb::RangeResponse>
                            > initData { initStub->AsyncRange(&initCtx, range, &queue) };
                        initData->Finish(&initResponse, &initStatus, (void *)1);

                        auto watchStub = etcdserverpb::Watch::NewStub(channel);
                        grpc::ClientContext watchCtx;
                        watchCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                
                        std::shared_ptr<
                            grpc::ClientAsyncReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>
                        > watchStream { watchStub->AsyncWatch(&watchCtx, &queue, (void *)2) };

                        while (true) {
                            auto status = queue.AsyncNext(&tag, &ok, std::chrono::system_clock::now()+std::chrono::milliseconds(1));
                            if (status == grpc::CompletionQueue::SHUTDOWN) {
                                wasShutdown = true;
                                break;
                            }
                            if (status == grpc::CompletionQueue::TIMEOUT) {
                                continue;
                            }
                            if (!ok) {
                                break;
                            }
                            auto tagNum = reinterpret_cast<intptr_t>(tag);
                            switch (tagNum) {
                            case 1:
                                {
                                    if (initResponse.kvs_size() > 0) {
                                        for (auto const &kv : initResponse.kvs()) {
                                            pub(std::string {kv.value()});
                                        }
                                    }
                                }
                                break;
                            case 2:
                                watchStream->Write(req, (void *)3);
                                watchStream->Read(&watchResponse, (void *)4);
                                break;
                            case 3:
                                watchStream->WritesDone((void *)5);
                                break;
                            case 4:
                                if (watchResponse.events_size() > 0) {
                                    for (auto const &ev : watchResponse.events()) {
                                        pub(std::string {ev.kv().value()});
                                    }
                                }
                                watchStream->Read(&watchResponse, (void *)4);  
                                break;
                            default:
                                break;
                            }
                        }
                        if (!wasShutdown) {
                            grpc::Status status;
                            watchStream->Finish(&status, (void *)6);
                        }
                    }
                    , infra::LiftParameters<typename M::TimePoint>().SuggestThreaded(true)
                );
            }
            static std::shared_ptr<typename M::template Importer<std::tuple<std::string, std::string>>> keyRangeImporter(std::string const &rangeStart, std::string const &rangeEnd) {
                return M::template simpleImporter<std::tuple<std::string, std::string>>(
                    [rangeStart, rangeEnd](typename M::template PublisherCall<std::tuple<std::string, std::string>> const &pub) {
                        etcdserverpb::RangeRequest range;
                        range.set_key(rangeStart);
                        range.set_range_end(rangeEnd);

                        etcdserverpb::RangeResponse initResponse;
                        grpc::Status initStatus;

                        etcdserverpb::WatchRequest req;
                        auto *r = req.mutable_create_request();
                        r->set_key(rangeStart); 
                        r->set_range_end(rangeEnd);
                
                        etcdserverpb::WatchResponse watchResponse;

                        grpc::CompletionQueue queue;
                        void *tag;
                        bool ok; 

                        bool wasShutdown = false;

                        auto channel = grpc::CreateChannel("127.0.0.1:2379", grpc::InsecureChannelCredentials());
                        auto initStub = etcdserverpb::KV::NewStub(channel);
                        grpc::ClientContext initCtx;
                        //All the requests in this app will die out after 24 hours
                        //, this is to avoid too many dead requests in server.
                        //(Please notice that if the deadline is set too early, then
                        //the watch stream will also be cut off at that time point)
                        initCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                        std::shared_ptr<
                                grpc::ClientAsyncResponseReader<etcdserverpb::RangeResponse>
                            > initData { initStub->AsyncRange(&initCtx, range, &queue) };
                        initData->Finish(&initResponse, &initStatus, (void *)1);

                        auto watchStub = etcdserverpb::Watch::NewStub(channel);
                        grpc::ClientContext watchCtx;
                        watchCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                
                        std::shared_ptr<
                            grpc::ClientAsyncReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>
                        > watchStream { watchStub->AsyncWatch(&watchCtx, &queue, (void *)2) };

                        while (true) {
                            auto status = queue.AsyncNext(&tag, &ok, std::chrono::system_clock::now()+std::chrono::milliseconds(1));
                            if (status == grpc::CompletionQueue::SHUTDOWN) {
                                wasShutdown = true;
                                break;
                            }
                            if (status == grpc::CompletionQueue::TIMEOUT) {
                                continue;
                            }
                            if (!ok) {
                                break;
                            }
                            auto tagNum = reinterpret_cast<intptr_t>(tag);
                            switch (tagNum) {
                            case 1:
                                {
                                    if (initResponse.kvs_size() > 0) {
                                        for (auto const &kv : initResponse.kvs()) {
                                            pub({kv.key(), kv.value()});
                                        }
                                    }
                                }
                                break;
                            case 2:
                                watchStream->Write(req, (void *)3);
                                watchStream->Read(&watchResponse, (void *)4);
                                break;
                            case 3:
                                watchStream->WritesDone((void *)5);
                                break;
                            case 4:
                                if (watchResponse.events_size() > 0) {
                                    for (auto const &ev : watchResponse.events()) {
                                        pub({ev.kv().key(), ev.kv().value()});
                                    }
                                }
                                watchStream->Read(&watchResponse, (void *)4);  
                                break;
                            default:
                                break;
                            }
                        }
                        if (!wasShutdown) {
                            grpc::Status status;
                            watchStream->Finish(&status, (void *)6);
                        }
                    }
                    , infra::LiftParameters<typename M::TimePoint>().SuggestThreaded(true)
                );
            }
        };
    }
} } } } }

#endif