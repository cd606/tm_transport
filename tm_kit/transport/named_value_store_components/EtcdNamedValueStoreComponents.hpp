#ifndef TM_KIT_TRANSPORT_ETCD_NAMED_VALUE_STORE_COMPONENTS_HPP_
#define TM_KIT_TRANSPORT_ETCD_NAMED_VALUE_STORE_COMPONENTS_HPP_

#include <tm_kit/basic/transaction/named_value_store/DataModel.hpp>

#include <grpcpp/grpcpp.h>
#ifdef _MSC_VER
#undef DELETE
#endif
#include <libetcd/rpc.grpc.pb.h>
#include <libetcd/kv.pb.h>

#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace named_value_store_components {
    namespace etcd {
        template <class Data>
        class DSComponent : public basic::transaction::current::DataStreamEnvComponent<
            basic::transaction::named_value_store::DI<Data>
        > {
        private:
            using DI = basic::transaction::named_value_store::DI<Data>;
            using Delta = basic::transaction::named_value_store::CollectionDelta<Data>;
            using Callback = typename basic::transaction::current::DataStreamEnvComponent<
                basic::transaction::named_value_store::DI<Data>
            >::Callback;

            std::shared_ptr<grpc::ChannelInterface> channel_;
            std::function<void(std::string)> logger_;
            std::thread watchThread_;
            std::atomic<bool> running_;

            Callback *watchListener_;

            std::string storagePrefix_;

            std::string lockNumberKey_;
            std::string lockQueueKey_;
            std::atomic<int64_t> *lockQueueVersion_, *lockQueueRevision_;

            std::optional<typename DI::OneDeltaUpdateItem> createDeltaUpdate(mvccpb::Event::EventType eventType, mvccpb::KeyValue const &kv, int64_t revision) {
                if (kv.key() == storagePrefix_+":"+lockQueueKey_) {
                    if (lockQueueRevision_) {
                        lockQueueRevision_->store(revision);
                    }
                    if (lockQueueVersion_) {
                        lockQueueVersion_->store(kv.version());
                    }
                    return std::nullopt;
                } else if (kv.key() == storagePrefix_+":"+lockNumberKey_) {
                    //do nothing
                } else if (boost::starts_with(kv.key(), storagePrefix_+":")) {
                    std::string realKey = kv.key().substr(storagePrefix_.length()+1);
                    if (eventType == mvccpb::Event::PUT) {
                        auto parseRes = basic::bytedata_utils::RunDeserializer<Data>::apply(kv.value());
                        if (parseRes) {
                            return typename DI::OneDeltaUpdateItem {
                                basic::VoidStruct {}
                                , revision 
                                , Delta {
                                    {}
                                    , {
                                        {realKey, std::move(*parseRes)}
                                    }
                                }
                            };
                        } else {
                            return std::nullopt;
                        }
                    } else {
                        return typename DI::OneDeltaUpdateItem {
                            basic::VoidStruct {}
                            , revision 
                            , Delta {
                                {realKey}
                                , {}
                            }
                        };
                    }
                } else {
                    return std::nullopt;
                }
            }
            void runWatchThread() {
                etcdserverpb::RangeRequest range;
                range.set_key(storagePrefix_+":");
                range.set_range_end(storagePrefix_+";");

                etcdserverpb::RangeResponse initResponse;
                grpc::Status initStatus;

                etcdserverpb::WatchRequest req;
                auto *r = req.mutable_create_request();
                r->set_key(storagePrefix_+":");
                r->set_range_end(storagePrefix_+";"); 
        
                etcdserverpb::WatchResponse watchResponse;

                grpc::CompletionQueue queue;
                void *tag;
                bool ok; 

                bool wasShutdown = false;

                auto initStub = etcdserverpb::KV::NewStub(channel_);
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

                auto watchStub = etcdserverpb::Watch::NewStub(channel_);
                grpc::ClientContext watchCtx;
                watchCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
        
                std::shared_ptr<
                    grpc::ClientAsyncReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>
                > watchStream { watchStub->AsyncWatch(&watchCtx, &queue, (void *)2) };

                while (running_) {
                    auto status = queue.AsyncNext(&tag, &ok, std::chrono::system_clock::now()+std::chrono::milliseconds(1));
                    if (!running_) {
                        break;
                    }
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
                    std::cerr << "Got tag num " << (int) tagNum << '\n';
                    switch (tagNum) {
                    case 1:
                        {
                            if (initResponse.kvs_size() > 0) {
                                std::vector<typename DI::OneUpdateItem> updates;
                                int64_t revision = watchResponse.header().revision();
                                for (auto const &kv : initResponse.kvs()) {
                                    auto delta = createDeltaUpdate(mvccpb::Event::PUT, kv, revision);
                                    if (delta) {
                                        updates.push_back(*delta);
                                    }
                                }
                                watchListener_->onUpdate(typename DI::Update {
                                    revision
                                    , std::move(updates)
                                });
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
                        { 
                            if (watchResponse.events_size() > 0) {
                                std::vector<typename DI::OneUpdateItem> updates;
                                int64_t revision = watchResponse.header().revision();
                                for (auto const &ev : watchResponse.events()) {
                                    auto delta = createDeltaUpdate(ev.type(), ev.kv(), revision);
                                    if (delta) {
                                        updates.push_back(*delta);
                                    }
                                }
                                watchListener_->onUpdate(typename DI::Update {
                                    revision
                                    , std::move(updates)
                                });
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
        public:
            DSComponent()
                : channel_(), logger_(), watchThread_(), running_(false)
                , watchListener_(nullptr)
                , storagePrefix_(), lockNumberKey_(), lockQueueKey_()
                , lockQueueVersion_(nullptr), lockQueueRevision_(nullptr)
            {}
            DSComponent &operator=(DSComponent &&c) {
                if (this != &c) {
                    //only copy these!
                    channel_ = std::move(c.channel_);
                    logger_ = std::move(c.logger_);
                    storagePrefix_ = std::move(c.storagePrefix_);
                    lockNumberKey_ = std::move(c.lockNumberKey_);
                    lockQueueKey_ = std::move(c.lockQueueKey_);
                    lockQueueVersion_ = std::move(c.lockQueueVersion_);
                    lockQueueRevision_ = std::move(c.lockQueueRevision_);
                }
                return *this;
            } 
            DSComponent(std::shared_ptr<grpc::ChannelInterface> const &channel, std::function<void(std::string)> const &logger, std::string const &storagePrefix, std::string const &lockNumberKey = "", std::string const &lockQueueKey = "", std::atomic<int64_t> *lockQueueVersion = nullptr, std::atomic<int64_t> *lockQueueRevision = nullptr) 
                : channel_(channel), logger_(logger)
                , watchThread_(), running_(false)
                , watchListener_(nullptr)
                , storagePrefix_(storagePrefix)
                , lockNumberKey_(lockNumberKey)
                , lockQueueKey_(lockQueueKey)
                , lockQueueVersion_(lockQueueVersion)
                , lockQueueRevision_(lockQueueRevision)
            {}
            virtual ~DSComponent() {
                if (running_) {
                    running_ = false;
                    watchThread_.join();
                }
            }
            void initialize(Callback *cb) {
                watchListener_ = cb;

                //This is to make sure there is a data entry
                //if the database has never been populated
                watchListener_->onUpdate(typename DI::Update {
                    0
                    , std::vector<typename DI::OneUpdateItem> {
                        typename DI::OneFullUpdateItem {
                            basic::VoidStruct {}
                            , 0
                            , basic::transaction::named_value_store::Collection<Data>()
                        }
                    }
                });

                running_ = true;
                watchThread_ = std::thread(&DSComponent::runWatchThread, this);
                watchThread_.detach();

                logger_("DS component started");
            }
        };

        //"None" means no inter-process lock is applied
        //"Compound" means using a two-integer lock that
        // we maintain by ourselves. This lock is lockout-free
        // , but is not robust against stopping failure. 
        // lock_helpers/LockHelper.ts is a tool to 
        // manage the lock from the outside.
        enum class LockChoice {
            None 
            , Compound
        };

        template <class Data>
        class THComponent : public basic::transaction::current::TransactionEnvComponent<
            basic::transaction::named_value_store::TI<Data>
        > {
        private:
            using TI = basic::transaction::named_value_store::TI<Data>;
            LockChoice lockChoice_;
            std::shared_ptr<grpc::ChannelInterface> channel_;
            std::function<void(std::string)> logger_;

            std::string storagePrefix_;
            std::string lockNumberKey_, lockQueueKey_;
            std::atomic<int64_t> const *lockQueueVersion_;
            std::atomic<int64_t> const *lockQueueRevision_;

            std::unique_ptr<etcdserverpb::KV::Stub> stub_;

            int64_t acquireCompundLock();
            int64_t releaseCompoundLock();
        public:
            THComponent()
                : lockChoice_(LockChoice::None), channel_(), logger_(), storagePrefix_(), lockNumberKey_(), lockQueueKey_(), lockQueueVersion_(nullptr), lockQueueRevision_(nullptr), stub_()
            {}
            THComponent &operator=(THComponent &&c) {
                if (this != &c) {
                    //only copy these!
                    lockChoice_ = c.lockChoice_;
                    channel_ = std::move(c.channel_);
                    logger_ = std::move(c.logger_);
                    storagePrefix_ = std::move(c.storagePrefix_);
                    lockNumberKey_ = std::move(c.lockNumberKey_);
                    lockQueueKey_ = std::move(c.lockQueueKey_);
                    lockQueueVersion_ = std::move(c.lockQueueVersion_);
                    lockQueueRevision_ = std::move(c.lockQueueRevision_);
                    stub_ = std::move(c.stub_);
                }
                return *this;
            } 
            THComponent(LockChoice lockChoice, std::shared_ptr<grpc::ChannelInterface> const &channel, std::function<void(std::string)> const &logger, std::string const &storagePrefix, std::string const &lockNumberKey = "", std::string const &lockQueueKey = "", std::atomic<int64_t> const *lockQueueVersion = nullptr, std::atomic<int64_t> const *lockQueueRevision = nullptr) 
                : lockChoice_(lockChoice), channel_(channel), logger_(logger), storagePrefix_(storagePrefix), lockNumberKey_(lockNumberKey), lockQueueKey_(lockQueueKey), lockQueueVersion_(lockQueueVersion), lockQueueRevision_(lockQueueRevision), stub_(etcdserverpb::KV::NewStub(channel_))
            {}
            virtual ~THComponent() {
            }
            typename TI::GlobalVersion acquireLock(std::string const &account, typename TI::Key const &, typename TI::DataDelta const *) override final {
                switch (lockChoice_) {
                case LockChoice::None: 
                    return 0;
                    break;
                case LockChoice::Compound:
                    {
                        if (lockQueueRevision_ == nullptr || lockQueueVersion_ == nullptr) {
                            return 0;
                        }
                        int64_t numVersion = 0;
                        int64_t numRevision = 0;
                        do {
                            etcdserverpb::TxnRequest txn;
                            auto *action = txn.add_success();
                            auto *put = action->mutable_request_put();
                            put->set_key(storagePrefix_+":"+lockNumberKey_);
                            action = txn.add_success();
                            auto *get = action->mutable_request_range();
                            get->set_key(storagePrefix_+":"+lockNumberKey_);

                            etcdserverpb::TxnResponse txnResp;
                            grpc::ClientContext txnCtx;
                            txnCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                            stub_->Txn(&txnCtx, txn, &txnResp);

                            if (txnResp.succeeded() && txnResp.responses_size() == 2) {
                                numVersion = txnResp.responses(1).response_range().kvs(0).version();
                                numRevision = txnResp.header().revision();
                            }
                        } while (numVersion == 0);
                        --numVersion;
                        while (lockQueueVersion_->load() != numVersion) {
                        }
                        return std::max(lockQueueRevision_->load(), numRevision);
                    }
                    break;
                default:
                    return 0;
                    break;
                }
            }
            typename TI::GlobalVersion releaseLock(std::string const &account, typename TI::Key const &, typename TI::DataDelta const *) override final {
                switch (lockChoice_) {
                case LockChoice::None:
                    return 0;
                    break;
                case LockChoice::Compound:
                    {
                        if (lockQueueRevision_ == nullptr || lockQueueVersion_ == nullptr) {
                            return 0;
                        }
                        etcdserverpb::PutRequest p;
                        p.set_key(storagePrefix_+":"+lockQueueKey_);
                        etcdserverpb::PutResponse putResp;
                        grpc::ClientContext putCtx;
                        putCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                        stub_->Put(&putCtx, p, &putResp);

                        return putResp.header().revision();
                    }
                    break;
                default:
                    return 0;
                    break;
                }
            }
            typename TI::TransactionResponse handleInsert(std::string const &account, typename TI::Key const &key, typename TI::Data const &data) override final {
                return {0, basic::transaction::current::RequestDecision::FailureConsistency};
            }
            typename TI::TransactionResponse handleUpdate(std::string const &account, typename TI::Key const &key, typename std::optional<typename TI::VersionSlice> const &updateVersionSlice, typename TI::ProcessedUpdate const &dataDelta) override final {
                etcdserverpb::TxnRequest txn;
                for (auto const &delKey : dataDelta.deletes) {
                    auto *action = txn.add_success();
                    auto *del = action->mutable_request_delete_range();
                    del->set_key(storagePrefix_+":"+delKey);
                }
                for (auto const &insUpd : dataDelta.inserts_updates) {
                    auto *action = txn.add_success();
                    auto *put = action->mutable_request_put();
                    put->set_key(storagePrefix_+":"+std::get<0>(insUpd));
                    put->set_value(basic::bytedata_utils::RunSerializer<Data>::apply(std::get<1>(insUpd)));
                }

                etcdserverpb::TxnResponse txnResp;
                grpc::ClientContext txnCtx;
                txnCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                stub_->Txn(&txnCtx, txn, &txnResp);

                return {txnResp.header().revision(), basic::transaction::v2::RequestDecision::Success};
            }
            typename TI::TransactionResponse handleDelete(std::string const &account, typename TI::Key const &key, std::optional<typename TI::Version> const &versionToDelete) override final {
                return {0, basic::transaction::current::RequestDecision::FailureConsistency};
            }
        };
    }
} } } } }

#endif