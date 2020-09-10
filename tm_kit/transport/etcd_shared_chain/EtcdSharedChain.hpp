#ifndef TM_KIT_TRANSPORT_ETCD_SHARED_CHAIN_HPP_
#define TM_KIT_TRANSPORT_ETCD_SHARED_CHAIN_HPP_

#include <tm_kit/basic/simple_shared_chain/ChainReader.hpp>
#include <tm_kit/basic/simple_shared_chain/ChainWriter.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/SerializationHelperMacros.hpp>

#ifdef _MSC_VER
#include <winsock2.h>
#undef min
#undef max
#endif
#include <hiredis/hiredis.h>

#include <grpcpp/grpcpp.h>
#ifdef _MSC_VER
#undef DELETE
#endif
#include <libetcd/rpc.grpc.pb.h>
#include <libetcd/kv.pb.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace etcd_shared_chain {
    #define ChainItemFields \
        ((int64_t, revision)) \
        ((std::string, id)) \
        ((T, data)) \
        ((std::string, nextID)) 
    
    #define ChainStorageFields \
        ((T, data)) \
        ((std::string, nextID)) 
    
    #define ChainRedisStorageFields \
        ((int64_t, revision)) \
        ((T, data)) \
        ((std::string, nextID)) 

    TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT(((typename, T)), ChainItem, ChainItemFields);
    TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT(((typename, T)), ChainStorage, ChainStorageFields);
    TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT(((typename, T)), ChainRedisStorage, ChainRedisStorageFields);
    TM_BASIC_CBOR_CAPABLE_TEMPLATE_EMPTY_STRUCT(((typename, T)), ChainUpdateNotification);
}}}}} 

TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT_SERIALIZE_NO_FIELD_NAMES(((typename, T)), dev::cd606::tm::transport::etcd_shared_chain::ChainItem, ChainItemFields);
TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT_SERIALIZE_NO_FIELD_NAMES(((typename, T)), dev::cd606::tm::transport::etcd_shared_chain::ChainStorage, ChainStorageFields);
TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT_SERIALIZE_NO_FIELD_NAMES(((typename, T)), dev::cd606::tm::transport::etcd_shared_chain::ChainRedisStorage, ChainRedisStorageFields);
TM_BASIC_CBOR_CAPABLE_TEMPLATE_EMPTY_STRUCT_SERIALIZE_NO_FIELD_NAMES(((typename, T)), dev::cd606::tm::transport::etcd_shared_chain::ChainUpdateNotification);

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace etcd_shared_chain {
    struct EtcdChainConfiguration {
        std::shared_ptr<grpc::ChannelInterface> etcdChannel {};
        std::string headKey="";
        bool saveDataOnSeparateStorage=false;

        std::string chainPrefix="shared_chain_test";
        std::string dataPrefix="shared_chain_test_data";
        std::string extraDataPrefix="shared_chain_test_extra_data";

        std::string redisServerAddr="127.0.0.1:6379";
        bool duplicateFromRedis=false;
        uint16_t redisTTLSeconds = 0;
        bool automaticallyDuplicateToRedis=false;

        EtcdChainConfiguration() = default;
        EtcdChainConfiguration(EtcdChainConfiguration const &) = default;
        EtcdChainConfiguration &operator=(EtcdChainConfiguration const &) = default;
        EtcdChainConfiguration(EtcdChainConfiguration &&) = default;
        EtcdChainConfiguration &operator=(EtcdChainConfiguration &&) = default;
        EtcdChainConfiguration &InsecureEtcdServerAddr(std::string const &addr="127.0.0.1:2379") {
            etcdChannel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
            return *this;
        }
        EtcdChainConfiguration &EtcdChannel(std::shared_ptr<grpc::ChannelInterface> const &channel) {
            etcdChannel = channel;
            return *this;
        }
        EtcdChainConfiguration &HeadKey(std::string const &key) {
            headKey = key;
            return *this;
        }
        EtcdChainConfiguration &SaveDataOnSeparateStorage(bool b) {
            saveDataOnSeparateStorage = b;
            return *this;
        }
        EtcdChainConfiguration &ChainPrefix(std::string const &p) {
            chainPrefix = p;
            return *this;
        }
        EtcdChainConfiguration &DataPrefix(std::string const &p) {
            dataPrefix = p;
            return *this;
        }
        EtcdChainConfiguration &ExtraDataPrefix(std::string const &p) {
            extraDataPrefix = p;
            return *this;
        }
        EtcdChainConfiguration &RedisServerAddr(std::string const &addr) {
            redisServerAddr = addr;
            return *this;
        }
        EtcdChainConfiguration &DuplicateFromRedis(bool b) {
            duplicateFromRedis = b;
            return *this;
        }
        EtcdChainConfiguration &RedisTTLSeconds(uint16_t s) {
            redisTTLSeconds = s;
            return *this;
        }
        EtcdChainConfiguration &AutomaticallyDuplicateToRedis(bool b) {
            automaticallyDuplicateToRedis = b;
            return *this;
        }
    };

    class EtcdChainException : public std::runtime_error {
    public:
        EtcdChainException(std::string const &s) : std::runtime_error(s) {}
    };

    template <class T>
    class EtcdChain {
    private:
        const EtcdChainConfiguration configuration_;
        std::function<void()> updateTriggerFunc_;
        
        std::shared_ptr<grpc::ChannelInterface> channel_;
        std::unique_ptr<etcdserverpb::KV::Stub> stub_;
        std::atomic<int64_t> latestModRevision_;
        std::atomic<bool> watchThreadRunning_;
        std::thread watchThread_;

        redisContext *redisCtx_;
        std::mutex redisMutex_;

        void runWatchThread() {
            etcdserverpb::WatchRequest req;
            auto *r = req.mutable_create_request();
            r->set_key(configuration_.chainPrefix+":");
            r->set_range_end(configuration_.chainPrefix+";");

            etcdserverpb::WatchResponse watchResponse;
            grpc::CompletionQueue queue;
            void *tag;
            bool ok; 

            auto watchStub = etcdserverpb::Watch::NewStub(channel_);
            grpc::ClientContext watchCtx;
            watchCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
        
            std::shared_ptr<
                grpc::ClientAsyncReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>
            > watchStream { watchStub->AsyncWatch(&watchCtx, &queue, (void *)1) };

            while (watchThreadRunning_) {
                auto status = queue.AsyncNext(&tag, &ok, std::chrono::system_clock::now()+std::chrono::milliseconds(1));
                if (!watchThreadRunning_) {
                    break;
                }
                if (status == grpc::CompletionQueue::SHUTDOWN) {
                    watchThreadRunning_ = false;
                    break;
                }
                if (status == grpc::CompletionQueue::TIMEOUT) {
                    continue;
                }
                if (!ok) {
                    watchThreadRunning_ = false;
                    break;
                }
                auto tagNum = reinterpret_cast<intptr_t>(tag);
                switch (tagNum) {
                case 1:
                    watchStream->Write(req, (void *)2);
                    watchStream->Read(&watchResponse, (void *)3);
                    break;
                case 2:
                    watchStream->WritesDone((void *)4);
                    break;
                case 3:
                    if (watchResponse.events_size() > 0) {
                        latestModRevision_.store(watchResponse.header().revision(), std::memory_order_release);
                        if (updateTriggerFunc_) {
                            updateTriggerFunc_();
                        }
                    }
                    watchStream->Read(&watchResponse, (void *)3);  
                    break;
                default:
                    break;
                }
            }
        }
    public:
        using ItemType = ChainItem<T>;
        using MapData = ChainStorage<T>;
        EtcdChain(EtcdChainConfiguration const &config) :
            configuration_(config) 
            , updateTriggerFunc_()
            , channel_(config.etcdChannel?config.etcdChannel:(EtcdChainConfiguration().InsecureEtcdServerAddr().etcdChannel))
            , stub_(etcdserverpb::KV::NewStub(channel_))
            , latestModRevision_(0), watchThreadRunning_(true), watchThread_()
            , redisCtx_(nullptr), redisMutex_()
        {
            watchThread_ = std::thread(&EtcdChain::runWatchThread, this);
            watchThread_.detach();
            if (configuration_.duplicateFromRedis || configuration_.automaticallyDuplicateToRedis) {
                auto idx = configuration_.redisServerAddr.find(':');
                std::lock_guard<std::mutex> _(redisMutex_);
                redisCtx_ = redisConnect(
                    configuration_.redisServerAddr.substr(
                        0, idx
                    ).c_str()
                    , (
                        idx == std::string::npos
                        ?
                        6379
                        :
                        std::stoi(
                            configuration_.redisServerAddr.substr(idx+1)
                        )
                    )
                );
            }
        }
        ~EtcdChain() {
            if (watchThreadRunning_) {
                watchThreadRunning_ = false;
                if (watchThread_.joinable()) {
                    watchThread_.join();
                }
            }
            if (configuration_.duplicateFromRedis) {
                std::lock_guard<std::mutex> _(redisMutex_);
                if (redisCtx_) {
                    redisFree(redisCtx_);
                }
            }   
        }
        void setUpdateTriggerFunc(std::function<void()> f) {
            if (updateTriggerFunc_) {
                throw EtcdChainException("Duplicate attempt to set update trigger function for EtcdChain");
            }
            updateTriggerFunc_ = f;
            if (updateTriggerFunc_) {
                updateTriggerFunc_();
            }
        }
        ItemType head(void *) {
            static const std::string headKeyStr = configuration_.chainPrefix+":"+configuration_.headKey;
            if (configuration_.duplicateFromRedis) {
                redisReply *r = nullptr;
                {
                    std::lock_guard<std::mutex> _(redisMutex_);
                    r = (redisReply *) redisCommand(
                        redisCtx_, "GET %s", headKeyStr.c_str()
                    );
                }
                if (r != nullptr) {
                    if (r->type == REDIS_REPLY_STRING) {
                        auto data = basic::bytedata_utils::RunCBORDeserializer<ChainRedisStorage<T>>::apply(
                            std::string_view(r->str, r->len), 0
                        );
                        if (data && std::get<1>(*data) == r->len) {
                            freeReplyObject((void *) r);
                            auto const &s = std::get<0>(*data);
                            return ItemType {s.revision, configuration_.headKey, std::move(s.data), s.nextID};
                        }
                    }
                    freeReplyObject((void *) r);
                }
            }
            if (configuration_.saveDataOnSeparateStorage) {
                etcdserverpb::TxnRequest txn;
                auto *cmp = txn.add_compare();
                cmp->set_result(etcdserverpb::Compare::GREATER);
                cmp->set_target(etcdserverpb::Compare::VERSION);
                cmp->set_key(headKeyStr);
                cmp->set_version(0);
                auto *action = txn.add_success();
                auto *get = action->mutable_request_range();
                get->set_key(headKeyStr);
                action = txn.add_failure();
                auto *put = action->mutable_request_put();
                put->set_key(headKeyStr);
                put->set_value("");
                action = txn.add_failure();
                get = action->mutable_request_range();
                get->set_key(headKeyStr);

                etcdserverpb::TxnResponse txnResp;
                grpc::ClientContext txnCtx;
                txnCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                stub_->Txn(&txnCtx, txn, &txnResp);

                auto const &kv = txnResp.responses(txnResp.succeeded()?0:1).response_range().kvs(0);
                return ItemType {kv.mod_revision(), configuration_.headKey, T{}, kv.value()};
            } else {
                static const std::string emptyHeadDataStr = basic::bytedata_utils::RunSerializer<basic::CBOR<MapData>>::apply({MapData {}}); 

                etcdserverpb::TxnRequest txn;
                auto *cmp = txn.add_compare();
                cmp->set_result(etcdserverpb::Compare::GREATER);
                cmp->set_target(etcdserverpb::Compare::VERSION);
                cmp->set_key(headKeyStr);
                cmp->set_version(0);
                auto *action = txn.add_success();
                auto *get = action->mutable_request_range();
                get->set_key(headKeyStr);
                action = txn.add_failure();
                auto *put = action->mutable_request_put();
                put->set_key(headKeyStr);
                put->set_value(emptyHeadDataStr);
                action = txn.add_failure();
                get = action->mutable_request_range();
                get->set_key(headKeyStr);

                etcdserverpb::TxnResponse txnResp;
                grpc::ClientContext txnCtx;
                txnCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                stub_->Txn(&txnCtx, txn, &txnResp);

                if (txnResp.succeeded()) {
                    auto &kv = txnResp.responses(0).response_range().kvs(0);
                    auto mapData = basic::bytedata_utils::RunDeserializer<basic::CBOR<MapData>>::apply(
                        kv.value()
                    );
                    if (mapData) {
                        return ItemType {kv.mod_revision(), configuration_.headKey, mapData->value.data, mapData->value.nextID};
                    } else {
                        return ItemType {};
                    }
                } else {
                    auto &kv = txnResp.responses(1).response_range().kvs(0);
                    auto mapData = basic::bytedata_utils::RunDeserializer<basic::CBOR<MapData>>::apply(
                        kv.value()
                    );
                    if (mapData) {
                        return ItemType {kv.mod_revision(), configuration_.headKey, mapData->value.data, mapData->value.nextID};
                    } else {
                        return ItemType {};
                    }
                }
            }
        }
        ItemType loadUntil(void *env, std::string const &id) {
            if (id == "") {
                return head(env);
            }
            if (configuration_.duplicateFromRedis) {
                std::string key = configuration_.chainPrefix+":"+id;
                redisReply *r = nullptr;
                {
                    std::lock_guard<std::mutex> _(redisMutex_);
                    r = (redisReply *) redisCommand(
                        redisCtx_, "GET %s", key.c_str()
                    );
                }
                if (r != nullptr) {
                    if (r->type == REDIS_REPLY_STRING) {
                        auto data = basic::bytedata_utils::RunCBORDeserializer<ChainRedisStorage<T>>::apply(
                            std::string_view(r->str, r->len), 0
                        );
                        if (data && std::get<1>(*data) == r->len) {
                            freeReplyObject((void *) r);
                            auto &s = std::get<0>(*data);
                            return ItemType {s.revision, id, std::move(s.data), s.nextID};
                        }
                    }
                    freeReplyObject((void *) r);
                }
            }
            if (configuration_.saveDataOnSeparateStorage) {  
                etcdserverpb::TxnRequest txn;
                auto *action = txn.add_success();
                auto *get = action->mutable_request_range();
                get->set_key(configuration_.chainPrefix+":"+id);
                action = txn.add_success();
                get = action->mutable_request_range();
                get->set_key(configuration_.dataPrefix+":"+id);

                etcdserverpb::TxnResponse txnResp;
                grpc::ClientContext txnCtx;
                txnCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                stub_->Txn(&txnCtx, txn, &txnResp);

                if (txnResp.succeeded()) {
                    if (txnResp.responses_size() < 2) {
                        throw EtcdChainException("LoadUntil Error! No record for "+configuration_.chainPrefix+":"+id+" or "+configuration_.dataPrefix+":"+id);
                    } 
                    if (txnResp.responses(0).response_range().kvs_size() == 0) {
                        throw EtcdChainException("LoadUntil Error! No record for "+configuration_.chainPrefix+":"+id);
                    }
                    if (txnResp.responses(1).response_range().kvs_size() == 0) {
                        throw EtcdChainException("LoadUntil Error! No record for "+configuration_.dataPrefix+":"+id);
                    }
                    auto const &dataKV = txnResp.responses(1).response_range().kvs(0);
                    auto data = basic::bytedata_utils::RunDeserializer<basic::CBOR<T>>::apply(
                        dataKV.value()
                    );
                    if (data) {
                        return ItemType {
                            dataKV.mod_revision()
                            , id
                            , std::move(data->value)
                            , txnResp.responses(0).response_range().kvs(0).value()
                        };
                    } else {
                        throw EtcdChainException("LoadUntil Error! Bad record for "+configuration_.dataPrefix+":"+id);
                    }
                } else {
                    throw EtcdChainException("LoadUntil Error! No record for "+configuration_.chainPrefix+":"+id+" or "+configuration_.dataPrefix+":"+id);
                }
            } else {
                etcdserverpb::RangeRequest range;
                range.set_key(configuration_.chainPrefix+":"+id);

                etcdserverpb::RangeResponse rangeResp;
                grpc::ClientContext rangeCtx;
                rangeCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                stub_->Range(&rangeCtx, range, &rangeResp);

                if (rangeResp.kvs_size() == 0) {
                    throw EtcdChainException("LoadUntil Error! No record for "+configuration_.chainPrefix+":"+id);
                }

                auto &kv = rangeResp.kvs(0);
                auto mapData = basic::bytedata_utils::RunDeserializer<basic::CBOR<MapData>>::apply(
                    kv.value()
                );
                if (mapData) {
                    return ItemType {kv.mod_revision(), id, mapData->value.data, mapData->value.nextID};
                } else {
                    throw EtcdChainException("LoadUntil Error! Bad record for "+configuration_.chainPrefix+":"+id);
                }
            }
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            if (current.nextID == "") {
                auto latestRev = latestModRevision_.load(std::memory_order_acquire);
                if (latestRev > 0 && current.revision >= latestRev) {
                    return std::nullopt;
                }
            }
            if (configuration_.duplicateFromRedis) {
                if (current.nextID != "") {
                    std::string key = configuration_.chainPrefix+":"+current.nextID;
                    redisReply *r = nullptr;
                    {
                        std::lock_guard<std::mutex> _(redisMutex_);
                        r = (redisReply *) redisCommand(
                            redisCtx_, "GET %s", key.c_str()
                        );
                    }
                    if (r != nullptr) {
                        if (r->type == REDIS_REPLY_STRING) {
                            auto nextData = basic::bytedata_utils::RunCBORDeserializer<ChainRedisStorage<T>>::apply(
                                std::string_view(r->str, r->len), 0
                            );
                            if (nextData && std::get<1>(*nextData) == r->len) {
                                freeReplyObject((void *) r);
                                auto &nextS = std::get<0>(*nextData);
                                return ItemType {nextS.revision, current.nextID, std::move(nextS.data), nextS.nextID};
                            }
                        }
                        freeReplyObject((void *) r);
                    }
                } else {
                    std::string key = configuration_.chainPrefix+":"+current.id;
                    redisReply *r = nullptr;
                    {
                        std::lock_guard<std::mutex> _(redisMutex_);
                        r = (redisReply *) redisCommand(
                            redisCtx_, "GET %s", key.c_str()
                        );
                    }
                    if (r != nullptr) {
                        if (r->type == REDIS_REPLY_STRING) {
                            auto data = basic::bytedata_utils::RunCBORDeserializer<ChainRedisStorage<T>>::apply(
                                std::string_view(r->str, r->len), 0
                            );
                            if (data && std::get<1>(*data) == r->len) {
                                freeReplyObject((void *) r);
                                r = nullptr;
                                auto const &s = std::get<0>(*data);
                                if (s.nextID != "") {
                                    key = configuration_.chainPrefix+":"+s.nextID;
                                    {
                                        std::lock_guard<std::mutex> _(redisMutex_);
                                        r = (redisReply *) redisCommand(
                                            redisCtx_, "GET %s", key.c_str()
                                        );
                                    }
                                    if (r != nullptr) {
                                        if (r->type == REDIS_REPLY_STRING) {
                                            auto nextData = basic::bytedata_utils::RunCBORDeserializer<ChainRedisStorage<T>>::apply(
                                                std::string_view(r->str, r->len), 0
                                            );
                                            if (nextData && std::get<1>(*nextData) == r->len) {
                                                freeReplyObject((void *) r);
                                                auto &nextS = std::get<0>(*nextData);
                                                return ItemType {nextS.revision, s.nextID, std::move(nextS.data), nextS.nextID};
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (r != nullptr) {
                            freeReplyObject((void *) r);
                        }
                    }
                }
            }
            if (configuration_.saveDataOnSeparateStorage) {  
                std::string nextID = current.nextID;
                if (nextID == "") {         
                    etcdserverpb::RangeRequest range;
                    range.set_key(configuration_.chainPrefix+":"+current.id);

                    etcdserverpb::RangeResponse rangeResp;
                    grpc::ClientContext rangeCtx;
                    rangeCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                    stub_->Range(&rangeCtx, range, &rangeResp);

                    if (rangeResp.kvs_size() == 0) {
                        throw EtcdChainException("FetchNext Error! No record for "+configuration_.chainPrefix+":"+current.id);
                    }
                    auto const &kv = rangeResp.kvs(0);
                    nextID = kv.value();
                }
                
                if (nextID != "") {
                    etcdserverpb::TxnRequest txn;
                    auto *action = txn.add_success();
                    auto *get = action->mutable_request_range();
                    get->set_key(configuration_.chainPrefix+":"+nextID);
                    action = txn.add_success();
                    get = action->mutable_request_range();
                    get->set_key(configuration_.dataPrefix+":"+nextID);

                    etcdserverpb::TxnResponse txnResp;
                    grpc::ClientContext txnCtx;
                    txnCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                    stub_->Txn(&txnCtx, txn, &txnResp);

                    if (txnResp.succeeded()) {
                        if (txnResp.responses_size() < 2) {
                            throw EtcdChainException("FetchNext Error! No record for "+configuration_.chainPrefix+":"+nextID+" or "+configuration_.dataPrefix+":"+nextID);
                        } 
                        if (txnResp.responses(0).response_range().kvs_size() == 0) {
                            throw EtcdChainException("FetchNext Error! No record for "+configuration_.chainPrefix+":"+nextID);
                        }
                        if (txnResp.responses(1).response_range().kvs_size() == 0) {
                            throw EtcdChainException("FetchNext Error! No record for "+configuration_.dataPrefix+":"+nextID);
                        }
                        auto const &dataKV = txnResp.responses(1).response_range().kvs(0);
                        auto data = basic::bytedata_utils::RunDeserializer<basic::CBOR<T>>::apply(
                            dataKV.value()
                        );
                        if (data) {
                            return ItemType {
                                dataKV.mod_revision()
                                , nextID
                                , std::move(data->value)
                                , txnResp.responses(0).response_range().kvs(0).value()
                            };
                        } else {
                            throw EtcdChainException("FetchNext Error! Bad record for "+configuration_.dataPrefix+":"+nextID);
                        }
                    } else {
                        throw EtcdChainException("Fetch Error! No record for "+configuration_.chainPrefix+":"+nextID);
                    }
                } else {
                    return std::nullopt;
                }
            } else {
                std::string nextID = current.nextID;
                if (nextID == "") {
                    etcdserverpb::RangeRequest range;
                    range.set_key(configuration_.chainPrefix+":"+current.id);

                    etcdserverpb::RangeResponse rangeResp;
                    grpc::ClientContext rangeCtx;
                    rangeCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                    stub_->Range(&rangeCtx, range, &rangeResp);

                    if (rangeResp.kvs_size() == 0) {
                        throw EtcdChainException("FetchNext Error! No record for "+configuration_.chainPrefix+":"+current.id);
                    }
                    auto &kv = rangeResp.kvs(0);
                    auto mapData = basic::bytedata_utils::RunDeserializer<basic::CBOR<MapData>>::apply(
                        kv.value()
                    );
                    if (mapData) {
                        nextID = mapData->value.nextID;
                    } else {
                        throw EtcdChainException("FetchNext Error! Bad record for "+configuration_.chainPrefix+":"+current.id);
                    }
                }
                if (nextID != "") {
                    etcdserverpb::RangeRequest range2;
                    range2.set_key(configuration_.chainPrefix+":"+nextID);

                    etcdserverpb::RangeResponse rangeResp2;
                    grpc::ClientContext rangeCtx2;
                    rangeCtx2.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                    stub_->Range(&rangeCtx2, range2, &rangeResp2);

                    if (rangeResp2.kvs_size() == 0) {
                        throw EtcdChainException("FetchNext Error! No record for "+configuration_.chainPrefix+":"+nextID);
                    }

                    auto &kv2 = rangeResp2.kvs(0);
                    auto mapData = basic::bytedata_utils::RunDeserializer<basic::CBOR<MapData>>::apply(
                        kv2.value()
                    );
                    if (mapData) {
                        return ItemType {kv2.mod_revision(), nextID, mapData->value.data, mapData->value.nextID};
                    } else {
                        throw EtcdChainException("FetchNext Error! Bad record for "+configuration_.chainPrefix+":"+nextID);
                    }
                } else {
                    return std::nullopt;
                }
            }
        }
        bool appendAfter(ItemType const &current, ItemType &&toBeWritten) {
            if (current.nextID != "") {
                return false;
            } 
            std::string currentChainKey = configuration_.chainPrefix+":"+current.id;
            if (configuration_.saveDataOnSeparateStorage) {
                etcdserverpb::TxnRequest txn;
                auto *cmp = txn.add_compare();
                cmp->set_result(etcdserverpb::Compare::EQUAL);
                cmp->set_target(etcdserverpb::Compare::VALUE);
                cmp->set_key(currentChainKey);
                cmp->set_value("");
                auto *action = txn.add_success();
                auto *put = action->mutable_request_put();
                put->set_key(currentChainKey);
                put->set_value(toBeWritten.id); 
                action = txn.add_success();
                put = action->mutable_request_put();
                put->set_key(configuration_.dataPrefix+":"+toBeWritten.id);
                put->set_value(basic::bytedata_utils::RunSerializer<basic::CBOR<T>>::apply(basic::CBOR<T> {std::move(toBeWritten.data)})); 
                action = txn.add_success();
                put = action->mutable_request_put();
                put->set_key(configuration_.chainPrefix+":"+toBeWritten.id);
                put->set_value(""); 
                etcdserverpb::TxnResponse txnResp;
                grpc::ClientContext txnCtx;
                txnCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                stub_->Txn(&txnCtx, txn, &txnResp);

                bool ret = txnResp.succeeded();
                if (ret) {
                    auto rev = txnResp.responses(2).response_put().header().revision();
                    latestModRevision_.store(rev, std::memory_order_release);
                    if (configuration_.automaticallyDuplicateToRedis) {
                        duplicateToRedis(rev, current.id, current.data, toBeWritten.id);     
                    }
                }
                return ret;
            } else {
                etcdserverpb::TxnRequest txn;
                auto *cmp = txn.add_compare();
                cmp->set_result(etcdserverpb::Compare::EQUAL);
                cmp->set_target(etcdserverpb::Compare::MOD);
                cmp->set_key(currentChainKey);
                cmp->set_mod_revision(current.revision);
                auto *action = txn.add_success();
                auto *put = action->mutable_request_put();
                put->set_key(currentChainKey);
                put->set_value(basic::bytedata_utils::RunSerializer<basic::CBOR<MapData>>::apply(basic::CBOR<MapData> {MapData {current.data, toBeWritten.id}})); 
                action = txn.add_success();
                put = action->mutable_request_put();
                put->set_key(configuration_.chainPrefix+":"+toBeWritten.id);
                put->set_value(basic::bytedata_utils::RunSerializer<basic::CBOR<MapData>>::apply(basic::CBOR<MapData> {MapData {std::move(toBeWritten.data), ""}})); 
                
                etcdserverpb::TxnResponse txnResp;
                grpc::ClientContext txnCtx;
                txnCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
                stub_->Txn(&txnCtx, txn, &txnResp);
                bool ret = txnResp.succeeded();
                if (ret) {
                    auto rev = txnResp.responses(1).response_put().header().revision();
                    latestModRevision_.store(rev, std::memory_order_release);
                    if (configuration_.automaticallyDuplicateToRedis) {
                        duplicateToRedis(rev, current.id, current.data, toBeWritten.id);     
                    }
                }
                return ret;
            }
        }
        void duplicateToRedis(int64_t rev, std::string const &id, T const &data, std::string const &nextID) {
            duplicateToRedis(nullptr, rev, id, data, nextID, configuration_.redisTTLSeconds);
        }
        void duplicateToRedis(redisContext *ctx, int64_t rev, std::string const &id, T const &data, std::string const &nextID, uint16_t ttlSeconds) {
            std::string redisS = basic::bytedata_utils::RunSerializer<basic::CBOR<ChainRedisStorage<T>>>::apply(
                basic::CBOR<ChainRedisStorage<T>> {
                    ChainRedisStorage<T> {
                        rev, data, nextID
                    }
                }
            );
            std::string currentChainKey = configuration_.chainPrefix+":"+id;
            redisReply *r = nullptr;
            if (ttlSeconds > 0) {
                if (ctx == nullptr) {
                    std::lock_guard<std::mutex> _(redisMutex_);
                    r = (redisReply *) redisCommand(
                        redisCtx_, "SET %s %b EX %i", currentChainKey.c_str(), redisS.c_str(), redisS.length(), ttlSeconds
                    );
                } else {
                    r = (redisReply *) redisCommand(
                        ctx, "SET %s %b EX %i", currentChainKey.c_str(), redisS.c_str(), redisS.length(), ttlSeconds
                    );
                }
            } else {
                if (ctx == nullptr) {
                    std::lock_guard<std::mutex> _(redisMutex_);
                    r = (redisReply *) redisCommand(
                        redisCtx_, "SET %s %b", currentChainKey.c_str(), redisS.c_str(), redisS.length()
                    );
                } else {
                    r = (redisReply *) redisCommand(
                        ctx, "SET %s %b", currentChainKey.c_str(), redisS.c_str(), redisS.length()
                    );
                }
            }
            if (r != nullptr) {
                freeReplyObject((void *) r);
            }
        }
        //These two are helper functions to store and load data under extraDataPrefix
        //The extra data are not duplicated on Redis
        template <class ExtraData>
        void saveExtraData(std::string const &key, ExtraData const &data) {
            etcdserverpb::PutRequest put;
            put.set_key(configuration_.extraDataPrefix+":"+key);
            put.set_value(
                basic::bytedata_utils::RunSerializer<basic::CBOR<ExtraData>>::apply(
                    basic::CBOR<ExtraData> {data}
                )
            );
            etcdserverpb::PutResponse putResp;
            grpc::ClientContext putCtx;
            putCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
            stub_->Put(&putCtx, put, &putResp);
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            etcdserverpb::RangeRequest range;
            range.set_key(configuration_.extraDataPrefix+":"+key);

            etcdserverpb::RangeResponse rangeResp;
            grpc::ClientContext rangeCtx;
            rangeCtx.set_deadline(std::chrono::system_clock::now()+std::chrono::hours(24));
            stub_->Range(&rangeCtx, range, &rangeResp);

            if (rangeResp.kvs_size() == 0) {
                return std::nullopt;
            }
            auto d = basic::bytedata_utils::RunDeserializer<basic::CBOR<ExtraData>>::apply(
                rangeResp.kvs(0).value()
            );
            if (d) {
                return d->value;
            } else {
                return std::nullopt;
            }
        }
    };
    
    //InMemoryChain has the same basic interface as EtcdChain, it can be used
    //for testing or for single-pass-iteration apps
    template <class T>
    class InMemoryChain {
    public:
        using MapData = ChainStorage<T>;
        using TheMap = std::unordered_map<std::string, MapData>;
        using ItemType = ChainItem<T>;
    private:
        std::function<void()> updateTriggerFunc_;
        TheMap theMap_;
        std::unordered_map<std::string, std::string> extraData_;
        std::mutex mutex_;
    public:
        InMemoryChain() : updateTriggerFunc_(), theMap_({{"", MapData {}}}), extraData_(), mutex_() {
        }
        void setUpdateTriggerFunc(std::function<void()> f) {
            if (updateTriggerFunc_) {
                throw EtcdChainException("Duplicate attempt to set update trigger function for EtcdChain");
            }
            updateTriggerFunc_ = f;
            if (updateTriggerFunc_) {
                updateTriggerFunc_();
            }
        }
        ItemType head(void *) {
            std::lock_guard<std::mutex> _(mutex_);
            auto &x = theMap_[""];
            return ItemType {0, "", x.data, x.nextID};
        }
        ItemType loadUntil(void *, std::string const &id) {
            std::lock_guard<std::mutex> _(mutex_);
            auto &x = theMap_[id];
            return ItemType {0, id, x.data, x.nextID};
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = theMap_.find(current.id);
            if (iter == theMap_.end()) {
                return std::nullopt;
            }
            if (iter->second.nextID == "") {
                return std::nullopt;
            }
            iter = theMap_.find(iter->second.nextID);
            if (iter == theMap_.end()) {
                return std::nullopt;
            }
            return ItemType {0, iter->first, iter->second.data, iter->second.nextID};
        }
        bool appendAfter(ItemType const &current, ItemType &&toBeWritten) {
            std::lock_guard<std::mutex> _(mutex_);
            if (theMap_.find(toBeWritten.id) != theMap_.end()) {
                return false;
            }
            auto iter = theMap_.find(current.id);
            if (iter == theMap_.end()) {
                return false;
            }
            if (iter->second.nextID != "") {
                return false;
            }
            iter->second.nextID = toBeWritten.id;
            theMap_.insert({iter->second.nextID, MapData {std::move(toBeWritten.data), ""}}).first;
            if (updateTriggerFunc_) {
                updateTriggerFunc_();
            }
            return true;
        }
        template <class ExtraData>
        void saveExtraData(std::string const &key, ExtraData const &data) {
            std::lock_guard<std::mutex> _(mutex_);
            extraData_[key] = basic::bytedata_utils::RunSerializer<basic::CBOR<ExtraData>>::apply(
                basic::CBOR<ExtraData> {data}
            );
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = extraData_.find(key);
            if (iter != extraData_.end()) {
                auto d = basic::bytedata_utils::RunDeserializer<basic::CBOR<ExtraData>>::apply(
                    iter->second
                );
                if (d) {
                    return std::get<0>(*d);
                } else {
                    return std::nullopt;
                }
            } else {
                return std::nullopt;
            }
        }
    };

    template <class App, class T>
    inline std::shared_ptr<typename App::template Importer<ChainUpdateNotification<T>>>
    createEtcdChainUpdateNotificationImporter(EtcdChain<T> *chain) {
        auto x = App::template constTriggerImporter<ChainUpdateNotification<T>>();
        chain->setUpdateTriggerFunc(std::get<1>(x));
        return std::get<0>(x);
    }
}}}}}

#endif