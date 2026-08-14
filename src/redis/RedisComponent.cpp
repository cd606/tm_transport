#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <cstring>
#include <sstream>
#include <unordered_map>

#include <tm_kit/transport/redis/RedisComponent.hpp>
#include <tm_kit/transport/TLSConfigurationComponent.hpp>

#ifdef _MSC_VER
#include <winsock2.h>
#endif
#include <hiredis/hiredis.h>
#if __has_include(<hiredis/hiredis_ssl.h>)
#include <hiredis/hiredis_ssl.h>
#define HIREDIS_USE_SSL 1
#else
#define HIREDIS_USE_SSL 0
#endif

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace redis {
    class RedisComponentImpl {
    private:
#if HIREDIS_USE_SSL
        static int initializeSSL() {
            redisInitOpenSSL();
            return 1;
        }
        static const int _ssl_initialized;
#endif
        static void auth(ConnectionLocator const &locator, redisContext *ctx, TLSClientConfigurationComponent *tlsConf) {
#if HIREDIS_USE_SSL
            redisSSLContext *ssl_context = nullptr;
            redisSSLContextError ssl_error = REDIS_SSL_CTX_NONE;

            auto locatorCACert = locator.query("ca_cert", "");
            auto locatorClientCert = locator.query("client_cert", "");
            auto locatorClientKey = locator.query("client_key", "");

            if (locatorCACert != "" && locatorClientCert != "" && locatorClientKey != "") {
                ssl_context = redisCreateSSLContext(
                    locatorCACert.c_str()
                    , nullptr
                    , locatorClientCert.c_str()
                    , locatorClientKey.c_str()
                    , nullptr
                    , &ssl_error
                    );
                if (ssl_context == nullptr || ssl_error != REDIS_SSL_CTX_NONE) {
                    throw std::runtime_error("Redis SSL context creation error");
                }
            } else {
                auto sslInfo = tlsConf?(tlsConf->getConfigurationItem(
                    TLSClientInfoKey {
                        locator.host(), (locator.port()==0?6379:locator.port())
                    }
                )):std::nullopt;

                if (sslInfo) {
                    //std::cerr << "TLS! " << sslInfo->caCertificateFile << ' ' << sslInfo->clientCertificateFile << ' ' << sslInfo->clientKeyFile << '\n';
                    ssl_context = redisCreateSSLContext(
                        sslInfo->caCertificateFile.c_str()
                        , nullptr
                        , sslInfo->clientCertificateFile.c_str()
                        , sslInfo->clientKeyFile.c_str()
                        , nullptr
                        , &ssl_error
                        );
                    if (ssl_context == nullptr || ssl_error != REDIS_SSL_CTX_NONE) {
                        throw std::runtime_error("Redis SSL context creation error");
                    }
                }
            }
            if (ssl_context != nullptr) {
                if (redisInitiateSSLWithContext(ctx, ssl_context) != REDIS_OK) {
                    throw std::runtime_error("Redis SSL negotiation error");
                }
            }
#endif
            if (locator.password() == "") {
                return;
            }
            redisReply *r = nullptr;
            if (locator.userName() != "") {
                r = (redisReply *) redisCommand(ctx, "AUTH %s %s", locator.userName().c_str(), locator.password().c_str());
            } else {
                r = (redisReply *) redisCommand(ctx, "AUTH %s", locator.password().c_str());
            }
            if (!r || r->type == REDIS_REPLY_ERROR) {
                throw std::runtime_error("Failure to authenticate with Redis server for "+locator.toSerializationFormat());
            }
            freeReplyObject((void *) r);
        }
        class OneRedisSubscription {
        private:
            ConnectionLocator locator_;
            std::string topic_;
            TLSClientConfigurationComponent *tlsConf_;
            redisContext *ctx_;
            struct ClientCB {
                uint32_t id;
                std::function<void(basic::ByteDataWithTopic &&)> cb;
                std::optional<WireToUserHook> hook;
            };
            std::vector<ClientCB> clients_;
            std::thread th_;
            std::mutex mutex_;
            std::atomic<bool> running_;
            std::mutex reconnectMutex_;
            std::condition_variable reconnectCondition_;

            inline void callClient(ClientCB const &c, basic::ByteDataWithTopic &&d) {
                if (c.hook) {
                    auto b = (c.hook->hook)(basic::ByteDataView {std::string_view(d.content)});
                    if (b) {
                        c.cb({std::move(d.topic), std::move(b->content)});
                    }
                } else {
                    c.cb(std::move(d));
                }
            }

            redisContext *createSubscribedContext() {
                struct timeval connectTimeout = {2, 0};
                redisContext *ctx = redisConnectWithTimeout(
                    locator_.host().c_str(), locator_.port(), connectTimeout
                );
                if (ctx == nullptr || ctx->err) {
                    if (ctx != nullptr) {
                        redisFree(ctx);
                    }
                    return nullptr;
                }
                try {
                    redisSetTimeout(ctx, connectTimeout);
                    RedisComponentImpl::auth(locator_, ctx, tlsConf_);
                    redisReply *reply = (redisReply *) redisCommand(
                        ctx, "PSUBSCRIBE %s", topic_.c_str()
                    );
                    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
                        if (reply != nullptr) {
                            freeReplyObject((void *) reply);
                        }
                        redisFree(ctx);
                        return nullptr;
                    }
                    freeReplyObject((void *) reply);
                } catch (...) {
                    redisFree(ctx);
                    return nullptr;
                }
                if (ctx->err) {
                    redisFree(ctx);
                    return nullptr;
                }
                return ctx;
            }
            bool waitBeforeReconnect(std::chrono::seconds delay) {
                std::unique_lock<std::mutex> lock(reconnectMutex_);
                return reconnectCondition_.wait_for(lock, delay, [this]() {
                    return !running_;
                });
            }
            static void increaseReconnectDelay(std::chrono::seconds &delay) {
                delay *= 2;
                if (delay > std::chrono::seconds(60)) {
                    delay = std::chrono::seconds(60);
                }
            }
            void run() {
                std::chrono::seconds reconnectDelay(1);
                while (running_) {
                    ctx_ = createSubscribedContext();
                    if (ctx_ == nullptr) {
                        if (waitBeforeReconnect(reconnectDelay)) {
                            break;
                        }
                        increaseReconnectDelay(reconnectDelay);
                        continue;
                    }

                    reconnectDelay = std::chrono::seconds(1);
                    struct timeval receiveTimeout = {0, 100000};
                    redisSetTimeout(ctx_, receiveTimeout);
                    redisReply *reply = nullptr;
                    try {
                        while (running_) {
                            reply = nullptr;
                            int result = redisGetReply(ctx_, (void **) &reply);
                            if (result != REDIS_OK) {
                                if (ctx_->err == REDIS_ERR_IO && errno == EAGAIN) {
                                    ctx_->err = 0;
                                    continue;
                                }
                                break;
                            }
                            if (!running_ || reply == nullptr) {
                                if (reply != nullptr) {
                                    freeReplyObject((void *) reply);
                                    reply = nullptr;
                                }
                                continue;
                            }
                            if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 4) {
                                freeReplyObject((void *) reply);
                                reply = nullptr;
                                continue;
                            }
                            if (reply->element[0]->type != REDIS_REPLY_STRING
                                || std::string(reply->element[0]->str, reply->element[0]->len) != "pmessage") {
                                freeReplyObject((void *) reply);
                                reply = nullptr;
                                continue;
                            }
                            std::string topic(reply->element[2]->str, reply->element[2]->len);
                            std::string content(reply->element[3]->str, reply->element[3]->len);
                            freeReplyObject((void *) reply);
                            reply = nullptr;

                            if (!running_) {
                                break;
                            }
                            std::lock_guard<std::mutex> lock(mutex_);
                            for (auto const &cb : clients_) {
                                callClient(cb, {topic, content});
                            }
                        }
                    } catch (...) {
                    }
                    if (reply != nullptr) {
                        freeReplyObject((void *) reply);
                    }
                    redisFree(ctx_);
                    ctx_ = nullptr;

                    if (running_) {
                        if (waitBeforeReconnect(reconnectDelay)) {
                            break;
                        }
                        increaseReconnectDelay(reconnectDelay);
                    }
                }
            }
            void stop() {
                running_ = false;
                reconnectCondition_.notify_all();
                if (th_.joinable()) {
                    try {
                        th_.join();
                    } catch (std::system_error const &) {
                    }
                }
            }
        public:
            OneRedisSubscription(ConnectionLocator const &locator, std::string const &topic, TLSClientConfigurationComponent *tlsConf) 
                : locator_(locator)
                , topic_(topic)
                , tlsConf_(tlsConf)
                , ctx_(nullptr)
                , clients_()
                , th_()
                , mutex_()
                , running_(true)
                , reconnectMutex_()
                , reconnectCondition_()
            {
                th_ = std::thread(&OneRedisSubscription::run, this);
            }
            ~OneRedisSubscription() {
                stop();
            }
            void addSubscription(
                uint32_t id
                , std::function<void(basic::ByteDataWithTopic &&)> handler
                , std::optional<WireToUserHook> wireToUserHook
            ) {
                std::lock_guard<std::mutex> _(mutex_);
                clients_.push_back({id, handler, wireToUserHook});
            }  
            void removeSubscription(uint32_t id) {
                std::lock_guard<std::mutex> _(mutex_);
                clients_.erase(std::remove_if(
                    clients_.begin()
                    , clients_.end()
                    , [id](auto const &x) {
                        return x.id == id;
                    }
                ), clients_.end());
            }
            bool checkWhetherNeedsToStop() {
                std::lock_guard<std::mutex> _(mutex_);
                if (clients_.empty()) {
                    running_ = false;
                    return true;
                } else {
                    return false;
                }
            }
            void unsubscribe() {
                stop();
            }
            ConnectionLocator const &locator() const {
                return locator_;
            }
            std::string const &topic() const {
                return topic_;
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };
        
        std::unordered_map<ConnectionLocator, std::unordered_map<std::string, std::unique_ptr<OneRedisSubscription>>> subscriptions_;

        class OneRedisSender {
        private:
            ConnectionLocator locator_;
            TLSClientConfigurationComponent *tlsConf_;
            redisContext *ctx_;
            std::mutex mutex_;
            std::condition_variable reconnectCondition_;
            std::thread reconnectThread_;
            bool connected_;
            bool stopping_;
            std::chrono::seconds reconnectDelay_;
            std::chrono::steady_clock::time_point nextReconnectAttempt_;

            redisContext *createConnection() {
                struct timeval connectTimeout = {2, 0};
                redisContext *ctx = redisConnectWithTimeout(
                    locator_.host().c_str(), locator_.port(), connectTimeout
                );
                if (ctx == nullptr || ctx->err) {
                    if (ctx != nullptr) {
                        redisFree(ctx);
                    }
                    return nullptr;
                }
                try {
                    // Also bound AUTH so that the reconnect worker cannot hang
                    // indefinitely on an unresponsive peer.
                    redisSetTimeout(ctx, connectTimeout);
                    RedisComponentImpl::auth(locator_, ctx, tlsConf_);
                } catch (...) {
                    redisFree(ctx);
                    return nullptr;
                }
                if (ctx->err) {
                    redisFree(ctx);
                    return nullptr;
                }
                return ctx;
            }
            void resetReconnectDelayLocked() {
                reconnectDelay_ = std::chrono::seconds(1);
            }
            void scheduleReconnectLocked() {
                nextReconnectAttempt_ = std::chrono::steady_clock::now()+reconnectDelay_;
                if (reconnectDelay_ < std::chrono::seconds(60)) {
                    reconnectDelay_ *= 2;
                    if (reconnectDelay_ > std::chrono::seconds(60)) {
                        reconnectDelay_ = std::chrono::seconds(60);
                    }
                }
            }
            void markDisconnectedLocked() {
                if (ctx_ != nullptr) {
                    redisFree(ctx_);
                    ctx_ = nullptr;
                }
                if (connected_) {
                    connected_ = false;
                    resetReconnectDelayLocked();
                    scheduleReconnectLocked();
                }
                reconnectCondition_.notify_one();
            }
            void reconnectLoop() {
                std::unique_lock<std::mutex> lock(mutex_);
                while (!stopping_) {
                    if (connected_) {
                        reconnectCondition_.wait(lock, [this]() {
                            return stopping_ || !connected_;
                        });
                        continue;
                    }
                    if (reconnectCondition_.wait_until(
                        lock, nextReconnectAttempt_, [this]() {
                            return stopping_ || connected_;
                        }
                    )) {
                        continue;
                    }

                    lock.unlock();
                    redisContext *newContext = createConnection();
                    lock.lock();
                    if (stopping_) {
                        if (newContext != nullptr) {
                            redisFree(newContext);
                        }
                        break;
                    }
                    if (newContext != nullptr) {
                        ctx_ = newContext;
                        connected_ = true;
                        resetReconnectDelayLocked();
                    } else {
                        scheduleReconnectLocked();
                    }
                }
            }
        public:
            OneRedisSender(ConnectionLocator const &locator, TLSClientConfigurationComponent *tlsConf)
                : locator_(locator), tlsConf_(tlsConf), ctx_(nullptr), mutex_()
                , reconnectCondition_(), reconnectThread_(), connected_(false)
                , stopping_(false), reconnectDelay_(1), nextReconnectAttempt_()
            {
                ctx_ = createConnection();
                connected_ = (ctx_ != nullptr);
                if (!connected_) {
                    scheduleReconnectLocked();
                }
                reconnectThread_ = std::thread(&OneRedisSender::reconnectLoop, this);
            }
            ~OneRedisSender() {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    stopping_ = true;
                    reconnectCondition_.notify_one();
                }
                if (reconnectThread_.joinable()) {
                    reconnectThread_.join();
                }
                if (ctx_ != nullptr) {
                    redisFree(ctx_);
                    ctx_ = nullptr;
                }
            }
            void publish(basic::ByteDataWithTopic &&data) {
                std::lock_guard<std::mutex> lock(mutex_);
                if (!connected_ || ctx_ == nullptr || ctx_->err) {
                    if (connected_) {
                        markDisconnectedLocked();
                    }
                    return;
                }
                redisReply *r = (redisReply *) redisCommand(
                    ctx_
                    , "PUBLISH %s %b"
                    , data.topic.c_str()
                    , data.content.c_str()
                    , data.content.length()
                ); 
                if (r != nullptr) {
                    freeReplyObject((void *) r);
                } else {
                    // This message has already failed. Do not retain or retry it.
                    markDisconnectedLocked();
                }
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRedisSender>> senders_;

        class OneRedisRPCClientConnection {
        private:
            redisContext *ctx_;
            std::string rpcTopic_;
            std::string myCommunicationID_;
            struct OneClientInfo {
                std::function<void(bool, basic::ByteDataWithID &&)> callback_;
                std::optional<WireToUserHook> wireToUserHook_;
            };
            uint32_t clientCounter_;
            std::unordered_map<uint32_t, OneClientInfo> clients_;
            std::unordered_map<uint32_t, std::unordered_set<std::string>> clientToIDMap_;
            std::unordered_map<std::string, uint32_t> idToClientMap_;
            std::mutex clientsMutex_;
            std::thread th_;
            std::atomic<bool> running_;
            OneRedisSender *sender_;
            void run() {
                struct redisReply *reply = nullptr;
                try {
                    while (running_) {
                        struct timeval tv = { 0, 1000 };
                        if (redisSetTimeout(ctx_, tv) != REDIS_OK) {
                            break;
                        }
                        if (!ctx_ || ctx_->err) {
                            throw std::runtime_error("Redis context error");
                        }
                        reply = nullptr;
                        int r = redisGetReply(ctx_, (void **) &reply);
                        if (r != REDIS_OK) {
                            if (ctx_->err == REDIS_ERR_EOF) {
                                break;
                            }
                            if (ctx_->err == REDIS_ERR_IO && errno == EAGAIN) {
                                ctx_->err = 0;
                                continue;
                            }
                            if (ctx_->err == 0) {
                                if (reply != nullptr) {
                                    freeReplyObject((void *) &reply);
                                }
                            }
                            continue;
                        }
                        if (!running_) {
                            break;
                        }
                        if (reply == nullptr) {
                            continue;
                        }
                        if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 3) {
                            freeReplyObject((void *) reply);
                            continue;
                        }
                        if (reply->element[0]->type != REDIS_REPLY_STRING
                            ||
                            std::string(reply->element[0]->str, reply->element[0]->len) != "message") {
                            freeReplyObject((void *) reply);
                            continue;
                        }
                        std::string topic(reply->element[1]->str, reply->element[1]->len);
                        if (topic != myCommunicationID_) {
                            freeReplyObject((void *) reply);
                            continue;
                        }

                        auto parseRes = basic::bytedata_utils::RunCBORDeserializer<std::tuple<bool,basic::ByteDataWithID>>::apply(std::string_view {reply->element[2]->str, reply->element[2]->len}, 0);
                        if (!parseRes || std::get<1>(*parseRes) != reply->element[2]->len) {
                            freeReplyObject((void *) reply);
                            continue;
                        }

                        freeReplyObject((void *) reply);    

                        if (!running_) {
                            break;
                        }             

                        {
                            std::lock_guard<std::mutex> _(clientsMutex_);
                            std::string theID = std::get<1>(std::get<0>(*parseRes)).id;
                            auto iter = idToClientMap_.find(theID);
                            if (iter != idToClientMap_.end()) {
                                auto iter1 = clients_.find(iter->second);
                                if (iter1 != clients_.end()) {
                                    if (iter1->second.wireToUserHook_) {
                                        auto d = (iter1->second.wireToUserHook_->hook)(basic::ByteDataView {std::string_view(std::get<1>(std::get<0>(*parseRes)).content)});
                                        if (d) {
                                            iter1->second.callback_(std::get<0>(std::get<0>(*parseRes)), {std::move(std::get<1>(std::get<0>(*parseRes)).id), std::move(d->content)});
                                        }
                                    } else {
                                        iter1->second.callback_(std::get<0>(std::get<0>(*parseRes)), std::move(std::get<1>(std::get<0>(*parseRes))));
                                    }
                                }
                                if (std::get<0>(std::get<0>(*parseRes))) {
                                    clientToIDMap_[iter->second].erase(theID);
                                    idToClientMap_.erase(iter);
                                }
                            }
                        }
                    }
                } catch (...) {}
            }
        public:
            OneRedisRPCClientConnection(ConnectionLocator const &locator, std::string const &myCommunicationID, OneRedisSender *sender, TLSClientConfigurationComponent *tlsConf)
                : ctx_(nullptr)
                , rpcTopic_(locator.identifier())
                , myCommunicationID_(myCommunicationID)
                , clientCounter_(0)
                , clients_()
                , clientToIDMap_()
                , idToClientMap_()
                , clientsMutex_()
                , th_()
                , running_(true)
                , sender_(sender)
            {
                ctx_ = redisConnect(locator.host().c_str(), locator.port());
                if (ctx_ != nullptr) {
                    RedisComponentImpl::auth(locator, ctx_, tlsConf);
                    redisReply *r = (redisReply *) redisCommand(ctx_, "SUBSCRIBE %s", myCommunicationID_.c_str());
                    freeReplyObject((void *) r);
                    th_ = std::thread(&OneRedisRPCClientConnection::run, this);
                    th_.detach();
                }
            }
            ~OneRedisRPCClientConnection() {
                running_ = false;
                try {
                    if (th_.joinable()) {
                        th_.join();
                    }
                } catch (std::system_error const &) {
                }
                //std::cerr << this << ": redis rpc client really exiting\n";
                if (ctx_ && !ctx_->err) {
                    redisFree(ctx_);
                }
            }
            uint32_t addClient(std::function<void(bool, basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook) {
                std::lock_guard<std::mutex> _(clientsMutex_);
                clients_[++clientCounter_] = {callback, wireToUserHook};
                return clientCounter_;
            }
            std::size_t removeClient(uint32_t clientNumber) {
                std::lock_guard<std::mutex> _(clientsMutex_);
                auto iter = clientToIDMap_.find(clientNumber);
                if (iter != clientToIDMap_.end()) {
                    for (auto const &id : iter->second) {
                        idToClientMap_.erase(id);
                    }
                    clientToIDMap_.erase(iter);
                }
                clients_.erase(clientNumber);
                return clients_.size();
            }
            void unsubscribe() {
                //std::cerr << this << ": rpc client unsubscribe\n";
                running_ = false;
                try {
                    if (th_.joinable()) {
                        th_.join();
                    }
                } catch (std::system_error const &) {
                }
                //std::cerr << this << ": unsubscribing on server level\n";
                if (ctx_ && !ctx_->err) {
                    redisReply *r = (redisReply *) redisCommand(ctx_, "UNSUBSCRIBE %s", myCommunicationID_.c_str());
                    if (r) {
                        freeReplyObject((void *) r);
                    }
                }
            }
            void sendRequest(uint32_t clientNumber, basic::ByteDataWithID &&data) {
                {
                    std::lock_guard<std::mutex> _(clientsMutex_);
                    clientToIDMap_[clientNumber].insert(data.id);
                    idToClientMap_[data.id] = clientNumber;
                }
                auto encodedData = basic::bytedata_utils::RunSerializer<basic::CBOR<basic::ByteDataWithID>>::apply({std::move(data)});
                auto encodedDataAndTopic = basic::bytedata_utils::RunSerializer<basic::CBOR<basic::ByteDataWithTopic>>::apply({myCommunicationID_, std::move(encodedData)});
                sender_->publish(basic::ByteDataWithTopic {rpcTopic_, std::move(encodedDataAndTopic)});       
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRedisRPCClientConnection>> rpcClientConnections_;

        class OneRedisRPCServerConnection {
        private:
            redisContext *ctx_;
            std::string rpcTopic_;
            std::function<void(basic::ByteDataWithID &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::unordered_map<std::string, std::string> replyTopicMap_;
            std::thread th_;
            std::mutex mutex_;
            OneRedisSender *sender_;
            std::atomic<bool> running_;
            void run() {
                struct redisReply *reply = nullptr;
                while (running_) {
                    struct timeval tv = { 0, 1000 };
                    if (redisSetTimeout(ctx_, tv) != REDIS_OK) {
                        break;
                    }
                    if (!ctx_ || ctx_->err) {
                        throw std::runtime_error("Redis context error");
                    }
                    reply = nullptr;
                    int r = redisGetReply(ctx_, (void **) &reply);
                    if (r != REDIS_OK) {
                        if (ctx_->err == REDIS_ERR_EOF) {
                            break;
                        }
                        if (ctx_->err == REDIS_ERR_IO && errno == EAGAIN) {
                            ctx_->err = 0;
                            continue;
                        }
                        if (ctx_->err == 0) {
                            if (reply != nullptr) {
                                freeReplyObject((void *) &reply);
                            }
                        }
                        continue;
                    }
                    if (!running_) {
                        break;
                    }
                    if (reply == nullptr) {
                        continue;
                    }
                    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 3) {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    if (reply->element[0]->type != REDIS_REPLY_STRING
                        ||
                        std::string(reply->element[0]->str, reply->element[0]->len) != "message") {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    std::string topic(reply->element[1]->str, reply->element[1]->len);
                    if (topic != rpcTopic_) {
                        freeReplyObject((void *) reply);
                        continue;
                    }

                    auto parseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithTopic>::apply(std::string_view {reply->element[2]->str, reply->element[2]->len}, 0);
                    if (!parseRes || std::get<1>(*parseRes) != reply->element[2]->len) {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    freeReplyObject((void *) reply);
                    auto innerParseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithID>::apply(std::string_view {std::get<0>(*parseRes).content}, 0);
                    if (!innerParseRes || std::get<1>(*innerParseRes) != std::get<0>(*parseRes).content.length()) {
                        continue;
                    }
                    if (!running_) {
                        break;
                    }
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        replyTopicMap_[std::get<0>(*innerParseRes).id] = std::get<0>(*parseRes).topic;
                    }
                    if (wireToUserHook_) {
                        auto d = (wireToUserHook_->hook)(basic::ByteDataView {std::string_view(std::get<0>(*innerParseRes).content)});
                        if (d) {
                            callback_({std::move(std::get<0>(*innerParseRes).id), std::move(d->content)});
                        }
                    } else {
                        callback_(std::move(std::get<0>(*innerParseRes)));
                    }
                }
            }
        public:
            OneRedisRPCServerConnection(ConnectionLocator const &locator, std::function<void(basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, OneRedisSender *sender, TLSClientConfigurationComponent *tlsConf)
                : ctx_(nullptr)
                , rpcTopic_(locator.identifier())
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , th_()
                , mutex_()
                , sender_(sender)
                , running_(true)
            {
                ctx_ = redisConnect(locator.host().c_str(), locator.port());
                if (ctx_ != nullptr) {
                    RedisComponentImpl::auth(locator, ctx_, tlsConf);
                    redisReply *r = (redisReply *) redisCommand(ctx_, "SUBSCRIBE %s", rpcTopic_.c_str());
                    freeReplyObject((void *) r);
                    th_ = std::thread(&OneRedisRPCServerConnection::run, this);
                    th_.detach();
                }
            }
            ~OneRedisRPCServerConnection() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
                if (ctx_ && !ctx_->err) {
                    redisReply *r = (redisReply *) redisCommand(ctx_, "UNSUBSCRIBE %s", rpcTopic_.c_str());
                    if (r) {
                        freeReplyObject((void *) r);
                    }
                    redisFree(ctx_);
                }
            }
            void sendReply(bool isFinal, basic::ByteDataWithID &&data) {
                std::string replyTopic;
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    auto iter = replyTopicMap_.find(data.id);
                    if (iter == replyTopicMap_.end()) {
                        return;
                    }
                    replyTopic = iter->second;
                    if (isFinal) {
                        replyTopicMap_.erase(iter);
                    }
                }
                auto encodedData = basic::bytedata_utils::RunSerializer<basic::CBOR<std::tuple<bool,basic::ByteDataWithID>>>::apply({{isFinal, std::move(data)}});
                sender_->publish(basic::ByteDataWithTopic {replyTopic, std::move(encodedData)});           
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRedisRPCServerConnection>> rpcServerConnections_;

        std::mutex mutex_;

        uint32_t counter_;
        std::unordered_map<uint32_t, OneRedisSubscription *> idToSubscriptionMap_;
        std::mutex idMutex_;

        OneRedisSubscription *getOrStartSubscription(ConnectionLocator const &d, std::string const &topic, TLSClientConfigurationComponent *tlsConf) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            std::lock_guard<std::mutex> _(mutex_);
            auto subscriptionIter = subscriptions_.find(hostAndPort);
            if (subscriptionIter == subscriptions_.end()) {
                subscriptionIter = subscriptions_.insert({hostAndPort, std::unordered_map<std::string, std::unique_ptr<OneRedisSubscription>> {}}).first;
            }
            auto innerIter = subscriptionIter->second.find(topic);
            if (innerIter == subscriptionIter->second.end()) {
                innerIter = subscriptionIter->second.insert({topic, std::make_unique<OneRedisSubscription>(d, topic, tlsConf)}).first;
            }
            return innerIter->second.get();
        }
        void potentiallyStopSubscription(OneRedisSubscription *p) {
            //std::cerr << "potentially stopping " << p << '\n';
            std::lock_guard<std::mutex> _(mutex_);
            if (p->checkWhetherNeedsToStop()) {
                //std::cerr << p << ": is being stopped\n";
                p->unsubscribe();
                //std::cerr << p << " is being removed from subscriptionn map '" << p->locator().toPrintFormat() << "'\n";
                ConnectionLocator hostAndPort {p->locator().host(), p->locator().port()};
                auto iter = subscriptions_.find(hostAndPort);
                if (iter != subscriptions_.end()) {
                    auto innerIter = iter->second.find(p->topic());
                    if (innerIter != iter->second.end()) {
                        innerIter->second.release(); //deliberate leak
                        iter->second.erase(innerIter);
                        std::thread([p]() {
                            std::this_thread::sleep_for(std::chrono::seconds(5));
                            delete p;
                        }).detach();
                    }
                    if (iter->second.empty()) {
                        subscriptions_.erase(iter);
                    }
                }
                //std::cerr << subscriptions_.size() << '\n';
            }
        }
        OneRedisSender *getOrStartSender(ConnectionLocator const &d, TLSClientConfigurationComponent *tlsConf) {
            std::lock_guard<std::mutex> _(mutex_);
            return getOrStartSenderNoLock(d, tlsConf);
        }
        OneRedisSender *getOrStartSenderNoLock(ConnectionLocator const &d, TLSClientConfigurationComponent *tlsConf) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            auto senderIter = senders_.find(hostAndPort);
            if (senderIter == senders_.end()) {
                senderIter = senders_.insert({hostAndPort, std::make_unique<OneRedisSender>(d, tlsConf)}).first;
            }
            return senderIter->second.get();
        }
        OneRedisRPCClientConnection *createRpcClientConnection(ConnectionLocator const &l, std::function<std::string()> clientCommunicationIDCreator, TLSClientConfigurationComponent *tlsConf) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcClientConnections_.find(l);
            if (iter == rpcClientConnections_.end()) {
                iter = rpcClientConnections_.insert(
                    {l, std::make_unique<OneRedisRPCClientConnection>(l, clientCommunicationIDCreator(), getOrStartSenderNoLock(l, tlsConf), tlsConf)}
                ).first;
            }
            return iter->second.get();
        }
        OneRedisRPCServerConnection *createRpcServerConnection(ConnectionLocator const &l, std::function<void(basic::ByteDataWithID &&)> handler, std::optional<WireToUserHook> wireToUserHook, TLSClientConfigurationComponent *tlsConf) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcServerConnections_.find(l);
            if (iter != rpcServerConnections_.end()) {
                throw RedisComponentException("Cannot create duplicate RPC server connection for "+l.toSerializationFormat());
            }
            iter = rpcServerConnections_.insert(
                {l, std::make_unique<OneRedisRPCServerConnection>(l, handler, wireToUserHook, getOrStartSenderNoLock(l, tlsConf), tlsConf)}
            ).first;
            return iter->second.get();
        }
    public:
        RedisComponentImpl() 
            : subscriptions_(), senders_(), rpcClientConnections_(), rpcServerConnections_(), mutex_()
            , counter_(0), idToSubscriptionMap_(), idMutex_()
        { 
        }
        ~RedisComponentImpl() {
            std::lock_guard<std::mutex> _(mutex_);
            subscriptions_.clear();
            senders_.clear();
            rpcClientConnections_.clear();
            rpcServerConnections_.clear();
        }
        uint32_t addSubscriptionClient(ConnectionLocator const &locator,
            std::string const &topic,
            std::function<void(basic::ByteDataWithTopic &&)> client,
            std::optional<WireToUserHook> wireToUserHook, 
            TLSClientConfigurationComponent *tlsConf) {
            auto *p = getOrStartSubscription(locator, topic, tlsConf);
            {
                std::lock_guard<std::mutex> _(idMutex_);
                ++counter_;
                p->addSubscription(counter_, client, wireToUserHook);
                idToSubscriptionMap_[counter_] = p;
                return counter_;
            }
        }
        void removeSubscriptionClient(uint32_t id) {
            OneRedisSubscription *p = nullptr;
            {
                std::lock_guard<std::mutex> _(idMutex_);
                auto iter = idToSubscriptionMap_.find(id);
                if (iter == idToSubscriptionMap_.end()) {
                    return;
                }
                p = iter->second;
                idToSubscriptionMap_.erase(iter);
            }
            if (p != nullptr) {
                p->removeSubscription(id);
                potentiallyStopSubscription(p);
            }
        }
        std::function<void(basic::ByteDataWithTopic &&)> getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook, TLSClientConfigurationComponent *tlsConf) {
            auto *p = getOrStartSender(locator, tlsConf);
            if (userToWireHook) {
                auto hook = userToWireHook->hook;
                return [p,hook](basic::ByteDataWithTopic &&data) {
                    auto w = hook(basic::ByteData {std::move(data.content)});
                    p->publish({std::move(data.topic), std::move(w.content)});
                };
            } else {
                return [p](basic::ByteDataWithTopic &&data) {
                    p->publish(std::move(data));
                };
            }
        }
        std::function<void(basic::ByteDataWithID &&)> setRPCClient(ConnectionLocator const &locator,
            std::function<std::string()> clientCommunicationIDCreator,
            std::function<void(bool, basic::ByteDataWithID &&)> client,
            std::optional<ByteDataHookPair> hookPair,
            uint32_t *clientNumberOutput, 
            TLSClientConfigurationComponent *tlsConf) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcClientConnection(locator, clientCommunicationIDCreator, tlsConf);
            auto clientNum = conn->addClient(client, wireToUserHook);
            if (clientNumberOutput) {
                *clientNumberOutput = clientNum;
            }
            if (hookPair && hookPair->userToWire) {
                auto hook = hookPair->userToWire->hook;
                return [conn,hook,clientNum](basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->sendRequest(clientNum, {data.id, std::move(x.content)});
                };
            } else {
                return [conn,clientNum](basic::ByteDataWithID &&data) {
                    conn->sendRequest(clientNum, std::move(data));
                };
            }
        }
        void removeRPCClient(ConnectionLocator const &locator, uint32_t clientNumber) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcClientConnections_.find(locator);
            if (iter != rpcClientConnections_.end()) {
                if (iter->second->removeClient(clientNumber) == 0) {
                    iter->second->unsubscribe();
                    auto *p = iter->second.release(); //intentional
                    rpcClientConnections_.erase(iter);
                    std::thread([p]() {
                        std::this_thread::sleep_for(std::chrono::seconds(5));
                        delete p;
                    }).detach();
                }
            }
        }
        std::function<void(bool, basic::ByteDataWithID &&)> setRPCServer(ConnectionLocator const &locator,
            std::function<void(basic::ByteDataWithID &&)> server,
            std::optional<ByteDataHookPair> hookPair,
            TLSClientConfigurationComponent *tlsConf) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcServerConnection(locator, server, wireToUserHook, tlsConf);
            if (hookPair && hookPair->userToWire) {
                auto hook = hookPair->userToWire->hook;
                return [conn,hook](bool isFinal, basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->sendReply(isFinal, {data.id, std::move(x.content)});
                };
            } else {
                return [conn](bool isFinal, basic::ByteDataWithID &&data) {
                    conn->sendReply(isFinal, std::move(data));
                };
            }
        }
        std::unordered_map<ConnectionLocator, std::thread::native_handle_type> threadHandles() {
            std::unordered_map<ConnectionLocator, std::thread::native_handle_type> retVal;
            std::lock_guard<std::mutex> _(mutex_);
            for (auto &item : subscriptions_) {
                for (auto &innerItem : item.second) {
                    ConnectionLocator l {item.first.host(), item.first.port(), "", "", innerItem.first};
                    retVal[l] = innerItem.second->getThreadHandle();
                }
            }
            for (auto &item : rpcClientConnections_) {
                retVal[item.first] = item.second->getThreadHandle();
            }
            for (auto &item : rpcServerConnections_) {
                retVal[item.first] = item.second->getThreadHandle();
            }
            return retVal;
        }
    };
#if HIREDIS_USE_SSL
    const int RedisComponentImpl::_ssl_initialized = RedisComponentImpl::initializeSSL();
#endif

    RedisComponent::RedisComponent() : impl_(std::make_unique<RedisComponentImpl>()) {}
    RedisComponent::~RedisComponent() {}
    RedisComponent::RedisComponent(RedisComponent &&) = default;
    RedisComponent &RedisComponent::operator=(RedisComponent &&) = default;
    uint32_t RedisComponent::redis_addSubscriptionClient(ConnectionLocator const &locator,
        std::string const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addSubscriptionClient(locator, topic, client, wireToUserHook, dynamic_cast<TLSClientConfigurationComponent *>(this));
    }
    void RedisComponent::redis_removeSubscriptionClient(uint32_t id) {
        impl_->removeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&)> RedisComponent::redis_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook, dynamic_cast<TLSClientConfigurationComponent *>(this));
    }
    std::function<void(basic::ByteDataWithID &&)> RedisComponent::redis_setRPCClient(ConnectionLocator const &locator,
                        std::function<std::string()> clientCommunicationIDCreator,
                        std::function<void(bool, basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair,
                        uint32_t *clientNumberOutput) {
        return impl_->setRPCClient(locator, clientCommunicationIDCreator, client, hookPair, clientNumberOutput, dynamic_cast<TLSClientConfigurationComponent *>(this));
    }
    void RedisComponent::redis_removeRPCClient(ConnectionLocator const &locator, uint32_t clientNumber) {
        impl_->removeRPCClient(locator, clientNumber);
    }
    std::function<void(bool, basic::ByteDataWithID &&)> RedisComponent::redis_setRPCServer(ConnectionLocator const &locator,
                    std::function<void(basic::ByteDataWithID &&)> server,
                    std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCServer(locator, server, hookPair, dynamic_cast<TLSClientConfigurationComponent *>(this));
    }
    std::unordered_map<ConnectionLocator, std::thread::native_handle_type> RedisComponent::redis_threadHandles() {
        return impl_->threadHandles();
    }

} } } } }
