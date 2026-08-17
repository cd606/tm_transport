#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <unordered_map>

#if defined(__has_include)
#if __has_include(<concurrentqueue/moodycamel/blockingconcurrentqueue.h>)
#include <concurrentqueue/moodycamel/blockingconcurrentqueue.h>
#define TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE 1
#elif __has_include(<concurrentqueue/blockingconcurrentqueue.h>)
#include <concurrentqueue/blockingconcurrentqueue.h>
#define TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE 1
#else
#define TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE 0
#endif
#else
#define TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE 0
#endif

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
        static void logRedisMessage(
            ConnectionLocator const &locator,
            std::string const &component,
            std::string const &message
        ) {
            std::ostringstream oss;
            oss << '[' << component << "] " << locator.host() << ':' << locator.port()
                << ": " << message << '\n';
            std::cerr << oss.str();
        }
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
            if (r == nullptr) {
                throw std::runtime_error(
                    "Failure to authenticate with Redis server "
                    +locator.host()+":"+std::to_string(locator.port())
                );
            }
            if (r->type == REDIS_REPLY_ERROR) {
                freeReplyObject((void *) r);
                throw std::runtime_error(
                    "Failure to authenticate with Redis server "
                    +locator.host()+":"+std::to_string(locator.port())
                );
            }
            freeReplyObject((void *) r);
        }
        static redisContext *createRPCSubscriptionContext(
            ConnectionLocator const &locator,
            TLSClientConfigurationComponent *tlsConf,
            std::string const &topic,
            std::string const &component
        ) {
            struct timeval connectTimeout = {2, 0};
            redisContext *ctx = redisConnectWithTimeout(
                locator.host().c_str(), locator.port(), connectTimeout
            );
            if (ctx == nullptr || ctx->err) {
                std::string detail = "connection failed";
                if (ctx != nullptr && ctx->errstr[0] != '\0') {
                    detail += ": ";
                    detail += ctx->errstr;
                }
                logRedisMessage(locator, component, detail);
                if (ctx != nullptr) {
                    redisFree(ctx);
                }
                return nullptr;
            }

            try {
                if (redisSetTimeout(ctx, connectTimeout) != REDIS_OK) {
                    logRedisMessage(locator, component, "failed to set connection timeout");
                    redisFree(ctx);
                    return nullptr;
                }
                auth(locator, ctx, tlsConf);
            } catch (std::exception const &e) {
                logRedisMessage(locator, component, std::string("connection setup failed: ")+e.what());
                redisFree(ctx);
                return nullptr;
            } catch (...) {
                logRedisMessage(locator, component, "connection setup failed with an unknown exception");
                redisFree(ctx);
                return nullptr;
            }

            redisReply *reply = (redisReply *) redisCommand(
                ctx, "SUBSCRIBE %s", topic.c_str()
            );
            bool validAcknowledgement = (
                reply != nullptr
                && reply->type == REDIS_REPLY_ARRAY
                && reply->elements >= 3
                && reply->element[0] != nullptr
                && reply->element[0]->type == REDIS_REPLY_STRING
                && reply->element[0]->str != nullptr
                && std::string_view(reply->element[0]->str, reply->element[0]->len) == "subscribe"
                && reply->element[1] != nullptr
                && reply->element[1]->type == REDIS_REPLY_STRING
                && reply->element[1]->str != nullptr
                && std::string_view(reply->element[1]->str, reply->element[1]->len) == topic
            );
            if (!validAcknowledgement) {
                std::string detail = "SUBSCRIBE failed for topic '"+topic+"'";
                if (reply != nullptr && reply->type == REDIS_REPLY_ERROR
                    && reply->str != nullptr && reply->len > 0) {
                    detail += ": ";
                    detail.append(reply->str, reply->len);
                }
                logRedisMessage(locator, component, detail);
                if (reply != nullptr) {
                    freeReplyObject((void *) reply);
                }
                redisFree(ctx);
                return nullptr;
            }
            freeReplyObject((void *) reply);

            struct timeval receiveTimeout = {0, 100000};
            if (redisSetTimeout(ctx, receiveTimeout) != REDIS_OK) {
                logRedisMessage(locator, component, "failed to set receive timeout");
                redisFree(ctx);
                return nullptr;
            }
            return ctx;
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
                    std::string detail = "connection failed";
                    if (ctx != nullptr && ctx->errstr[0] != '\0') {
                        detail += ": ";
                        detail += ctx->errstr;
                    }
                    RedisComponentImpl::logRedisMessage(locator_, "RedisSubscription", detail);
                    if (ctx != nullptr) {
                        redisFree(ctx);
                    }
                    return nullptr;
                }
                try {
                    if (redisSetTimeout(ctx, connectTimeout) != REDIS_OK) {
                        RedisComponentImpl::logRedisMessage(
                            locator_, "RedisSubscription", "failed to set connection timeout"
                        );
                        redisFree(ctx);
                        return nullptr;
                    }
                    RedisComponentImpl::auth(locator_, ctx, tlsConf_);
                    redisReply *reply = (redisReply *) redisCommand(
                        ctx, "PSUBSCRIBE %s", topic_.c_str()
                    );
                    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
                        std::string detail = "PSUBSCRIBE failed for topic '"+topic_+"'";
                        if (reply != nullptr && reply->str != nullptr && reply->len > 0) {
                            detail += ": ";
                            detail.append(reply->str, reply->len);
                        }
                        RedisComponentImpl::logRedisMessage(locator_, "RedisSubscription", detail);
                        if (reply != nullptr) {
                            freeReplyObject((void *) reply);
                        }
                        redisFree(ctx);
                        return nullptr;
                    }
                    freeReplyObject((void *) reply);
                } catch (std::exception const &e) {
                    RedisComponentImpl::logRedisMessage(
                        locator_, "RedisSubscription", std::string("connection setup failed: ")+e.what()
                    );
                    redisFree(ctx);
                    return nullptr;
                } catch (...) {
                    RedisComponentImpl::logRedisMessage(
                        locator_, "RedisSubscription", "connection setup failed with an unknown exception"
                    );
                    redisFree(ctx);
                    return nullptr;
                }
                if (ctx->err) {
                    std::string detail = "connection setup failed";
                    if (ctx->errstr[0] != '\0') {
                        detail += ": ";
                        detail += ctx->errstr;
                    }
                    RedisComponentImpl::logRedisMessage(locator_, "RedisSubscription", detail);
                    redisFree(ctx);
                    return nullptr;
                }
                return ctx;
            }
            bool waitBeforeReconnect(std::chrono::seconds delay) {
                RedisComponentImpl::logRedisMessage(
                    locator_, "RedisSubscription",
                    "retrying connection in "+std::to_string(delay.count())+" seconds"
                );
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
                bool recovering = false;
                while (running_) {
                    ctx_ = createSubscribedContext();
                    if (ctx_ == nullptr) {
                        recovering = true;
                        if (waitBeforeReconnect(reconnectDelay)) {
                            break;
                        }
                        increaseReconnectDelay(reconnectDelay);
                        continue;
                    }

                    if (recovering) {
                        RedisComponentImpl::logRedisMessage(
                            locator_, "RedisSubscription", "reconnected and re-subscribed to topic '"+topic_+"'"
                        );
                        recovering = false;
                    }
                    reconnectDelay = std::chrono::seconds(1);
                    struct timeval receiveTimeout = {0, 100000};
                    if (redisSetTimeout(ctx_, receiveTimeout) != REDIS_OK) {
                        RedisComponentImpl::logRedisMessage(
                            locator_, "RedisSubscription", "failed to set receive timeout"
                        );
                        redisFree(ctx_);
                        ctx_ = nullptr;
                        recovering = true;
                        if (waitBeforeReconnect(reconnectDelay)) {
                            break;
                        }
                        increaseReconnectDelay(reconnectDelay);
                        continue;
                    }
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
                                std::string detail = "receive failed";
                                if (ctx_->errstr[0] != '\0') {
                                    detail += ": ";
                                    detail += ctx_->errstr;
                                }
                                RedisComponentImpl::logRedisMessage(
                                    locator_, "RedisSubscription", detail
                                );
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
                                || std::string_view(reply->element[0]->str, reply->element[0]->len) != "pmessage") {
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
                    } catch (std::exception const &e) {
                        RedisComponentImpl::logRedisMessage(
                            locator_, "RedisSubscription", std::string("receive loop exception: ")+e.what()
                        );
                    } catch (...) {
                        RedisComponentImpl::logRedisMessage(
                            locator_, "RedisSubscription", "receive loop failed with an unknown exception"
                        );
                    }
                    if (reply != nullptr) {
                        freeReplyObject((void *) reply);
                    }
                    redisFree(ctx_);
                    ctx_ = nullptr;
                    recovering = true;

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
            static constexpr std::size_t DEFAULT_ASYNC_QUEUE_CAPACITY = 16384;
            static constexpr std::size_t DEFAULT_ASYNC_BATCH_SIZE = 128;
            static constexpr std::size_t MAX_ASYNC_QUEUE_CAPACITY = 1048576;
            static constexpr std::size_t MAX_ASYNC_BATCH_SIZE = 65536;

            ConnectionLocator locator_;
            TLSClientConfigurationComponent *tlsConf_;
            redisContext *ctx_;
            std::mutex mutex_;
            std::condition_variable reconnectCondition_;
            std::thread reconnectThread_;
            bool async_;
            std::size_t queueCapacity_;
            std::size_t batchSize_;
#if TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE
            std::unique_ptr<moodycamel::BlockingConcurrentQueue<basic::ByteDataWithTopic>> queue_;
            std::size_t queuedMessageCount_;
#else
            std::deque<basic::ByteDataWithTopic> queue_;
#endif
            bool connected_;
            bool connectionCheckRequested_;
            bool stopping_;
            std::chrono::seconds reconnectDelay_;
            std::chrono::steady_clock::time_point nextReconnectAttempt_;

            static void reportPublishError(
                ConnectionLocator const &locator,
                redisReply const *reply
            ) {
                std::ostringstream oss;
                oss << "PUBLISH command failed";
                if (reply != nullptr && reply->str != nullptr && reply->len > 0) {
                    oss << ": " << std::string_view(reply->str, reply->len);
                }
                RedisComponentImpl::logRedisMessage(locator, "RedisSender", oss.str());
            }

            static std::size_t readPositiveSizeProperty(
                ConnectionLocator const &locator,
                std::string const &name,
                std::size_t defaultValue,
                std::size_t maximumValue
            ) {
                std::string value = locator.query(name, std::to_string(defaultValue));
                try {
                    std::size_t parsedCharacters = 0;
                    unsigned long long parsed = std::stoull(value, &parsedCharacters);
                    if (parsedCharacters != value.size() || parsed == 0 || parsed > maximumValue) {
                        throw std::invalid_argument("out of range");
                    }
                    return static_cast<std::size_t>(parsed);
                } catch (...) {
                    throw RedisComponentException(
                        "Invalid Redis locator property '"+name+"': expected an integer from 1 to "
                        +std::to_string(maximumValue)
                    );
                }
            }

            bool asyncQueueEmptyLocked() const {
#if TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE
                return queuedMessageCount_ == 0;
#else
                return queue_.empty();
#endif
            }
            bool enqueueAsyncMessageLocked(basic::ByteDataWithTopic &&data) {
#if TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE
                if (queuedMessageCount_ >= queueCapacity_) {
                    return false;
                }
                // enqueue() may allocate internal queue bookkeeping, but the
                // explicit count above remains the authoritative message bound.
                if (!queue_->enqueue(std::move(data))) {
                    return false;
                }
                ++queuedMessageCount_;
#else
                if (queue_.size() >= queueCapacity_) {
                    return false;
                }
                queue_.push_back(std::move(data));
#endif
                return true;
            }
            void takeAsyncBatchLocked(std::vector<basic::ByteDataWithTopic> &batch) {
#if TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE
                std::size_t count = queue_->try_dequeue_bulk(
                    std::back_inserter(batch), batchSize_
                );
                queuedMessageCount_ -= count;
#else
                while (!queue_.empty() && batch.size() < batchSize_) {
                    batch.push_back(std::move(queue_.front()));
                    queue_.pop_front();
                }
#endif
            }
            void clearAsyncQueueLocked() {
#if TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE
                basic::ByteDataWithTopic item;
                while (queue_->try_dequeue(item)) {
                }
                queuedMessageCount_ = 0;
#else
                queue_.clear();
#endif
            }

            redisContext *createConnection() {
                struct timeval connectTimeout = {2, 0};
                redisContext *ctx = redisConnectWithTimeout(
                    locator_.host().c_str(), locator_.port(), connectTimeout
                );
                if (ctx == nullptr || ctx->err) {
                    std::string detail = "connection failed";
                    if (ctx != nullptr && ctx->errstr[0] != '\0') {
                        detail += ": ";
                        detail += ctx->errstr;
                    }
                    RedisComponentImpl::logRedisMessage(locator_, "RedisSender", detail);
                    if (ctx != nullptr) {
                        redisFree(ctx);
                    }
                    return nullptr;
                }
                try {
                    // Also bound AUTH so that the reconnect worker cannot hang
                    // indefinitely on an unresponsive peer.
                    if (redisSetTimeout(ctx, connectTimeout) != REDIS_OK) {
                        RedisComponentImpl::logRedisMessage(
                            locator_, "RedisSender", "failed to set connection timeout"
                        );
                        redisFree(ctx);
                        return nullptr;
                    }
                    RedisComponentImpl::auth(locator_, ctx, tlsConf_);
                } catch (std::exception const &e) {
                    RedisComponentImpl::logRedisMessage(
                        locator_, "RedisSender", std::string("connection setup failed: ")+e.what()
                    );
                    if (ctx != nullptr) {
                        redisFree(ctx);
                    }
                    return nullptr;
                } catch (...) {
                    RedisComponentImpl::logRedisMessage(
                        locator_, "RedisSender", "connection setup failed with an unknown exception"
                    );
                    if (ctx != nullptr) {
                        redisFree(ctx);
                    }
                    return nullptr;
                }
                if (ctx->err) {
                    std::string detail = "connection setup failed";
                    if (ctx->errstr[0] != '\0') {
                        detail += ": ";
                        detail += ctx->errstr;
                    }
                    RedisComponentImpl::logRedisMessage(locator_, "RedisSender", detail);
                    redisFree(ctx);
                    return nullptr;
                }
                return ctx;
            }
            void resetReconnectDelayLocked() {
                reconnectDelay_ = std::chrono::seconds(1);
            }
            void scheduleReconnectLocked() {
                RedisComponentImpl::logRedisMessage(
                    locator_, async_ ? "RedisAsyncSender" : "RedisSender",
                    "retrying connection in "+std::to_string(reconnectDelay_.count())+" seconds"
                );
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
                    RedisComponentImpl::logRedisMessage(
                        locator_, "RedisSender", "publish transport failed; reconnecting"
                    );
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
                        RedisComponentImpl::logRedisMessage(
                            locator_, "RedisSender", "reconnected"
                        );
                    } else {
                        scheduleReconnectLocked();
                    }
                }
            }
            bool publishBatch(
                redisContext *ctx,
                std::vector<basic::ByteDataWithTopic> const &batch
            ) {
                for (auto const &data : batch) {
                    if (redisAppendCommand(
                        ctx, "PUBLISH %s %b", data.topic.c_str(),
                        data.content.data(), data.content.length()
                    ) != REDIS_OK) {
                        return false;
                    }
                }

                bool connectionHealthy = true;
                for (std::size_t ii=0; ii<batch.size(); ++ii) {
                    redisReply *reply = nullptr;
                    int result = redisGetReply(ctx, (void **) &reply);
                    if (result != REDIS_OK || reply == nullptr) {
                        if (reply != nullptr) {
                            freeReplyObject((void *) reply);
                        }
                        connectionHealthy = false;
                        break;
                    }
                    // A command-level error drops that message but does not make
                    // the connection unusable. Continue consuming every reply so
                    // the pipeline remains synchronized.
                    if (reply->type == REDIS_REPLY_ERROR) {
                        reportPublishError(locator_, reply);
                    }
                    freeReplyObject((void *) reply);
                }
                return connectionHealthy && !ctx->err;
            }
            static bool checkConnection(redisContext *ctx) {
                redisReply *reply = (redisReply *) redisCommand(ctx, "PING");
                bool healthy = (
                    reply != nullptr
                    && reply->type == REDIS_REPLY_STATUS
                    && reply->str != nullptr
                    && std::string_view(reply->str, reply->len) == "PONG"
                    && !ctx->err
                );
                if (reply != nullptr) {
                    freeReplyObject((void *) reply);
                }
                return healthy;
            }
            void asyncSenderLoop() {
                std::vector<basic::ByteDataWithTopic> batch;
                batch.reserve(batchSize_);
                std::unique_lock<std::mutex> lock(mutex_);

                while (!stopping_) {
                    if (!connected_) {
                        connectionCheckRequested_ = false;
                        clearAsyncQueueLocked();
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
                            RedisComponentImpl::logRedisMessage(
                                locator_, "RedisAsyncSender", "reconnected"
                            );
                        } else {
                            scheduleReconnectLocked();
                        }
                        continue;
                    }

                    if (connectionCheckRequested_) {
                        connectionCheckRequested_ = false;
                        redisContext *activeContext = ctx_;
                        lock.unlock();
                        bool connectionHealthy = checkConnection(activeContext);
                        lock.lock();
                        if (stopping_) {
                            break;
                        }
                        if (!connectionHealthy) {
                            RedisComponentImpl::logRedisMessage(
                                locator_, "RedisAsyncSender",
                                "connection check after RPC receiver failure failed; reconnecting"
                            );
                            if (ctx_ != nullptr) {
                                redisFree(ctx_);
                                ctx_ = nullptr;
                            }
                            clearAsyncQueueLocked();
                            connected_ = false;
                            resetReconnectDelayLocked();
                            scheduleReconnectLocked();
                        }
                        continue;
                    }

                    reconnectCondition_.wait(lock, [this]() {
                        return stopping_ || !connected_ || connectionCheckRequested_
                            || !asyncQueueEmptyLocked();
                    });
                    if (stopping_) {
                        break;
                    }
                    if (!connected_) {
                        continue;
                    }

                    batch.clear();
                    takeAsyncBatchLocked(batch);
                    redisContext *activeContext = ctx_;
                    lock.unlock();
                    bool connectionHealthy = publishBatch(activeContext, batch);
                    lock.lock();

                    if (!connectionHealthy) {
                        RedisComponentImpl::logRedisMessage(
                            locator_, "RedisAsyncSender",
                            "batch transport failed; dropped current batch and queued messages"
                        );
                        // The attempted batch and every item still queued are
                        // deliberately dropped. Nothing is replayed after recovery.
                        if (ctx_ != nullptr) {
                            redisFree(ctx_);
                            ctx_ = nullptr;
                        }
                        clearAsyncQueueLocked();
                        connected_ = false;
                        connectionCheckRequested_ = false;
                        resetReconnectDelayLocked();
                        scheduleReconnectLocked();
                    }
                }
                clearAsyncQueueLocked();
            }
        public:
            OneRedisSender(ConnectionLocator const &locator, TLSClientConfigurationComponent *tlsConf)
                : locator_(locator), tlsConf_(tlsConf), ctx_(nullptr), mutex_()
                , reconnectCondition_(), reconnectThread_(), async_(false)
                , queueCapacity_(0), batchSize_(0), queue_()
#if TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE
                , queuedMessageCount_(0)
#endif
                , connected_(false), connectionCheckRequested_(false)
                , stopping_(false), reconnectDelay_(1), nextReconnectAttempt_()
            {
                std::string senderMode = locator.query("sender_mode", "sync");
                if (senderMode == "async") {
                    async_ = true;
                    queueCapacity_ = readPositiveSizeProperty(
                        locator, "queue_capacity", DEFAULT_ASYNC_QUEUE_CAPACITY,
                        MAX_ASYNC_QUEUE_CAPACITY
                    );
                    batchSize_ = readPositiveSizeProperty(
                        locator, "batch_size", DEFAULT_ASYNC_BATCH_SIZE,
                        MAX_ASYNC_BATCH_SIZE
                    );
                    if (batchSize_ > queueCapacity_) {
                        throw RedisComponentException(
                            "Invalid Redis locator properties: 'batch_size' cannot exceed 'queue_capacity'"
                        );
                    }
#if TM_KIT_TRANSPORT_REDIS_HAS_BLOCKING_CONCURRENT_QUEUE
                    queue_ = std::make_unique<moodycamel::BlockingConcurrentQueue<basic::ByteDataWithTopic>>(
                        queueCapacity_
                    );
#endif
                } else if (senderMode != "sync") {
                    throw RedisComponentException(
                        "Invalid Redis locator property 'sender_mode': expected 'sync' or 'async'"
                    );
                }

                ctx_ = createConnection();
                connected_ = (ctx_ != nullptr);
                if (!connected_) {
                    scheduleReconnectLocked();
                }
                reconnectThread_ = std::thread(
                    async_ ? &OneRedisSender::asyncSenderLoop : &OneRedisSender::reconnectLoop,
                    this
                );
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
            void checkConnectionAfterReceiverFailure() {
                std::lock_guard<std::mutex> lock(mutex_);
                if (!connected_) {
                    return;
                }
                if (async_) {
                    connectionCheckRequested_ = true;
                    reconnectCondition_.notify_one();
                    return;
                }
                if (!checkConnection(ctx_)) {
                    if (ctx_ != nullptr) {
                        redisFree(ctx_);
                        ctx_ = nullptr;
                    }
                    RedisComponentImpl::logRedisMessage(
                        locator_, "RedisSender",
                        "connection check after RPC receiver failure failed; reconnecting"
                    );
                    connected_ = false;
                    resetReconnectDelayLocked();
                    scheduleReconnectLocked();
                    reconnectCondition_.notify_one();
                }
            }
            bool isConnected() {
                std::lock_guard<std::mutex> lock(mutex_);
                return connected_ && !connectionCheckRequested_;
            }
            void publish(basic::ByteDataWithTopic &&data) {
                std::lock_guard<std::mutex> lock(mutex_);
                if (async_) {
                    if (stopping_ || !connected_ || !enqueueAsyncMessageLocked(std::move(data))) {
                        return;
                    }
                    reconnectCondition_.notify_one();
                    return;
                }
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
                    // A REDIS_REPLY_ERROR is a command-level failure, not a
                    // transport failure. The message is dropped either way.
                    if (r->type == REDIS_REPLY_ERROR) {
                        reportPublishError(locator_, r);
                    }
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
            ConnectionLocator locator_;
            TLSClientConfigurationComponent *tlsConf_;
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
            std::atomic<bool> connected_;
            std::mutex reconnectMutex_;
            std::condition_variable reconnectCondition_;
            OneRedisSender *sender_;

            bool waitBeforeReconnect(std::chrono::seconds delay) {
                RedisComponentImpl::logRedisMessage(
                    locator_, "RedisRPCClient",
                    "retrying connection in "+std::to_string(delay.count())+" seconds"
                );
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
            void abandonOutstandingRequests() {
                std::lock_guard<std::mutex> lock(clientsMutex_);
                clientToIDMap_.clear();
                idToClientMap_.clear();
            }
            void stop() {
                running_ = false;
                connected_ = false;
                reconnectCondition_.notify_all();
                if (th_.joinable()) {
                    try {
                        th_.join();
                    } catch (std::system_error const &) {
                    }
                }
                abandonOutstandingRequests();
            }
            void run(redisContext *ctx) {
                std::chrono::seconds reconnectDelay(1);
                bool recovering = (ctx == nullptr);
                while (running_) {
                    if (ctx == nullptr) {
                        connected_ = false;
                        sender_->checkConnectionAfterReceiverFailure();
                        abandonOutstandingRequests();
                        if (waitBeforeReconnect(reconnectDelay)) {
                            break;
                        }
                        increaseReconnectDelay(reconnectDelay);
                        ctx = RedisComponentImpl::createRPCSubscriptionContext(
                            locator_, tlsConf_, myCommunicationID_, "RedisRPCClient"
                        );
                        if (ctx == nullptr) {
                            continue;
                        }
                    }

                    connected_ = true;
                    if (recovering) {
                        RedisComponentImpl::logRedisMessage(
                            locator_, "RedisRPCClient",
                            "reconnected and re-subscribed to reply topic '"+myCommunicationID_+"'"
                        );
                        recovering = false;
                    }
                    reconnectDelay = std::chrono::seconds(1);

                    while (running_) {
                        redisReply *reply = nullptr;
                        int result = redisGetReply(ctx, (void **) &reply);
                        if (result != REDIS_OK) {
                            if (reply != nullptr) {
                                freeReplyObject((void *) reply);
                            }
                            if (ctx->err == REDIS_ERR_IO && errno == EAGAIN) {
                                ctx->err = 0;
                                continue;
                            }
                            std::string detail = "receive failed";
                            if (ctx->errstr[0] != '\0') {
                                detail += ": ";
                                detail += ctx->errstr;
                            }
                            RedisComponentImpl::logRedisMessage(locator_, "RedisRPCClient", detail);
                            break;
                        }
                        if (!running_ || reply == nullptr) {
                            if (reply != nullptr) {
                                freeReplyObject((void *) reply);
                            }
                            continue;
                        }
                        if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 3
                            || reply->element[0] == nullptr || reply->element[1] == nullptr
                            || reply->element[2] == nullptr
                            || reply->element[0]->type != REDIS_REPLY_STRING
                            || reply->element[1]->type != REDIS_REPLY_STRING
                            || reply->element[2]->type != REDIS_REPLY_STRING
                            || reply->element[0]->str == nullptr
                            || reply->element[1]->str == nullptr
                            || reply->element[2]->str == nullptr
                            || std::string_view(reply->element[0]->str, reply->element[0]->len) != "message"
                            || std::string_view(reply->element[1]->str, reply->element[1]->len) != myCommunicationID_) {
                            freeReplyObject((void *) reply);
                            continue;
                        }

                        std::size_t encodedLength = reply->element[2]->len;
                        auto parseRes = basic::bytedata_utils::RunCBORDeserializer<std::tuple<bool,basic::ByteDataWithID>>::apply(
                            std::string_view {reply->element[2]->str, reply->element[2]->len}, 0
                        );
                        freeReplyObject((void *) reply);
                        if (!parseRes || std::get<1>(*parseRes) != encodedLength) {
                            continue;
                        }

                        auto parsed = std::move(std::get<0>(*parseRes));
                        bool isFinal = std::get<0>(parsed);
                        basic::ByteDataWithID response = std::move(std::get<1>(parsed));
                        std::function<void(bool, basic::ByteDataWithID &&)> callback;
                        std::optional<WireToUserHook> hook;
                        {
                            std::lock_guard<std::mutex> lock(clientsMutex_);
                            auto mapping = idToClientMap_.find(response.id);
                            if (mapping == idToClientMap_.end()) {
                                continue;
                            }
                            uint32_t clientNumber = mapping->second;
                            auto client = clients_.find(clientNumber);
                            if (client != clients_.end()) {
                                callback = client->second.callback_;
                                hook = client->second.wireToUserHook_;
                            }
                            if (isFinal) {
                                auto requestSet = clientToIDMap_.find(clientNumber);
                                if (requestSet != clientToIDMap_.end()) {
                                    requestSet->second.erase(response.id);
                                }
                                idToClientMap_.erase(mapping);
                            }
                        }
                        if (!callback) {
                            continue;
                        }
                        try {
                            if (hook) {
                                auto transformed = hook->hook(
                                    basic::ByteDataView {std::string_view(response.content)}
                                );
                                if (transformed) {
                                    callback(isFinal, {
                                        std::move(response.id), std::move(transformed->content)
                                    });
                                }
                            } else {
                                callback(isFinal, std::move(response));
                            }
                        } catch (std::exception const &e) {
                            RedisComponentImpl::logRedisMessage(
                                locator_, "RedisRPCClient", std::string("response callback failed: ")+e.what()
                            );
                        } catch (...) {
                            RedisComponentImpl::logRedisMessage(
                                locator_, "RedisRPCClient", "response callback failed with an unknown exception"
                            );
                        }
                    }

                    connected_ = false;
                    redisFree(ctx);
                    ctx = nullptr;
                    if (running_) {
                        sender_->checkConnectionAfterReceiverFailure();
                    }
                    abandonOutstandingRequests();
                    recovering = true;
                }
                connected_ = false;
            }
        public:
            OneRedisRPCClientConnection(ConnectionLocator const &locator, std::string const &myCommunicationID, OneRedisSender *sender, TLSClientConfigurationComponent *tlsConf)
                : locator_(locator)
                , tlsConf_(tlsConf)
                , rpcTopic_(locator.identifier())
                , myCommunicationID_(myCommunicationID)
                , clientCounter_(0)
                , clients_()
                , clientToIDMap_()
                , idToClientMap_()
                , clientsMutex_()
                , th_()
                , running_(true)
                , connected_(false)
                , reconnectMutex_()
                , reconnectCondition_()
                , sender_(sender)
            {
                redisContext *initialContext = RedisComponentImpl::createRPCSubscriptionContext(
                    locator_, tlsConf_, myCommunicationID_, "RedisRPCClient"
                );
                connected_ = (initialContext != nullptr);
                try {
                    th_ = std::thread(&OneRedisRPCClientConnection::run, this, initialContext);
                } catch (...) {
                    connected_ = false;
                    if (initialContext != nullptr) {
                        redisFree(initialContext);
                    }
                    throw;
                }
            }
            ~OneRedisRPCClientConnection() {
                stop();
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
                stop();
            }
            void sendRequest(uint32_t clientNumber, basic::ByteDataWithID &&data) {
                if (!connected_ || !sender_->isConnected()) {
                    return;
                }
                {
                    std::lock_guard<std::mutex> _(clientsMutex_);
                    if (!connected_ || !sender_->isConnected()
                        || clients_.find(clientNumber) == clients_.end()) {
                        return;
                    }
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
            ConnectionLocator locator_;
            TLSClientConfigurationComponent *tlsConf_;
            std::string rpcTopic_;
            std::function<void(basic::ByteDataWithID &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::unordered_map<std::string, std::string> replyTopicMap_;
            std::thread th_;
            std::mutex mutex_;
            OneRedisSender *sender_;
            std::atomic<bool> running_;
            std::atomic<bool> connected_;
            std::mutex reconnectMutex_;
            std::condition_variable reconnectCondition_;

            bool waitBeforeReconnect(std::chrono::seconds delay) {
                RedisComponentImpl::logRedisMessage(
                    locator_, "RedisRPCServer",
                    "retrying connection in "+std::to_string(delay.count())+" seconds"
                );
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
            void abandonOutstandingRequests() {
                std::lock_guard<std::mutex> lock(mutex_);
                replyTopicMap_.clear();
            }
            void removeReplyTopic(std::string const &id) {
                std::lock_guard<std::mutex> lock(mutex_);
                replyTopicMap_.erase(id);
            }
            void stop() {
                running_ = false;
                connected_ = false;
                reconnectCondition_.notify_all();
                if (th_.joinable()) {
                    try {
                        th_.join();
                    } catch (std::system_error const &) {
                    }
                }
                abandonOutstandingRequests();
            }
            void run(redisContext *ctx) {
                std::chrono::seconds reconnectDelay(1);
                bool recovering = (ctx == nullptr);
                while (running_) {
                    if (ctx == nullptr) {
                        connected_ = false;
                        sender_->checkConnectionAfterReceiverFailure();
                        abandonOutstandingRequests();
                        if (waitBeforeReconnect(reconnectDelay)) {
                            break;
                        }
                        increaseReconnectDelay(reconnectDelay);
                        ctx = RedisComponentImpl::createRPCSubscriptionContext(
                            locator_, tlsConf_, rpcTopic_, "RedisRPCServer"
                        );
                        if (ctx == nullptr) {
                            continue;
                        }
                    }

                    connected_ = true;
                    if (recovering) {
                        RedisComponentImpl::logRedisMessage(
                            locator_, "RedisRPCServer",
                            "reconnected and re-subscribed to request topic '"+rpcTopic_+"'"
                        );
                        recovering = false;
                    }
                    reconnectDelay = std::chrono::seconds(1);

                    while (running_) {
                        redisReply *reply = nullptr;
                        int result = redisGetReply(ctx, (void **) &reply);
                        if (result != REDIS_OK) {
                            if (reply != nullptr) {
                                freeReplyObject((void *) reply);
                            }
                            if (ctx->err == REDIS_ERR_IO && errno == EAGAIN) {
                                ctx->err = 0;
                                continue;
                            }
                            std::string detail = "receive failed";
                            if (ctx->errstr[0] != '\0') {
                                detail += ": ";
                                detail += ctx->errstr;
                            }
                            RedisComponentImpl::logRedisMessage(locator_, "RedisRPCServer", detail);
                            break;
                        }
                        if (!running_ || reply == nullptr) {
                            if (reply != nullptr) {
                                freeReplyObject((void *) reply);
                            }
                            continue;
                        }
                        if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 3
                            || reply->element[0] == nullptr || reply->element[1] == nullptr
                            || reply->element[2] == nullptr
                            || reply->element[0]->type != REDIS_REPLY_STRING
                            || reply->element[1]->type != REDIS_REPLY_STRING
                            || reply->element[2]->type != REDIS_REPLY_STRING
                            || reply->element[0]->str == nullptr
                            || reply->element[1]->str == nullptr
                            || reply->element[2]->str == nullptr
                            || std::string_view(reply->element[0]->str, reply->element[0]->len) != "message"
                            || std::string_view(reply->element[1]->str, reply->element[1]->len) != rpcTopic_) {
                            freeReplyObject((void *) reply);
                            continue;
                        }

                        std::size_t encodedLength = reply->element[2]->len;
                        auto parseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithTopic>::apply(
                            std::string_view {reply->element[2]->str, reply->element[2]->len}, 0
                        );
                        freeReplyObject((void *) reply);
                        if (!parseRes || std::get<1>(*parseRes) != encodedLength) {
                            continue;
                        }
                        basic::ByteDataWithTopic requestEnvelope = std::move(std::get<0>(*parseRes));
                        auto innerParseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithID>::apply(
                            std::string_view {requestEnvelope.content}, 0
                        );
                        if (!innerParseRes || std::get<1>(*innerParseRes) != requestEnvelope.content.length()) {
                            continue;
                        }
                        basic::ByteDataWithID request = std::move(std::get<0>(*innerParseRes));
                        std::string requestID = request.id;
                        {
                            std::lock_guard<std::mutex> lock(mutex_);
                            replyTopicMap_[requestID] = std::move(requestEnvelope.topic);
                        }
                        try {
                            if (wireToUserHook_) {
                                auto transformed = wireToUserHook_->hook(
                                    basic::ByteDataView {std::string_view(request.content)}
                                );
                                if (transformed) {
                                    callback_({std::move(request.id), std::move(transformed->content)});
                                } else {
                                    removeReplyTopic(requestID);
                                }
                            } else {
                                callback_(std::move(request));
                            }
                        } catch (std::exception const &e) {
                            removeReplyTopic(requestID);
                            RedisComponentImpl::logRedisMessage(
                                locator_, "RedisRPCServer", std::string("request callback failed: ")+e.what()
                            );
                        } catch (...) {
                            removeReplyTopic(requestID);
                            RedisComponentImpl::logRedisMessage(
                                locator_, "RedisRPCServer", "request callback failed with an unknown exception"
                            );
                        }
                    }

                    connected_ = false;
                    redisFree(ctx);
                    ctx = nullptr;
                    if (running_) {
                        sender_->checkConnectionAfterReceiverFailure();
                    }
                    abandonOutstandingRequests();
                    recovering = true;
                }
                connected_ = false;
            }
        public:
            OneRedisRPCServerConnection(ConnectionLocator const &locator, std::function<void(basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, OneRedisSender *sender, TLSClientConfigurationComponent *tlsConf)
                : locator_(locator)
                , tlsConf_(tlsConf)
                , rpcTopic_(locator.identifier())
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , th_()
                , mutex_()
                , sender_(sender)
                , running_(true)
                , connected_(false)
                , reconnectMutex_()
                , reconnectCondition_()
            {
                redisContext *initialContext = RedisComponentImpl::createRPCSubscriptionContext(
                    locator_, tlsConf_, rpcTopic_, "RedisRPCServer"
                );
                connected_ = (initialContext != nullptr);
                try {
                    th_ = std::thread(&OneRedisRPCServerConnection::run, this, initialContext);
                } catch (...) {
                    connected_ = false;
                    if (initialContext != nullptr) {
                        redisFree(initialContext);
                    }
                    throw;
                }
            }
            ~OneRedisRPCServerConnection() {
                stop();
            }
            void sendReply(bool isFinal, basic::ByteDataWithID &&data) {
                if (!connected_ || !sender_->isConnected()) {
                    return;
                }
                std::string replyTopic;
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    if (!connected_ || !sender_->isConnected()) {
                        return;
                    }
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
            ConnectionLocator senderKey = d.copyOfBasicPortionWithProperties();
            auto senderIter = senders_.find(senderKey);
            if (senderIter == senders_.end()) {
                senderIter = senders_.insert({senderKey, std::make_unique<OneRedisSender>(d, tlsConf)}).first;
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
                throw RedisComponentException(
                    "Cannot create duplicate Redis RPC server connection for "
                    +l.host()+":"+std::to_string(l.port())+" topic '"+l.identifier()+"'"
                );
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
            rpcClientConnections_.clear();
            rpcServerConnections_.clear();
            // RPC receive workers notify their shared sender when their Redis
            // connection fails, so senders must outlive those workers.
            senders_.clear();
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
