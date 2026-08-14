#include <thread>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <unordered_map>
#include <chrono>
#include <iostream>
#include <cstdlib>
#include <array>

#include <tm_kit/transport/redis/RedisComponent.hpp>
#include <tm_kit/transport/TLSConfigurationComponent.hpp>

// BlockingConcurrentQueue used by OneRedisSenderAsyncQueue

#if __has_include(<concurrentqueue/moodycamel/blockingconcurrentqueue.h>)
#include <concurrentqueue/moodycamel/blockingconcurrentqueue.h>
#else
#include <concurrentqueue/blockingconcurrentqueue.h>
#endif

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

    // Configuration for reconnection behavior which could be modified by environment variables
    struct RedisReconnectConfig {
        int maxRetries;
        int initialBackoffMs;
        int maxBackoffMs;

        static RedisReconnectConfig fromEnv() {
            static RedisReconnectConfig config = [&]() {
                RedisReconnectConfig config;
                const char* maxRetriesEnv = std::getenv("TM_TRANSPORT_REDIS_MAX_RETRIES");
                config.maxRetries = (maxRetriesEnv && *maxRetriesEnv) ? std::atoi(maxRetriesEnv) : 10;

                const char* initialBackoffEnv = std::getenv("TM_TRANSPORT_REDIS_INITIAL_BACKOFF_MS");
                config.initialBackoffMs = (initialBackoffEnv && *initialBackoffEnv) ? std::atoi(initialBackoffEnv) : 100;

                const char* maxBackoffEnv = std::getenv("TM_TRANSPORT_REDIS_MAX_BACKOFF_MS");
                config.maxBackoffMs = (maxBackoffEnv && *maxBackoffEnv) ? std::atoi(maxBackoffEnv) : 30000;
                return config;
            }();
            return config;
        }
    };

    class RedisComponentImpl {
    private:
#if HIREDIS_USE_SSL
        static int initializeSSL() {
            redisInitOpenSSL();
            return 1;
        }
        static const int _ssl_initialized;
#endif

        static redisContext* connectWithRetry(ConnectionLocator const &locator, TLSClientConfigurationComponent *tlsConf, RedisReconnectConfig const &config) {
            int attempt = 0;
            int backoffMs = config.initialBackoffMs;

            while (true) {
                try {
                    auto ctx = connect(locator, tlsConf);
                    if (attempt > 0) {
                        std::cerr << "[RedisComponent] Successfully reconnected to " << locator.toSerializationFormat()
                                  << " after " << attempt << " attempts" << std::endl;
                    }
                    return ctx;
                } catch (std::exception const &e) {
                    attempt++;

                    std::cerr << "[RedisComponent] Connection attempt " << attempt << "/" << config.maxRetries
                              << " failed for " << locator.toSerializationFormat()
                              << ": " << e.what() << std::endl;

                    if (attempt >= config.maxRetries) {
                        std::string errorMsg = "Failed to connect to Redis after " + std::to_string(config.maxRetries) +
                                               " attempts: " + locator.toSerializationFormat() + ". Last error: " + e.what();
                        throw RedisComponentException(errorMsg);
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
                    backoffMs = std::min(backoffMs * 2, config.maxBackoffMs);
                }
            }
        }

        static redisContext* connect(ConnectionLocator const &locator, TLSClientConfigurationComponent *tlsConf) {
            auto ctx = redisConnect(locator.host().c_str(), locator.port());
            if (!ctx) {
                throw RedisComponentException("Failure to connect with Redis server: cannot create redis context");
            }

            if (ctx->err) {
                std::ostringstream oss;
                oss << "Failure to connect with Redis server \"" << locator.toSerializationFormat()
                    << "\". Error code: " << ctx->err;
                if (ctx->errstr[0] != '\0') {
                    oss << ", message: " << ctx->errstr;
                }
                redisFree(ctx);
                throw RedisComponentException(oss.str());
            }

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
                    redisFree(ctx);
                    throw RedisComponentException("Redis SSL context creation error");
                }
            } else {
                auto sslInfo = tlsConf?(tlsConf->getConfigurationItem(
                    TLSClientInfoKey {
                        locator.host(), (locator.port()==0?6379:locator.port())
                    }
                )):std::nullopt;

                if (sslInfo) {
                    ssl_context = redisCreateSSLContext(
                        sslInfo->caCertificateFile.c_str()
                        , nullptr
                        , sslInfo->clientCertificateFile.c_str()
                        , sslInfo->clientKeyFile.c_str()
                        , nullptr
                        , &ssl_error
                        );
                    if (ssl_context == nullptr || ssl_error != REDIS_SSL_CTX_NONE) {
                        redisFree(ctx);
                        throw RedisComponentException("Redis SSL context creation error");
                    }
                }
            }
            if (ssl_context != nullptr) {
                if (redisInitiateSSLWithContext(ctx, ssl_context) != REDIS_OK) {
                    redisFree(ctx);
                    throw RedisComponentException("Redis SSL negotiation error");
                }
            }
#endif
            if (!locator.password().empty()) {
                redisReply *r = nullptr;
                if (locator.userName() != "") {
                    r = (redisReply *) redisCommand(ctx, "AUTH %s %s", locator.userName().c_str(), locator.password().c_str());
                } else {
                    r = (redisReply *) redisCommand(ctx, "AUTH %s", locator.password().c_str());
                }
                if (!r) {
                    redisFree(ctx);
                    throw RedisComponentException("Failure to authenticate with Redis server for " + locator.toSerializationFormat() + ": no reply");
                }
                if (r->type == REDIS_REPLY_ERROR) {
                    std::string errMsg = r->str ? r->str : "unknown error";
                    freeReplyObject((void *) r);
                    redisFree(ctx);
                    throw RedisComponentException("Failure to authenticate with Redis server for " + locator.toSerializationFormat() + ": " + errMsg);
                }
                freeReplyObject((void *) r);
            }
            // do ping-pong test
            redisReply* r = static_cast<redisReply*>(redisCommand(ctx, "PING"));
            if (!r) {
                redisFree(ctx);
                throw RedisComponentException("Failure to connect to Redis server: PING command returned null");
            }
            if (r->type != REDIS_REPLY_STATUS || !r->str || std::strcmp(r->str, "PONG") != 0) {
                freeReplyObject(r);
                redisFree(ctx);
                throw RedisComponentException("Failure to connect to Redis server: ping-pong test failed");
            }
            freeReplyObject(r);
            return ctx;
        }

        class OneRedisSubscription {
        private:
            ConnectionLocator locator_;
            std::string topic_;
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
            TLSClientConfigurationComponent *tlsConf_;
            RedisReconnectConfig reconnectConfig_;

            static int getPollTimeoutMs() {
                static const int pollTimeoutMs = []() {
                const char *env = std::getenv("TM_TRANSPORT_REDIS_POLL_TIMEOUT_MS");
                if (env && *env) {
                    int val = std::atoi(env);
                    if (val >= 0) return val;
                }
                return 50;
            }();
                return pollTimeoutMs;
            }

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

            bool reconnect() {
                std::cerr << "[RedisSubscription] Attempting to reconnect to " << locator_.toSerializationFormat() << std::endl;

                if (ctx_) {
                    redisFree(ctx_);
                    ctx_ = nullptr;
                }

                // will throw if failed
                ctx_ = RedisComponentImpl::connectWithRetry(locator_, tlsConf_, reconnectConfig_);

                // Re-subscribe to the topic
                redisReply *r = (redisReply *) redisCommand(ctx_, "PSUBSCRIBE %s", topic_.c_str());
                if (!r) {
                    std::cerr << "[RedisSubscription] Failed to re-subscribe: no reply" << std::endl;
                    redisFree(ctx_);
                    ctx_ = nullptr;
                    return false;
                }
                if (r->type == REDIS_REPLY_ERROR) {
                    std::cerr << "[RedisSubscription] Failed to re-subscribe: " << (r->str ? r->str : "unknown error") << std::endl;
                    freeReplyObject((void *) r);
                    redisFree(ctx_);
                    ctx_ = nullptr;
                    return false;
                }
                freeReplyObject((void *) r);

                // Set timeout
                int pollTimeoutMs = getPollTimeoutMs();
                struct timeval tv = { pollTimeoutMs / 1000, (pollTimeoutMs % 1000) * 1000 };
                if (redisSetTimeout(ctx_, tv) != REDIS_OK) {
                    std::cerr << "[RedisSubscription] redisSetTimeout failed after reconnect" << std::endl;
                    redisFree(ctx_);
                    ctx_ = nullptr;
                    return false;
                }

                std::cerr << "[RedisSubscription] Successfully reconnected and re-subscribed to " << topic_ << std::endl;
                return true;
            }

            void run() {
                struct redisReply *reply = nullptr;
                int consecutiveErrors = 0;
                const int maxConsecutiveErrors = 10;

                while (running_) {
                    if (!ctx_ || ctx_->err) {
                        std::cerr << "[RedisSubscription] Connection error detected (err=" << (ctx_ ? ctx_->err : -1) << ")" << std::endl;
                        if (!reconnect()) {
                            std::cerr << "[RedisSubscription] Failed to reconnect, exiting subscription thread" << std::endl;
                            break;
                        }
                        consecutiveErrors = 0;
                        continue;
                    }

                    reply = nullptr;
                    int r = redisGetReply(ctx_, (void **) &reply);

                    if (r != REDIS_OK) {
                        if (ctx_->err == REDIS_ERR_EOF) {
                            std::cerr << "[RedisSubscription] Connection closed (EOF), attempting reconnect" << std::endl;
                            if (!reconnect()) {
                                std::cerr << "[RedisSubscription] Failed to reconnect, exiting subscription thread" << std::endl;
                                break;
                            }
                            consecutiveErrors = 0;
                            continue;
                        }

                        if (ctx_->err == REDIS_ERR_IO && errno == EAGAIN) {
                            ctx_->err = 0;
                            continue;
                        }

                        if (ctx_->err == 0) {
                            if (reply != nullptr) {
                                freeReplyObject((void *) reply);
                            }
                            continue;
                        }

                        // Other errors
                        consecutiveErrors++;
                        std::cerr << "[RedisSubscription] Redis error (consecutive=" << consecutiveErrors
                                    << "): code=" << ctx_->err << ", msg=" << ctx_->errstr << std::endl;

                        if (consecutiveErrors >= maxConsecutiveErrors) {
                            std::cerr << "[RedisSubscription] Too many consecutive errors, attempting reconnect" << std::endl;
                            if (!reconnect()) {
                                break;
                            }
                            consecutiveErrors = 0;
                        }
                        continue;
                    }

                    consecutiveErrors = 0;

                    if (!running_) {
                        break;
                    }
                    if (reply == nullptr) {
                        continue;
                    }

                    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 4) {
                        freeReplyObject((void *) reply);
                        continue;
                    }

                    if (reply->element[0]->type != REDIS_REPLY_STRING
                    || std::string_view(reply->element[0]->str, reply->element[0]->len) != "pmessage") {
                        freeReplyObject((void *) reply);
                        continue;
                    }

                    std::string topic(reply->element[2]->str, reply->element[2]->len);
                    std::string content(reply->element[3]->str, reply->element[3]->len);
                    freeReplyObject((void *) reply);

                    if (!running_) {
                        break;
                    }
                    std::lock_guard<std::mutex> _(mutex_);
                    std::size_t remaining = clients_.size();
                    for (auto const &cb : clients_) {
                        if (--remaining == 0) {
                            callClient(cb, {std::move(topic), std::move(content)});
                        } else {
                            callClient(cb, {topic, content});
                        }
                    }
                }
            }

        public:
            OneRedisSubscription(ConnectionLocator const &locator, std::string const &topic, TLSClientConfigurationComponent *tlsConf) 
                : locator_(locator)
                , topic_(topic)
                , ctx_(nullptr)
                , clients_()
                , th_()
                , mutex_()
                , running_(true)
                , tlsConf_(tlsConf)
                , reconnectConfig_(RedisReconnectConfig::fromEnv())
            {
                ctx_ = RedisComponentImpl::connectWithRetry(locator, tlsConf, reconnectConfig_);
                redisReply *r = (redisReply *) redisCommand(ctx_, "PSUBSCRIBE %s", topic.c_str());
                if (!r || r->type == REDIS_REPLY_ERROR) {
                    if (r) {
                        freeReplyObject((void *) r);
                    }
                    redisFree(ctx_);
                    ctx_ = nullptr;
                    throw RedisComponentException("Failed to subscribe to topic: " + topic);
                }
                freeReplyObject((void *) r);
                int pollTimeoutMs = getPollTimeoutMs();
                struct timeval tv = { pollTimeoutMs / 1000, (pollTimeoutMs % 1000) * 1000 };
                if (redisSetTimeout(ctx_, tv) != REDIS_OK) {
                    redisFree(ctx_);
                    ctx_ = nullptr;
                    throw RedisComponentException("redisSetTimeout failed");
                }
                th_ = std::thread(&OneRedisSubscription::run, this);
            }

            ~OneRedisSubscription() {
                running_ = false;
                if (th_.joinable()) {
                    try {
                        th_.join();
                    } catch (std::system_error const &) {
                    }
                }
                if (ctx_) {
                    redisFree(ctx_);
                    ctx_ = nullptr;
                }
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
                running_ = false;
                if (th_.joinable()) {
                    try {
                        th_.join();
                    } catch (std::system_error const &) {
                    }
                }
                if (ctx_ && !ctx_->err) {
                    redisReply *r = (redisReply *) redisCommand(ctx_, "PUNSUBSCRIBE %s", topic_.c_str());
                    if (r) {
                        freeReplyObject((void *) r);
                    }
                }
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

        // Redis Sender Base Class
        class OneRedisSenderBase {
        public:
            virtual ~OneRedisSenderBase() = default;
            virtual void publish(basic::ByteDataWithTopic &&data) = 0;
        };

        class OneRedisSenderSync final : public OneRedisSenderBase {
        private:
            static constexpr int MAX_SENDER_COUNT = 8;
            ConnectionLocator locator_;
            TLSClientConfigurationComponent *tlsConf_;
            RedisReconnectConfig reconnectConfig_;

            std::vector<redisContext*> pool_;
            std::vector<redisContext*> free_;
            std::uint64_t senderId_;
            std::mutex poolMutex_;

            static std::uint64_t nextSenderId() {
                static std::atomic<std::uint64_t> counter {0};
                auto id = counter.fetch_add(1, std::memory_order_relaxed);
                if (id >= MAX_SENDER_COUNT) {
                    throw std::runtime_error("reaches max redis sender count: " + std::to_string(MAX_SENDER_COUNT));
                }
                return id;
            }

            __attribute__((always_inline)) inline redisContext* getThreadContext() {
                static thread_local std::array<redisContext*, MAX_SENDER_COUNT> threadContexts{};
                auto &m = threadContexts;
                redisContext *ctx = m[senderId_];

                if (__builtin_expect(!!(ctx), 1)) {
                    if (__builtin_expect(!!(ctx->err), 0)) {
                        std::cerr << "[RedisSenderSync] Reconnecting context for " << locator_.toSerializationFormat() << std::endl;
                        ctx = RedisComponentImpl::connectWithRetry(locator_, tlsConf_, reconnectConfig_);
                        {
                            std::lock_guard<std::mutex> _(poolMutex_);
                            pool_.push_back(ctx);
                        }
                        m[senderId_] = ctx;
                    }
                    return ctx;
                } else {
                    {
                        std::lock_guard<std::mutex> _(poolMutex_);
                        if (!free_.empty()) {
                            ctx = free_.back();
                            free_.pop_back();
                            if (ctx->err) {
                                // just leave the useless redisContext in the pool
                                std::cerr << "[RedisSenderSync] Reconnecting context for " << locator_.toSerializationFormat() << std::endl;
                                ctx = RedisComponentImpl::connectWithRetry(locator_, tlsConf_, reconnectConfig_);
                                pool_.push_back(ctx);
                            }
                        } else {
                            ctx = RedisComponentImpl::connectWithRetry(locator_, tlsConf_, reconnectConfig_);
                            pool_.push_back(ctx);
                        }
                    }
                    m[senderId_] = ctx; // update cache
                    return ctx;
                }
            }

        public:
            OneRedisSenderSync(ConnectionLocator const &locator, TLSClientConfigurationComponent *tlsConf)
                : locator_(locator)
                , tlsConf_(tlsConf)
                , reconnectConfig_(RedisReconnectConfig::fromEnv())
                , poolMutex_()
                , pool_()
                , free_()
                , senderId_(nextSenderId())
            {
                pool_.reserve(8);
                // add one context to pool before actually publish
                redisContext *ctx = RedisComponentImpl::connectWithRetry(locator_, tlsConf_, reconnectConfig_);
                pool_.push_back(ctx);
                free_.push_back(ctx);
            }

            ~OneRedisSenderSync() {
                std::lock_guard<std::mutex> _(poolMutex_);
                for (auto *ctx : pool_) {
                    if (ctx) {
                        redisFree(ctx);
                    }
                }
                pool_.clear();
                free_.clear();
            }

            void publish(basic::ByteDataWithTopic &&data) override {
                redisContext *ctx = getThreadContext();
                redisReply *r = (redisReply *) redisCommand(
                    ctx
                    , "PUBLISH %s %b"
                    , data.topic.c_str()
                    , data.content.c_str()
                    , data.content.length()
                );

                if (r != nullptr) {
                    if (r->type == REDIS_REPLY_ERROR) {
                        std::cerr << "[RedisSenderSync] PUBLISH command error: " << (r->str ? r->str : "unknown") << std::endl;
                    }
                    freeReplyObject((void *) r);
                    return; // Success or command-level error
                } else {
                    std::cerr << "[RedisSenderSync] PUBLISH command return null reply" << std::endl;
                }
            }
        };

        class OneRedisSenderAsyncQueue final : public OneRedisSenderBase {
        private:
            ConnectionLocator locator_;
            TLSClientConfigurationComponent *tlsConf_{nullptr};
            redisContext *ctx_{nullptr};
            RedisReconnectConfig reconnectConfig_;

            moodycamel::BlockingConcurrentQueue<basic::ByteDataWithTopic> queue_;
            std::thread senderThread_;
            std::atomic<bool> running_{false};

            bool useDepthHeuristic_{false};
            bool drainQueueOnShutdown_{false};

            static constexpr size_t QUEUE_CAPACITY = 16384;
            static constexpr size_t BATCH_SIZE_DEFAULT = 128;
            static constexpr size_t BATCH_SIZE_MIN = 32;
            static constexpr size_t BATCH_SIZE_MAX = 512;

            static constexpr std::int64_t TIMEOUT_MIN_US = 10;
            static constexpr std::int64_t TIMEOUT_MAX_US = 1000;

            static constexpr double DEPTH_THRESHOLD_FACTOR = 1.0;
            static constexpr size_t DEPTH_HIGH = static_cast<size_t>(BATCH_SIZE_MAX * DEPTH_THRESHOLD_FACTOR);
            static constexpr size_t DEPTH_MEDIUM = static_cast<size_t>(BATCH_SIZE_DEFAULT * DEPTH_THRESHOLD_FACTOR);

            void reconnect() {
                std::cerr << "[RedisSenderAsync] Attempting to reconnect to " << locator_.toSerializationFormat() << std::endl;
                if (ctx_) {
                    redisFree(ctx_);
                }
                ctx_ = RedisComponentImpl::connectWithRetry(locator_, tlsConf_, reconnectConfig_);
                std::cerr << "[RedisSenderAsync] Successfully reconnected" << std::endl;
            }

            void senderThreadMain() {
                std::vector<basic::ByteDataWithTopic> batch;
                batch.reserve(BATCH_SIZE_MAX);

                std::int64_t timeout_us = TIMEOUT_MIN_US;
                size_t batch_size = BATCH_SIZE_DEFAULT;
                
                static constexpr int maxConsecutiveErrors = 3;
                int consecutiveErrors = 0;

                while (running_.load(std::memory_order_relaxed)) {
                    if (!ctx_ || ctx_->err) {
                        std::cerr << "[RedisSenderAsync] Connection error detected in sender thread" << std::endl;
                        reconnect();
                        ++consecutiveErrors;
                    }

                    batch.clear();

                    if (useDepthHeuristic_) {
                        size_t approx_size = queue_.size_approx();
                        if (approx_size > DEPTH_HIGH) {
                            timeout_us = TIMEOUT_MIN_US;
                            batch_size = BATCH_SIZE_MAX;
                        } else if (approx_size > DEPTH_MEDIUM) {
                            timeout_us = TIMEOUT_MIN_US * 5;
                            batch_size = BATCH_SIZE_DEFAULT;
                        } else {
                            timeout_us = TIMEOUT_MIN_US;
                            batch_size = BATCH_SIZE_MIN;
                        }
                    } else {
                        batch_size = BATCH_SIZE_DEFAULT;
                    }

                    size_t n = queue_.wait_dequeue_bulk_timed(
                        std::back_inserter(batch),
                        batch_size,
                        std::chrono::microseconds(timeout_us)
                    );

                    if (n == 0) {
                        if (!useDepthHeuristic_) {
                            timeout_us = std::min(TIMEOUT_MAX_US, timeout_us * 5);
                        }
                        continue;
                    }

                    if (!useDepthHeuristic_) {
                        timeout_us = TIMEOUT_MIN_US;
                    }

                    // Pipeline all messages
                    bool succeed = true;
                    for (size_t i = 0; i < n; ++i) {
                        int appendResult = redisAppendCommand(
                            ctx_,
                            "PUBLISH %s %b",
                            batch[i].topic.c_str(),
                            batch[i].content.c_str(),
                            batch[i].content.length()
                        );
                        if (appendResult != REDIS_OK) {
                            std::cerr << "[RedisSenderAsync] redisAppendCommand failed with return " << appendResult << std::endl;
                            succeed = false;
                            break;
                        }
                    }
                    if (!succeed) {
                        std::cerr << "[RedisSenderAsync] Append current batch data failed, dropped " << n << " data" << std::endl;
                        ++consecutiveErrors;
                        reconnect();
                        continue;   // drop current batch
                    }

                    // Flush
                    int done = 0;
                    int flushResult = redisBufferWrite(ctx_, &done);
                    if (flushResult != REDIS_OK || ctx_->err) {
                        std::cerr << "[RedisSenderAsync] redisBufferWrite failed: " << (ctx_->errstr[0] ? ctx_->errstr : "unknown error") << std::endl;
                        ++consecutiveErrors;
                        reconnect();
                        continue;
                    }

                    // Collect replies
                    bool hasError = false;
                    for (size_t i = 0; i < n; ++i) {
                        redisReply *r = nullptr;
                        int replyResult = redisGetReply(ctx_, (void**)&r);
                        if (replyResult != REDIS_OK) {
                            std::cerr << "[RedisSenderAsync] redisGetReply failed for message " << i << std::endl;
                            hasError = true;
                            if (r) {
                                freeReplyObject(r);
                            }
                            break;
                        }
                        if (r) {
                            if (r->type == REDIS_REPLY_ERROR) {
                                std::cerr << "[RedisSenderAsync] PUBLISH error for message " << i << ": " << (r->str ? r->str : "unknown") << std::endl;
                                hasError = true;
                                freeReplyObject(r);
                                break;
                            } else {
                                freeReplyObject(r);
                            }
                        } else {
                            std::cerr << "[RedisSenderAsync] PUBLISH error for message " << i << ". Got null reply" << std::endl;
                            hasError = true;
                            break;
                        }
                    }

                    if (hasError) {
                        consecutiveErrors++;
                        if (consecutiveErrors >= maxConsecutiveErrors) {
                            throw RedisComponentException("[RedisSenderAsync] Too many consecutive error in sender thread");
                        } else {
                            reconnect();
                            continue;
                        }
                    }
                    consecutiveErrors = 0;
                }

                // Drain queue on shutdown
                if (drainQueueOnShutdown_) {
                    batch.clear();
                    while (true) {
                        basic::ByteDataWithTopic item;
                        if (!queue_.try_dequeue(item)) {
                            break;
                        }
                        batch.push_back(std::move(item));
                        if (batch.size() >= QUEUE_CAPACITY) {
                            break;
                        }
                    }

                    if (!batch.empty() && ctx_ && !ctx_->err) {
                        for (auto &item : batch) {
                            redisAppendCommand(
                                ctx_,
                                "PUBLISH %s %b",
                                item.topic.c_str(),
                                item.content.c_str(),
                                item.content.length()
                            );
                        }
                        int done = 0;
                        redisBufferWrite(ctx_, &done);
                        for (size_t i = 0; i < batch.size(); ++i) {
                            redisReply *r = nullptr;
                            if (redisGetReply(ctx_, (void**)&r) == REDIS_OK && r != nullptr) {
                                freeReplyObject(r);
                            }
                        }
                    }
                }
            }

        public:
            OneRedisSenderAsyncQueue(ConnectionLocator const &locator, TLSClientConfigurationComponent *tlsConf)
                : locator_(locator)
                , tlsConf_(tlsConf)
                , ctx_(nullptr)
                , reconnectConfig_(RedisReconnectConfig::fromEnv())
                , queue_(QUEUE_CAPACITY)
                , running_(true)
            {
                ctx_ = RedisComponentImpl::connectWithRetry(locator, tlsConf, reconnectConfig_);

                // change send batch size and wait timeout depend on sender queue depth
                const char *depthEnv = std::getenv("TM_REDIS_SENDER_USE_DEPTH_HEURISTIC");
                useDepthHeuristic_ = (depthEnv && std::string_view(depthEnv) == "1");

                const char* drainQueueEnv = std::getenv("TM_REDIS_DRAIN_QUEUE_ON_SHUTDOWN");
                drainQueueOnShutdown_ = (drainQueueEnv && std::string_view(drainQueueEnv) == "1");

                senderThread_ = std::thread([this]() { senderThreadMain(); });
            }

            ~OneRedisSenderAsyncQueue() {
                running_.store(false, std::memory_order_relaxed);
                if (senderThread_.joinable()) {
                    senderThread_.join();
                }
                if (ctx_) {
                    redisFree(ctx_);
                }
            }

            void publish(basic::ByteDataWithTopic &&data) override {
                queue_.enqueue(std::move(data));
            }
        };

        static std::unique_ptr<OneRedisSenderBase> createSender(
            ConnectionLocator const &locator,
            TLSClientConfigurationComponent *tlsConf
        ) {
            const char *mode= "async"; // default
            if(auto e = std::getenv("TM_REDIS_SENDER_MODE"); e) {
                mode = e;
            }
            if (std::string_view(mode) == "async") {
                return std::make_unique<OneRedisSenderAsyncQueue>(locator, tlsConf);
            } else if (std::string_view(mode) == "sync") {
                return std::make_unique<OneRedisSenderSync>(locator, tlsConf);
            } else {
                throw std::runtime_error("invalid \"TM_REDIS_SENDER_MODE\" value: " + std::string(mode));
            }
        }

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRedisSenderBase>> senders_;

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
            OneRedisSenderBase *sender_;
            void run() {
                struct redisReply *reply = nullptr;
                try {
                    while (running_) {
                        struct timeval tv = { 0, 1000 };
                        if (redisSetTimeout(ctx_, tv) != REDIS_OK) {
                            break;
                        }
                        if (!ctx_ || ctx_->err) {
                            throw RedisComponentException("Redis context error");
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
                        // string_view compares: neither the kind tag nor the topic
                        // check needs to allocate.
                        if (reply->element[0]->type != REDIS_REPLY_STRING
                            ||
                            std::string_view(reply->element[0]->str, reply->element[0]->len) != "message") {
                            freeReplyObject((void *) reply);
                            continue;
                        }
                        if (std::string_view(reply->element[1]->str, reply->element[1]->len) != myCommunicationID_) {
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
            OneRedisRPCClientConnection(ConnectionLocator const &locator, std::string const &myCommunicationID, OneRedisSenderBase *sender, TLSClientConfigurationComponent *tlsConf)
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
                ctx_ = RedisComponentImpl::connect(locator, tlsConf);
                redisReply *r = (redisReply *) redisCommand(ctx_, "SUBSCRIBE %s", myCommunicationID_.c_str());
                freeReplyObject((void *) r);
                th_ = std::thread(&OneRedisRPCClientConnection::run, this);
                th_.detach();
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
            OneRedisSenderBase *sender_;
            std::atomic<bool> running_;
            void run() {
                struct redisReply *reply = nullptr;
                while (running_) {
                    struct timeval tv = { 0, 1000 };
                    if (redisSetTimeout(ctx_, tv) != REDIS_OK) {
                        break;
                    }
                    if (!ctx_ || ctx_->err) {
                        throw RedisComponentException("Redis context error");
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
                    // string_view compares: neither the kind tag nor the topic
                    // check needs to allocate.
                    if (reply->element[0]->type != REDIS_REPLY_STRING
                        ||
                        std::string_view(reply->element[0]->str, reply->element[0]->len) != "message") {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    if (std::string_view(reply->element[1]->str, reply->element[1]->len) != rpcTopic_) {
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
            OneRedisRPCServerConnection(ConnectionLocator const &locator, std::function<void(basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, OneRedisSenderBase *sender, TLSClientConfigurationComponent *tlsConf)
                : ctx_(nullptr)
                , rpcTopic_(locator.identifier())
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , th_()
                , mutex_()
                , sender_(sender)
                , running_(true)
            {
                ctx_ = RedisComponentImpl::connect(locator, tlsConf);
                redisReply *r = (redisReply *) redisCommand(ctx_, "SUBSCRIBE %s", rpcTopic_.c_str());
                freeReplyObject((void *) r);
                th_ = std::thread(&OneRedisRPCServerConnection::run, this);
                th_.detach();
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
            std::lock_guard<std::mutex> _(mutex_);
            if (p->checkWhetherNeedsToStop()) {
                p->unsubscribe();
                ConnectionLocator hostAndPort {p->locator().host(), p->locator().port()};
                auto iter = subscriptions_.find(hostAndPort);
                if (iter != subscriptions_.end()) {
                    auto innerIter = iter->second.find(p->topic());
                    if (innerIter != iter->second.end()) {
                        innerIter->second.release();
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
            }
        }

        OneRedisSenderBase *getOrStartSender(ConnectionLocator const &d, TLSClientConfigurationComponent *tlsConf) {
            std::lock_guard<std::mutex> _(mutex_);
            return getOrStartSenderNoLock(d, tlsConf);
        }

        OneRedisSenderBase *getOrStartSenderNoLock(ConnectionLocator const &d, TLSClientConfigurationComponent *tlsConf) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            auto senderIter = senders_.find(hostAndPort);
            if (senderIter == senders_.end()) {
                senderIter = senders_.insert({hostAndPort, createSender(d, tlsConf)}).first;
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

        OneRedisRPCServerConnection *createRpcServerConnection(ConnectionLocator const &l, std::function<void(basic::ByteDataWithID &&)> handler,
                                                                std::optional<WireToUserHook> wireToUserHook, TLSClientConfigurationComponent *tlsConf) {
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
                    auto *p = iter->second.release();
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
