#include <thread>
#include <mutex>
#include <atomic>
#include <cstring>
#include <sstream>
#include <unordered_map>

#include <tm_kit/transport/redis/RedisComponent.hpp>

#ifdef _MSC_VER
#include <winsock2.h>
#include <Shlwapi.h>
#else
#include <fnmatch.h> 
#endif
#include <hiredis/hiredis.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace redis {
    class RedisComponentImpl {
    private:
        class OneRedisSubscription {
        private:
            static bool isMatch(std::string const &subTopic, std::string const &realTopic) {
#ifdef _MSC_VER
                return (PathMatchSpecExA(realTopic.c_str(), subTopic.c_str(), PMSF_NORMAL));
#else
                return (fnmatch(subTopic.c_str(), realTopic.c_str(), 0) == 0);
#endif
            }
            redisContext *ctx_;
            struct ClientCB {
                std::function<void(basic::ByteDataWithTopic &&)> cb;
                std::optional<WireToUserHook> hook;
            };
            std::unordered_map<std::string, std::vector<ClientCB>> clients_;
            std::thread th_;
            std::mutex mutex_;
            std::atomic<bool> running_;

            inline void callClient(ClientCB const &c, basic::ByteDataWithTopic &&d) {
                if (c.hook) {
                    auto b = (c.hook->hook)(basic::ByteData {std::move(d.content)});
                    if (b) {
                        c.cb({std::move(d.topic), std::move(b->content)});
                    }
                } else {
                    c.cb(std::move(d));
                }
            }

            inline void handleDataByTopic(std::string const &topic, std::string const &content) {
                std::lock_guard<std::mutex> _(mutex_);
                for (auto const &item : clients_) {
                    if (isMatch(item.first, topic)) {
                        for (auto const &cb : item.second) {
                            callClient(cb, {topic, content});
                        }
                    }
                }
            }
            inline void handleDataByPTopic(std::string const &ptopic, std::string const &topic, std::string const &content) {
                std::lock_guard<std::mutex> _(mutex_);
                auto iter = clients_.find(ptopic);
                if (iter != clients_.end()) {
                    for (auto const &cb : iter->second) {
                        callClient(cb, {topic, content});
                    }
                }
            }
            void run() {
                struct redisReply *reply = nullptr;
                while (running_) {
                    int r;
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        r = redisGetReply(ctx_, (void **) &reply);
                    }
                    if (r != REDIS_OK) {
                        if (reply != nullptr) {
                            freeReplyObject((void *) &reply);
                        }
                        continue;
                    }
                    if (reply->type != REDIS_REPLY_ARRAY || (reply->elements != 4 && reply->elements != 3)) {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    if (reply->elements == 3) {
                        if (reply->element[0]->type != REDIS_REPLY_STRING
                            ||
                            std::string(reply->element[0]->str, reply->element[0]->len) != "message") {
                            freeReplyObject((void *) reply);
                            continue;
                        }
                        std::string topic(reply->element[1]->str, reply->element[1]->len);
                        std::string content(reply->element[2]->str, reply->element[2]->len);
                        freeReplyObject((void *) reply);
                        handleDataByTopic(topic, content);
                    } else {
                        if (reply->element[0]->type != REDIS_REPLY_STRING
                            ||
                            std::string(reply->element[0]->str, reply->element[0]->len) != "pmessage") {
                            freeReplyObject((void *) reply);
                            continue;
                        }
                        std::string ptopic(reply->element[1]->str, reply->element[1]->len);
                        std::string topic(reply->element[2]->str, reply->element[2]->len);
                        std::string content(reply->element[3]->str, reply->element[3]->len);
                        freeReplyObject((void *) reply);
                        handleDataByPTopic(ptopic, topic, content);
                    }
                }
            }
        public:
            OneRedisSubscription(ConnectionLocator const &locator) 
                : ctx_(nullptr)
                , clients_()
                , th_()
                , mutex_()
                , running_(false)
            {
                ctx_ = redisConnect(locator.host().c_str(), locator.port());
                if (ctx_ != nullptr) {
                    running_ = true;
                    th_ = std::thread(&OneRedisSubscription::run, this);
                }
            }
            ~OneRedisSubscription() {
                if (ctx_ != nullptr) {
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        for (auto const &item : clients_) {
                            redisReply *r = (redisReply *) redisCommand(ctx_, "PUNSUBSCRIBE %s", item.first.c_str());
                            freeReplyObject((void *) r);
                        }
                    }
                    running_ = false;
                    th_.join();
                    redisFree(ctx_);
                } 
            }
            void addSubscription(
                std::string const &topic
                , std::function<void(basic::ByteDataWithTopic &&)> handler
                , std::optional<WireToUserHook> wireToUserHook
            ) {
                bool needSubscribe = false;
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    auto iter = clients_.find(topic);
                    if (iter == clients_.end()) {
                        iter = clients_.insert({topic, std::vector<ClientCB> {}}).first;
                        needSubscribe = true;
                    }
                    iter->second.push_back({handler, wireToUserHook});
                }
                if (needSubscribe) {
                    std::lock_guard<std::mutex> _(mutex_);
                    redisReply *r = (redisReply *) redisCommand(ctx_, "PSUBSCRIBE %s", topic.c_str());
                    freeReplyObject((void *) r);
                } 
            }  
        };
        
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRedisSubscription>> subscriptions_;

        class OneRedisSender {
        private:
            redisContext *ctx_;
            std::mutex mutex_;
        public:
            OneRedisSender(ConnectionLocator const &locator)
                : ctx_(nullptr), mutex_()
            {
                ctx_ = redisConnect(locator.host().c_str(), locator.port());
            }
            void publish(basic::ByteDataWithTopic &&data) {
                std::lock_guard<std::mutex> _(mutex_);
                redisReply *r = (redisReply *) redisCommand(
                    ctx_
                    , "PUBLISH %s %b"
                    , data.topic.c_str()
                    , data.content.c_str()
                    , data.content.length()
                ); 
                if (r != nullptr) {
                    freeReplyObject((void *) r);
                }
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRedisSender>> senders_;

        std::mutex mutex_;

        OneRedisSubscription *getOrStartSubscription(ConnectionLocator const &d) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            std::lock_guard<std::mutex> _(mutex_);
            auto subscriptionIter = subscriptions_.find(hostAndPort);
            if (subscriptionIter == subscriptions_.end()) {
                subscriptionIter = subscriptions_.insert({hostAndPort, std::make_unique<OneRedisSubscription>(hostAndPort)}).first;
            }
            return subscriptionIter->second.get();
        }

        OneRedisSender *getOrStartSender(ConnectionLocator const &d) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            std::lock_guard<std::mutex> _(mutex_);
            auto senderIter = senders_.find(hostAndPort);
            if (senderIter == senders_.end()) {
                senderIter = senders_.insert({hostAndPort, std::make_unique<OneRedisSender>(hostAndPort)}).first;
            }
            return senderIter->second.get();
        }
    public:
        RedisComponentImpl() 
            : subscriptions_(), senders_(), mutex_() { 
        }
        ~RedisComponentImpl() = default;
        void addSubscriptionClient(ConnectionLocator const &locator,
            std::string const &topic,
            std::function<void(basic::ByteDataWithTopic &&)> client,
            std::optional<WireToUserHook> wireToUserHook) {
            auto *p = getOrStartSubscription(locator);
            p->addSubscription(topic, client, wireToUserHook);
        }
        std::function<void(basic::ByteDataWithTopic &&)> getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
            auto *p = getOrStartSender(locator);
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
    };

    RedisComponent::RedisComponent() : impl_(std::make_unique<RedisComponentImpl>()) {}
    RedisComponent::~RedisComponent() {}
    RedisComponent::RedisComponent(RedisComponent &&) = default;
    RedisComponent &RedisComponent::operator=(RedisComponent &&) = default;
    void RedisComponent::redis_addSubscriptionClient(ConnectionLocator const &locator,
        std::string const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        impl_->addSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    std::function<void(basic::ByteDataWithTopic &&)> RedisComponent::redis_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook);
    }

} } } } }
