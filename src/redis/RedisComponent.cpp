#include <thread>
#include <mutex>
#include <atomic>
#include <cstring>
#include <sstream>
#include <unordered_map>

#include <fnmatch.h>

#include <redox.hpp>

#include <tm_kit/transport/redis/RedisComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace redis {
    class RedisComponentImpl {
    private:
        class OneRedisSubscription {
        private:
            redox::Subscriber sub_;
            struct ClientCB {
                std::function<void(basic::ByteDataWithTopic &&)> cb;
                std::optional<WireToUserHook> hook;
            };
            std::unordered_map<std::string, std::vector<ClientCB>> clients_;
            std::mutex mutex_;

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

            inline void handleData(std::string const &topic, std::string const &content) {
                std::lock_guard<std::mutex> _(mutex_);
                for (auto const &item : clients_) {
                    if (fnmatch(item.first.c_str(), topic.c_str(), 0) == 0) {
                        for (auto const &cb : item.second) {
                            callClient(cb, {topic, content});
                        }
                    }
                }
            }

        public:
            OneRedisSubscription(ConnectionLocator const &locator) 
                : sub_()
                , clients_()
                , mutex_()
            {
                sub_.connect(locator.host(), locator.port());
            }
            ~OneRedisSubscription() {
                std::lock_guard<std::mutex> _(mutex_);
                for (auto const &item : clients_) {
                    sub_.punsubscribe(item.first);
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
                    sub_.psubscribe(topic, [this](std::string const &t, std::string const &c) {
                        handleData(t, c);
                    });
                } 
            }  
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRedisSubscription>> subscriptions_;
        
        class OneRedisSender {
        private:
            redox::Redox client_;
        public:
            OneRedisSender(ConnectionLocator const &locator)
                : client_()
            {
                client_.connect(locator.host(), locator.port());
            }
            void publish(basic::ByteDataWithTopic &&data) {
                client_.publish(data.topic, data.content);
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