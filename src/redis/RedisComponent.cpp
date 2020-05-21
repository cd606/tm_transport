#include <thread>
#include <mutex>
#include <atomic>
#include <cstring>
#include <sstream>
#include <unordered_map>
#include <boost/endian/conversion.hpp>

#include <tm_kit/transport/redis/RedisComponent.hpp>

#ifdef _MSC_VER
#include <winsock2.h>
#endif
#include <hiredis/hiredis.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace redis {
    class RedisComponentImpl {
    private:
        class OneRedisSubscription {
        private:
            redisContext *ctx_;
            struct ClientCB {
                std::function<void(basic::ByteDataWithTopic &&)> cb;
                std::optional<WireToUserHook> hook;
            };
            std::vector<ClientCB> clients_;
            std::thread th_;
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

            void run() {
                struct redisReply *reply = nullptr;
                while (true) {
                    int r = redisGetReply(ctx_, (void **) &reply);
                    if (r != REDIS_OK) {
                        if (reply != nullptr) {
                            freeReplyObject((void *) &reply);
                        }
                        continue;
                    }
                    if (reply->type != REDIS_REPLY_ARRAY || (reply->elements != 4 /*&& reply->elements != 3*/)) {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    if (reply->element[0]->type != REDIS_REPLY_STRING
                        ||
                        std::string(reply->element[0]->str, reply->element[0]->len) != "pmessage") {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    std::string topic(reply->element[2]->str, reply->element[2]->len);
                    std::string content(reply->element[3]->str, reply->element[3]->len);
                    freeReplyObject((void *) reply);

                    std::lock_guard<std::mutex> _(mutex_);
                    for (auto const &cb : clients_) {
                        callClient(cb, {topic, content});
                    }
                }
            }
        public:
            OneRedisSubscription(ConnectionLocator const &locator, std::string const &topic) 
                : ctx_(nullptr)
                , clients_()
                , th_()
                , mutex_()
            {
                ctx_ = redisConnect(locator.host().c_str(), locator.port());
                if (ctx_ != nullptr) {
                    redisReply *r = (redisReply *) redisCommand(ctx_, "PSUBSCRIBE %s", topic.c_str());
                    freeReplyObject((void *) r);
                    th_ = std::thread(&OneRedisSubscription::run, this);
                    th_.detach();
                }
            }
            ~OneRedisSubscription() {
            }
            void addSubscription(
                std::function<void(basic::ByteDataWithTopic &&)> handler
                , std::optional<WireToUserHook> wireToUserHook
            ) {
                std::lock_guard<std::mutex> _(mutex_);
                clients_.push_back({handler, wireToUserHook});
            }  
        };
        
        std::unordered_map<ConnectionLocator, std::unordered_map<std::string, std::unique_ptr<OneRedisSubscription>>> subscriptions_;

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

        class OneRedisRPCClientConnection {
        private:
            redisContext *ctx_;
            std::string rpcTopic_;
            std::string myCommunicationID_;
            std::function<void(basic::ByteDataWithID &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::thread th_;
            OneRedisSender *sender_;
            void run() {
                struct redisReply *reply = nullptr;
                while (true) {
                    int r = redisGetReply(ctx_, (void **) &reply);
                    if (r != REDIS_OK) {
                        if (reply != nullptr) {
                            freeReplyObject((void *) &reply);
                        }
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

                    size_t l = reply->element[2]->len;
                    char const *p = reply->element[2]->str;
                    if (l < sizeof(uint32_t)) {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    uint32_t idLen;
                    std::memcpy(&idLen, p, sizeof(uint32_t));
                    idLen = boost::endian::little_to_native<uint32_t>(idLen);
                    l -= sizeof(uint32_t);
                    p += sizeof(uint32_t);
                    if (l < idLen) {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    std::string id {p, p+idLen};
                    l -= idLen;
                    p += idLen;
                    std::string content {p, p+l};
                    freeReplyObject((void *) reply);

                    if (wireToUserHook_) {
                        auto d = (wireToUserHook_->hook)(basic::ByteData {std::move(content)});
                        if (d) {
                            callback_({std::move(id), std::move(d->content)});
                        }
                    } else {
                        callback_({std::move(id), std::move(content)});
                    }
                }
            }
        public:
            OneRedisRPCClientConnection(ConnectionLocator const &locator, std::string const &myCommunicationID, std::function<void(basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, OneRedisSender *sender)
                : ctx_(nullptr)
                , rpcTopic_(locator.identifier())
                , myCommunicationID_(myCommunicationID)
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , th_()
                , sender_(sender)
            {
                ctx_ = redisConnect(locator.host().c_str(), locator.port());
                if (ctx_ != nullptr) {
                    redisReply *r = (redisReply *) redisCommand(ctx_, "SUBSCRIBE %s", myCommunicationID_.c_str());
                    freeReplyObject((void *) r);
                    th_ = std::thread(&OneRedisRPCClientConnection::run, this);
                    th_.detach();
                }
            }
            ~OneRedisRPCClientConnection() {
            }
            void sendRequest(basic::ByteDataWithID &&data) {
                uint32_t topicLen = boost::endian::native_to_little<uint32_t>(myCommunicationID_.length());
                uint32_t idLen = boost::endian::native_to_little<uint32_t>(data.id.length());
                std::ostringstream oss;
                oss.write(reinterpret_cast<char const *>(&topicLen), sizeof(uint32_t));
                oss.write(myCommunicationID_.c_str(), topicLen);
                oss.write(reinterpret_cast<char const *>(&idLen), sizeof(uint32_t));
                oss.write(data.id.c_str(), idLen);
                oss.write(data.content.c_str(), data.content.length());
                sender_->publish(basic::ByteDataWithTopic {rpcTopic_, oss.str()});       
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
            void run() {
                struct redisReply *reply = nullptr;
                while (true) {
                    int r = redisGetReply(ctx_, (void **) &reply);
                    if (r != REDIS_OK) {
                        if (reply != nullptr) {
                            freeReplyObject((void *) &reply);
                        }
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

                    size_t l = reply->element[2]->len;
                    char const *p = reply->element[2]->str;
                    if (l < sizeof(uint32_t)) {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    uint32_t replyTopicLen;
                    std::memcpy(&replyTopicLen, p, sizeof(uint32_t));
                    replyTopicLen = boost::endian::little_to_native<uint32_t>(replyTopicLen);
                    l -= sizeof(uint32_t);
                    p += sizeof(uint32_t);
                    if (l < replyTopicLen) {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    std::string replyTopic {p, p+replyTopicLen};
                    l -= replyTopicLen;
                    p += replyTopicLen;
                    if (l < sizeof(uint32_t)) {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    uint32_t idLen;
                    std::memcpy(&idLen, p, sizeof(uint32_t));
                    idLen = boost::endian::little_to_native<uint32_t>(idLen);
                    l -= sizeof(uint32_t);
                    p += sizeof(uint32_t);
                    if (l < idLen) {
                        freeReplyObject((void *) reply);
                        continue;
                    }
                    std::string id {p, p+idLen};
                    l -= idLen;
                    p += idLen;
                    std::string content {p, p+l};
                    freeReplyObject((void *) reply);
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        replyTopicMap_[id] = replyTopic;
                    }
                    if (wireToUserHook_) {
                        auto d = (wireToUserHook_->hook)(basic::ByteData {std::move(content)});
                        if (d) {
                            callback_({std::move(id), std::move(d->content)});
                        }
                    } else {
                        callback_({std::move(id), std::move(content)});
                    }
                }
            }
        public:
            OneRedisRPCServerConnection(ConnectionLocator const &locator, std::function<void(basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, OneRedisSender *sender)
                : ctx_(nullptr)
                , rpcTopic_(locator.identifier())
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , th_()
                , mutex_()
                , sender_(sender)
            {
                ctx_ = redisConnect(locator.host().c_str(), locator.port());
                if (ctx_ != nullptr) {
                    redisReply *r = (redisReply *) redisCommand(ctx_, "SUBSCRIBE %s", rpcTopic_.c_str());
                    freeReplyObject((void *) r);
                    th_ = std::thread(&OneRedisRPCServerConnection::run, this);
                    th_.detach();
                }
            }
            ~OneRedisRPCServerConnection() {
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
                uint32_t idLen = boost::endian::native_to_little<uint32_t>(data.id.length());
                std::ostringstream oss;
                oss.write(reinterpret_cast<char const *>(&idLen), sizeof(uint32_t));
                oss.write(data.id.c_str(), idLen);
                oss.write(data.content.c_str(), data.content.length());
                sender_->publish(basic::ByteDataWithTopic {replyTopic, oss.str()});           
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRedisRPCServerConnection>> rpcServerConnections_;

        std::mutex mutex_;

        OneRedisSubscription *getOrStartSubscription(ConnectionLocator const &d, std::string const &topic) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            std::lock_guard<std::mutex> _(mutex_);
            auto subscriptionIter = subscriptions_.find(hostAndPort);
            if (subscriptionIter == subscriptions_.end()) {
                subscriptionIter = subscriptions_.insert({hostAndPort, std::unordered_map<std::string, std::unique_ptr<OneRedisSubscription>> {}}).first;
            }
            auto innerIter = subscriptionIter->second.find(topic);
            if (innerIter == subscriptionIter->second.end()) {
                innerIter = subscriptionIter->second.insert({topic, std::make_unique<OneRedisSubscription>(hostAndPort, topic)}).first;
            }
            return innerIter->second.get();
        }
        OneRedisSender *getOrStartSender(ConnectionLocator const &d) {
            std::lock_guard<std::mutex> _(mutex_);
            return getOrStartSenderNoLock(d);
        }
        OneRedisSender *getOrStartSenderNoLock(ConnectionLocator const &d) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            auto senderIter = senders_.find(hostAndPort);
            if (senderIter == senders_.end()) {
                senderIter = senders_.insert({hostAndPort, std::make_unique<OneRedisSender>(hostAndPort)}).first;
            }
            return senderIter->second.get();
        }
        OneRedisRPCClientConnection *createRpcClientConnection(ConnectionLocator const &l, std::function<std::string()> clientCommunicationIDCreator, std::function<void(basic::ByteDataWithID &&)> client, std::optional<WireToUserHook> wireToUserHook) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcClientConnections_.find(l);
            if (iter != rpcClientConnections_.end()) {
                throw RedisComponentException("Cannot create duplicate RPC client connection for "+l.toSerializationFormat());
            }
            iter = rpcClientConnections_.insert(
                {l, std::make_unique<OneRedisRPCClientConnection>(l, clientCommunicationIDCreator(), client, wireToUserHook, getOrStartSenderNoLock(l))}
            ).first;
            return iter->second.get();
        }
        OneRedisRPCServerConnection *createRpcServerConnection(ConnectionLocator const &l, std::function<void(basic::ByteDataWithID &&)> handler, std::optional<WireToUserHook> wireToUserHook) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcServerConnections_.find(l);
            if (iter != rpcServerConnections_.end()) {
                throw RedisComponentException("Cannot create duplicate RPC server connection for "+l.toSerializationFormat());
            }
            iter = rpcServerConnections_.insert(
                {l, std::make_unique<OneRedisRPCServerConnection>(l, handler, wireToUserHook, getOrStartSenderNoLock(l))}
            ).first;
            return iter->second.get();
        }
    public:
        RedisComponentImpl() 
            : subscriptions_(), senders_(), rpcClientConnections_(), rpcServerConnections_(), mutex_() { 
        }
        ~RedisComponentImpl() = default;
        void addSubscriptionClient(ConnectionLocator const &locator,
            std::string const &topic,
            std::function<void(basic::ByteDataWithTopic &&)> client,
            std::optional<WireToUserHook> wireToUserHook) {
            auto *p = getOrStartSubscription(locator, topic);
            p->addSubscription(client, wireToUserHook);
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
        std::function<void(basic::ByteDataWithID &&)> setRPCClient(ConnectionLocator const &locator,
            std::function<std::string()> clientCommunicationIDCreator,
            std::function<void(basic::ByteDataWithID &&)> client,
            std::optional<ByteDataHookPair> hookPair) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcClientConnection(locator, clientCommunicationIDCreator, client, wireToUserHook);
            if (hookPair) {
                auto hook = hookPair->userToWire.hook;
                return [conn,hook](basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->sendRequest({data.id, std::move(x.content)});
                };
            } else {
                return [conn](basic::ByteDataWithID &&data) {
                    conn->sendRequest(std::move(data));
                };
            }
        }
        std::function<void(bool, basic::ByteDataWithID &&)> setRPCServer(ConnectionLocator const &locator,
            std::function<void(basic::ByteDataWithID &&)> server,
            std::optional<ByteDataHookPair> hookPair) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcServerConnection(locator, server, wireToUserHook);
            if (hookPair) {
                auto hook = hookPair->userToWire.hook;
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
    std::function<void(basic::ByteDataWithID &&)> RedisComponent::redis_setRPCClient(ConnectionLocator const &locator,
                        std::function<std::string()> clientCommunicationIDCreator,
                        std::function<void(basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCClient(locator, clientCommunicationIDCreator, client, hookPair);
    }
    std::function<void(bool, basic::ByteDataWithID &&)> RedisComponent::redis_setRPCServer(ConnectionLocator const &locator,
                    std::function<void(basic::ByteDataWithID &&)> server,
                    std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCServer(locator, server, hookPair);
    }

} } } } }