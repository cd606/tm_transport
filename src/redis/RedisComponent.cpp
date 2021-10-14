#include <thread>
#include <mutex>
#include <atomic>
#include <cstring>
#include <sstream>
#include <unordered_map>

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
            ConnectionLocator locator_;
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

            void run() {
                struct redisReply *reply = nullptr;
                while (running_) {
                    int r = redisGetReply(ctx_, (void **) &reply);
                    if (!running_) {
                        break;
                    }
                    if (r != REDIS_OK) {
                        if (ctx_->err == REDIS_ERR_EOF) {
                            break;
                        }
                        if (reply != nullptr) {
                            freeReplyObject((void *) &reply);
                        }
                        continue;
                    }
                    if (reply == nullptr) {
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

                    if (!running_) {
                        break;
                    }
                    std::lock_guard<std::mutex> _(mutex_);
                    for (auto const &cb : clients_) {
                        callClient(cb, {topic, content});
                    }
                }
                redisFree(ctx_);
            }
        public:
            OneRedisSubscription(ConnectionLocator const &locator, std::string const &topic) 
                : locator_(locator)
                , ctx_(nullptr)
                , clients_()
                , th_()
                , mutex_()
                , running_(true)
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
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
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
            ConnectionLocator const &locator() const {
                return locator_;
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
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
            ~OneRedisSender() {
                redisFree(ctx_);
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
                while (running_) {
                    int r = redisGetReply(ctx_, (void **) &reply);
                    if (!running_) {
                        break;
                    }
                    if (r != REDIS_OK) {
                        if (ctx_->err == REDIS_ERR_EOF) {
                            break;
                        }
                        if (reply != nullptr) {
                            freeReplyObject((void *) &reply);
                        }
                        continue;
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
                redisFree(ctx_);
            }
        public:
            OneRedisRPCClientConnection(ConnectionLocator const &locator, std::string const &myCommunicationID, OneRedisSender *sender)
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
                    redisReply *r = (redisReply *) redisCommand(ctx_, "SUBSCRIBE %s", myCommunicationID_.c_str());
                    freeReplyObject((void *) r);
                    th_ = std::thread(&OneRedisRPCClientConnection::run, this);
                    th_.detach();
                }
            }
            ~OneRedisRPCClientConnection() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
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
                    int r = redisGetReply(ctx_, (void **) &reply);
                    if (!running_) {
                        break;
                    }
                    if (r != REDIS_OK) {
                        if (ctx_->err == REDIS_ERR_EOF) {
                            break;
                        }
                        if (reply != nullptr) {
                            freeReplyObject((void *) &reply);
                        }
                        continue;
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
                redisFree(ctx_);
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
                , running_(true)
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
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
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
        void potentiallyStopSubscription(OneRedisSubscription *p) {
            std::lock_guard<std::mutex> _(mutex_);
            if (p->checkWhetherNeedsToStop()) {
                subscriptions_.erase(p->locator());
            }
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
        OneRedisRPCClientConnection *createRpcClientConnection(ConnectionLocator const &l, std::function<std::string()> clientCommunicationIDCreator) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcClientConnections_.find(l);
            if (iter == rpcClientConnections_.end()) {
                iter = rpcClientConnections_.insert(
                    {l, std::make_unique<OneRedisRPCClientConnection>(l, clientCommunicationIDCreator(), getOrStartSenderNoLock(l))}
                ).first;
            }
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
            std::optional<WireToUserHook> wireToUserHook) {
            auto *p = getOrStartSubscription(locator, topic);
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
            std::function<void(bool, basic::ByteDataWithID &&)> client,
            std::optional<ByteDataHookPair> hookPair,
            uint32_t *clientNumberOutput) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcClientConnection(locator, clientCommunicationIDCreator);
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
                    rpcClientConnections_.erase(iter);
                }
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

    RedisComponent::RedisComponent() : impl_(std::make_unique<RedisComponentImpl>()) {}
    RedisComponent::~RedisComponent() {}
    RedisComponent::RedisComponent(RedisComponent &&) = default;
    RedisComponent &RedisComponent::operator=(RedisComponent &&) = default;
    uint32_t RedisComponent::redis_addSubscriptionClient(ConnectionLocator const &locator,
        std::string const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    void RedisComponent::redis_removeSubscriptionClient(uint32_t id) {
        impl_->removeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&)> RedisComponent::redis_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook);
    }
    std::function<void(basic::ByteDataWithID &&)> RedisComponent::redis_setRPCClient(ConnectionLocator const &locator,
                        std::function<std::string()> clientCommunicationIDCreator,
                        std::function<void(bool, basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair,
                        uint32_t *clientNumberOutput) {
        return impl_->setRPCClient(locator, clientCommunicationIDCreator, client, hookPair, clientNumberOutput);
    }
    void RedisComponent::redis_removeRPCClient(ConnectionLocator const &locator, uint32_t clientNumber) {
        impl_->removeRPCClient(locator, clientNumber);
    }
    std::function<void(bool, basic::ByteDataWithID &&)> RedisComponent::redis_setRPCServer(ConnectionLocator const &locator,
                    std::function<void(basic::ByteDataWithID &&)> server,
                    std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCServer(locator, server, hookPair);
    }
    std::unordered_map<ConnectionLocator, std::thread::native_handle_type> RedisComponent::redis_threadHandles() {
        return impl_->threadHandles();
    }

} } } } }
