#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>

#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <vector>
#include <unordered_map>

#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace rabbitmq {
    
    class RabbitMQComponentImpl {
    private:
        static AmqpClient::Channel::ptr_t createChannel(ConnectionLocator const &l) {
            std::string useSSL = boost::to_lower_copy(boost::trim_copy(l.query("ssl", "false")));
            if (useSSL == "true" || useSSL == "yes") {
                return AmqpClient::Channel::CreateSecure(
                    l.query("ca_cert", ""), l.host(), l.query("client_key", ""), l.query("client_cert", ""),
                    (l.port()==0?5671:l.port()), l.userName(), l.password(), l.query("vhost", "/")
                );
            } else {
                return AmqpClient::Channel::Create(
                    l.host(), (l.port()==0?5672:l.port()), l.userName(), l.password(), l.query("vhost", "/")
                );                
            }
        }
        class OneExchangeSubscriptionConnection {
        private:
            AmqpClient::Channel::ptr_t channel_;
            std::function<void(std::string const &, basic::ByteDataWithTopic &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::thread th_;
            std::atomic<bool> running_;
            void run(std::string const &tag) {
                while (running_) {
                    AmqpClient::Envelope::ptr_t msg;
                    if (channel_->BasicConsumeMessage(tag, msg, 1000)) {
                        if (wireToUserHook_) {
                            auto b = (wireToUserHook_->hook)(basic::ByteData {msg->Message()->Body()});
                            if (b) {
                                callback_(msg->Message()->ContentEncoding(), {
                                    msg->RoutingKey()
                                    , std::move(b->content)
                                });
                            }
                        } else {
                            callback_(msg->Message()->ContentEncoding(), {
                                msg->RoutingKey()
                                , msg->Message()->Body()
                            });
                        }
                    }
                }
            }
        public:
            OneExchangeSubscriptionConnection(ConnectionLocator const &l, std::string const &topic, std::function<void(std::string const &, basic::ByteDataWithTopic &&)> callback, std::optional<WireToUserHook> wireToUserHook)
                : channel_(createChannel(l))
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , th_()
                , running_(true)
            {
                channel_->DeclareExchange(
                    l.identifier()

#ifdef _MSC_VER
                    , "topic"
#else
                    , AmqpClient::Channel::EXCHANGE_TYPE_TOPIC
#endif
                    , (l.query("passive", "false") == "true")
                    , (l.query("durable", "false") == "true")
                    , (l.query("auto_delete", "false") == "true")
                );
                std::string queueName = channel_->DeclareQueue("");
                channel_->BindQueue(
                    queueName
                    , l.identifier()
                    , topic
                );
                channel_->BasicConsume(
                    queueName
                    , queueName
                );
                th_ = std::thread(&OneExchangeSubscriptionConnection::run, this, queueName);
            }
            ~OneExchangeSubscriptionConnection() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
            }
        };
        std::unordered_map<uint32_t, std::unique_ptr<OneExchangeSubscriptionConnection>> exchangeSubscriptionConnections_;

        class OnePublishingConnection {
        private:
            struct PublishingData {
                std::string exchange;
                std::string routingKey;
                AmqpClient::BasicMessage::ptr_t message;
            };
            AmqpClient::Channel::ptr_t channel_;
            std::mutex mutex_;
            std::condition_variable cond_;
            std::list<PublishingData> incoming_, processing_;
            std::thread th_;
            std::atomic<bool> running_;
            void run() {
                while (running_) {
                    {
                        std::unique_lock<std::mutex> lock(mutex_);
                        cond_.wait_for(lock, std::chrono::milliseconds(1));
                        //cond_.wait(lock);
                        if (incoming_.empty()) {
                            lock.unlock();
                            continue;
                        }
                        processing_.splice(processing_.end(), incoming_);
                        lock.unlock();
                    }
                    while (!processing_.empty()) {
                        auto const &item = processing_.front();
                        channel_->BasicPublish(
                            item.exchange
                            , item.routingKey
                            , item.message
                        );
                        processing_.pop_front();
                    }
                }
            }
        public:
            OnePublishingConnection(ConnectionLocator const &l) 
                : channel_(createChannel(l))
                , mutex_()
                , cond_()
                , incoming_()
                , processing_()
                , th_()
                , running_(true)
            {
                th_ = std::thread(&OnePublishingConnection::run, this);
            }
            ~OnePublishingConnection() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
            }
            void publishOnExchange(std::string const &exchange, std::string const &contentEncoding, basic::ByteDataWithTopic &&data) {
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    auto msg = AmqpClient::BasicMessage::Create(data.content);
                    msg->ContentEncoding(contentEncoding);
                    msg->DeliveryMode(AmqpClient::BasicMessage::dm_nonpersistent);
                    msg->Expiration("1000");
                    incoming_.push_back(PublishingData {exchange, data.topic, msg});
                }
                cond_.notify_one();
            }
            void publishOnQueue(std::string const &queue, AmqpClient::BasicMessage::ptr_t message) {
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    incoming_.push_back(PublishingData {"", queue, message});
                }
                cond_.notify_one();
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OnePublishingConnection>> publishingConnections_;
        
        class OneRPCQueueClientConnection {
        private:
            AmqpClient::Channel::ptr_t channel_;
            std::string rpcQueue_, localQueue_;
            std::function<void(std::string const &, basic::ByteDataWithID &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::thread th_;
            std::atomic<bool> running_;
            OnePublishingConnection *publishing_;
            bool persistent_;
            void run() {
                while (running_) {
                    AmqpClient::Envelope::ptr_t msg;
                    if (channel_->BasicConsumeMessage(localQueue_, msg, 1000)) {
                        std::string corrID = msg->Message()->CorrelationId();
                        if (wireToUserHook_) {
                            auto d = (wireToUserHook_->hook)(basic::ByteData {msg->Message()->Body()});
                            if (d) {
                                callback_(msg->Message()->ContentEncoding(), {corrID, std::move(d->content)});
                            }
                        } else {
                            callback_(msg->Message()->ContentEncoding(), {corrID, msg->Message()->Body()});
                        }
                    }
                }
            }
        public:
            OneRPCQueueClientConnection(ConnectionLocator const &l, std::function<void(std::string const &, basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, OnePublishingConnection *publishing)
                : channel_(createChannel(l))
                , rpcQueue_(l.identifier())
                , localQueue_(channel_->DeclareQueue(""))
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , th_()
                , running_(true)
                , publishing_(publishing)
                , persistent_(l.query("persistent", "false") == "true")
            {
                channel_->BasicConsume(
                    localQueue_
                    , localQueue_
                );
                th_ = std::thread(&OneRPCQueueClientConnection::run, this);
            }
            ~OneRPCQueueClientConnection() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
            }
            void sendRequest(std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                AmqpClient::BasicMessage::ptr_t msg = AmqpClient::BasicMessage::Create(std::move(data.content));
                msg->CorrelationId(data.id);
                msg->ReplyTo(localQueue_);
                msg->ContentEncoding(contentEncoding);
                msg->DeliveryMode(persistent_?AmqpClient::BasicMessage::dm_persistent:AmqpClient::BasicMessage::dm_nonpersistent);
                msg->Expiration("5000");
                publishing_->publishOnQueue(rpcQueue_, msg);           
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRPCQueueClientConnection>> rpcQueueClientConnections_;
        
        class OneRPCQueueServerConnection {
        private:
            AmqpClient::Channel::ptr_t channel_;
            std::string rpcQueue_;
            std::function<void(std::string const &, basic::ByteDataWithID &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::unordered_map<std::string, std::string> replyQueueMap_;
            std::thread th_;
            std::mutex mutex_;
            std::atomic<bool> running_;
            OnePublishingConnection *publishing_;
            void run() {
                while (running_) {
                    AmqpClient::Envelope::ptr_t msg;
                    if (channel_->BasicConsumeMessage(rpcQueue_, msg, 1000)) {
                        channel_->BasicAck(msg);
                        std::string corrID = msg->Message()->CorrelationId();
                        {
                            std::lock_guard<std::mutex> _(mutex_);
                            replyQueueMap_[corrID] = msg->Message()->ReplyTo();
                        }
                        if (wireToUserHook_) {
                            auto d = (wireToUserHook_->hook)(basic::ByteData {msg->Message()->Body()});
                            if (d) {
                                callback_(msg->Message()->ContentEncoding(), {corrID, std::move(d->content)});
                            }
                        } else {
                            callback_(msg->Message()->ContentEncoding(), {corrID, msg->Message()->Body()});
                        }
                    }                   
                }
            }
        public:
            OneRPCQueueServerConnection(ConnectionLocator const &l, std::function<void(std::string const &, basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, OnePublishingConnection *publishing)
                : channel_(createChannel(l))
                , rpcQueue_(l.identifier())
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , replyQueueMap_()
                , th_()
                , mutex_()
                , running_(true)
                , publishing_(publishing)
            {
                channel_->DeclareQueue(
                    rpcQueue_
                    , false //passive
                    , false //durable
                    , false //exclusive
                    , false //auto_delete
                );
                channel_->BasicConsume(
                    rpcQueue_
                    , rpcQueue_
                    , true //no_local
                    , false //no_ack
                );                 
                channel_->BasicQos(rpcQueue_, 0);
                th_ = std::thread(&OneRPCQueueServerConnection::run, this);
            }
            ~OneRPCQueueServerConnection() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
            }
            void sendReply(bool isFinal, std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                std::string replyQueue;
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    auto iter = replyQueueMap_.find(data.id);
                    if (iter == replyQueueMap_.end()) {
                        return;
                    }
                    replyQueue = iter->second;
                    if (isFinal) {
                        replyQueueMap_.erase(iter);
                    }
                }
                AmqpClient::BasicMessage::ptr_t msg = AmqpClient::BasicMessage::Create(std::move(data.content));
                msg->CorrelationId(data.id);
                msg->ReplyTo(rpcQueue_);
                msg->ContentEncoding(contentEncoding);
                publishing_->publishOnQueue(replyQueue, msg);            
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRPCQueueServerConnection>> rpcQueueServerConnections_;

        std::mutex mutex_;
        uint32_t counter_;

        OnePublishingConnection *publishingConnection(ConnectionLocator const &l) {
            std::lock_guard<std::mutex> _(mutex_);
            return publishingConnectionNoLock(l);
        }
        OnePublishingConnection *publishingConnectionNoLock(ConnectionLocator const &l) {
            auto basicPortion = l.copyOfBasicPortionWithProperties();
            auto iter = publishingConnections_.find(basicPortion);
            if (iter == publishingConnections_.end()) {
                iter = publishingConnections_.insert(
                    {basicPortion, std::make_unique<OnePublishingConnection>(basicPortion)}
                ).first;
            }
            return iter->second.get();
        }
        OneRPCQueueClientConnection *createRpcQueueClientConnection(ConnectionLocator const &l, std::function<void(std::string const &, basic::ByteDataWithID &&)> client, std::optional<WireToUserHook> wireToUserHook) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcQueueClientConnections_.find(l);
            if (iter != rpcQueueClientConnections_.end()) {
                throw RabbitMQComponentException("Cannot create duplicate RPC Queue client connection for "+l.toSerializationFormat());
            }
            iter = rpcQueueClientConnections_.insert(
                {l, std::make_unique<OneRPCQueueClientConnection>(l, client, wireToUserHook, publishingConnectionNoLock(l))}
            ).first;
            return iter->second.get();
        }
        OneRPCQueueServerConnection *createRpcQueueServerConnection(ConnectionLocator const &l, std::function<void(std::string const &, basic::ByteDataWithID &&)> handler, std::optional<WireToUserHook> wireToUserHook) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcQueueServerConnections_.find(l);
            if (iter != rpcQueueServerConnections_.end()) {
                throw RabbitMQComponentException("Cannot create duplicate RPC Queue server connection for "+l.toSerializationFormat());
            }
            iter = rpcQueueServerConnections_.insert(
                {l, std::make_unique<OneRPCQueueServerConnection>(l, handler, wireToUserHook, publishingConnectionNoLock(l))}
            ).first;
            return iter->second.get();
        }
    public:
        RabbitMQComponentImpl() = default;
        ~RabbitMQComponentImpl() = default;
        uint32_t addExchangeSubscriptionClient(ConnectionLocator const &locator,
            std::string const &topic,
            std::function<void(std::string const &, basic::ByteDataWithTopic &&)> client,
            std::optional<WireToUserHook> wireToUserHook) {
            std::lock_guard<std::mutex> _(mutex_);
            exchangeSubscriptionConnections_.insert(
                std::make_pair(
                    ++counter_
                    , std::make_unique<OneExchangeSubscriptionConnection>(
                        locator, topic, client, wireToUserHook
                    )
                )
            );
            return counter_;
        }
        void removeExchangeSubscriptionClient(uint32_t id) {
            std::lock_guard<std::mutex> _(mutex_);
            exchangeSubscriptionConnections_.erase(id);
        }
        std::function<void(std::string const &, basic::ByteDataWithTopic &&)> getExchangePublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
            auto *conn = publishingConnection(locator);
            auto id = locator.identifier();
            if (userToWireHook) {
                auto hook = userToWireHook->hook;
                return [id,conn,hook](std::string const &contentEncoding, basic::ByteDataWithTopic &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->publishOnExchange(id, contentEncoding, {std::move(data.topic), std::move(x.content)});
                };
            } else {
                return [id,conn](std::string const &contentEncoding, basic::ByteDataWithTopic &&data) {
                    conn->publishOnExchange(id, contentEncoding, std::move(data));
                };
            }
            
        }
        std::function<void(std::string const &, basic::ByteDataWithID &&)> setRPCQueueClient(ConnectionLocator const &locator,
            std::function<void(std::string const &, basic::ByteDataWithID &&)> client,
            std::optional<ByteDataHookPair> hookPair) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcQueueClientConnection(locator, client, wireToUserHook);
            if (hookPair) {
                auto hook = hookPair->userToWire.hook;
                return [conn,hook](std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->sendRequest(contentEncoding, {data.id, std::move(x.content)});
                };
            } else {
                return [conn](std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                    conn->sendRequest(contentEncoding, std::move(data));
                };
            }
        }
        void removeRPCQueueClient(ConnectionLocator const &locator) {
            std::lock_guard<std::mutex> _(mutex_);
            rpcQueueClientConnections_.erase(locator);
        }
        std::function<void(bool, std::string const &, basic::ByteDataWithID &&)> setRPCQueueServer(ConnectionLocator const &locator,
            std::function<void(std::string const &, basic::ByteDataWithID &&)> server,
            std::optional<ByteDataHookPair> hookPair) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcQueueServerConnection(locator, server, wireToUserHook);
            if (hookPair) {
                auto hook = hookPair->userToWire.hook;
                return [conn,hook](bool isFinal, std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->sendReply(isFinal, contentEncoding, {data.id, std::move(x.content)});
                };
            } else {
                return [conn](bool isFinal, std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                    conn->sendReply(isFinal, contentEncoding, std::move(data));
                };
            }
        }
    };

    RabbitMQComponent::RabbitMQComponent() : impl_(std::make_unique<RabbitMQComponentImpl>()) {}
    RabbitMQComponent::~RabbitMQComponent() {}

    uint32_t RabbitMQComponent::rabbitmq_addExchangeSubscriptionClient(ConnectionLocator const &locator,
        std::string const &topic,
        std::function<void(std::string const &, basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addExchangeSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    void RabbitMQComponent::rabbitmq_removeExchangeSubscriptionClient(uint32_t id) {
        impl_->removeExchangeSubscriptionClient(id);
    }
    std::function<void(std::string const &, basic::ByteDataWithTopic &&)> RabbitMQComponent::rabbitmq_getExchangePublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getExchangePublisher(locator, userToWireHook);
    }
    std::function<void(std::string const &, basic::ByteDataWithID &&)> RabbitMQComponent::rabbitmq_setRPCQueueClient(ConnectionLocator const &locator,
        std::function<void(std::string const &, basic::ByteDataWithID &&)> client,
        std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCQueueClient(locator, client, hookPair);
    }
    void RabbitMQComponent::rabbitmq_removeRPCQueueClient(ConnectionLocator const &locator) {
        impl_->removeRPCQueueClient(locator);
    }
    std::function<void(bool, std::string const &, basic::ByteDataWithID &&)> RabbitMQComponent::rabbitmq_setRPCQueueServer(ConnectionLocator const &locator,
        std::function<void(std::string const &, basic::ByteDataWithID &&)> server,
        std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCQueueServer(locator, server, hookPair);
    }

} } } } }