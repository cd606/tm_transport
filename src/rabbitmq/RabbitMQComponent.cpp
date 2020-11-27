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
#if (SIMPLEAMQPCLIENT_VERSION_MAJOR >= 3) || (SIMPLEAMQPCLIENT_VERSION_MAJOR == 2 && SIMPLEAMQPCLIENT_VERSION_MINOR >= 6)
                AmqpClient::Channel::OpenOpts opts;
                opts.host = l.host();
                opts.vhost = l.query("vhost", "/");
                opts.port = (l.port()==0?5671:l.port());
                opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth {
                    l.userName()
                    , l.password()
                };
                AmqpClient::Channel::OpenOpts::TLSParams tlsParams;
                tlsParams.client_key_path = l.query("client_key", "");
                tlsParams.client_cert_path = l.query("client_cert", "");
                tlsParams.ca_cert_path = l.query("ca_cert", "");
                opts.tls_params = tlsParams;
                return AmqpClient::Channel::Open(opts);
#else
                return AmqpClient::Channel::CreateSecure(
                    l.query("ca_cert", ""), l.host(), l.query("client_key", ""), l.query("client_cert", ""),
                    (l.port()==0?5671:l.port()), l.userName(), l.password(), l.query("vhost", "/")
                );
#endif
            } else {
#if (SIMPLEAMQPCLIENT_VERSION_MAJOR >= 3) || (SIMPLEAMQPCLIENT_VERSION_MAJOR == 2 && SIMPLEAMQPCLIENT_VERSION_MINOR >= 6)
                AmqpClient::Channel::OpenOpts opts;
                opts.host = l.host();
                opts.vhost = l.query("vhost", "/");
                opts.port = (l.port()==0?5672:l.port());
                opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth {
                    l.userName()
                    , l.password()
                };
                return AmqpClient::Channel::Open(opts);
#else
                return AmqpClient::Channel::Create(
                    l.host(), (l.port()==0?5672:l.port()), l.userName(), l.password(), l.query("vhost", "/")
                );  
#endif              
            }
        }
        class OneExchangeSubscriptionConnection {
        private:
            AmqpClient::Channel::ptr_t channel_;
            std::function<void(basic::ByteDataWithTopic &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::thread th_;
            std::atomic<bool> running_;
            void run(std::string const &tag) {
                while (running_) {
                    AmqpClient::Envelope::ptr_t msg;
                    if (channel_->BasicConsumeMessage(tag, msg, 1000)) {
                        if (wireToUserHook_) {
                            auto b = (wireToUserHook_->hook)(basic::ByteDataView {std::string_view(msg->Message()->Body())});
                            if (b) {
                                callback_({ 
                                    msg->RoutingKey()
                                    , std::move(b->content)
                                });
                            }
                        } else {
                            callback_({
                                msg->RoutingKey()
                                , msg->Message()->Body()
                            });
                        }
                    }
                }
            }
        public:
            OneExchangeSubscriptionConnection(ConnectionLocator const &l, std::string const &topic, std::function<void(basic::ByteDataWithTopic &&)> callback, std::optional<WireToUserHook> wireToUserHook)
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
            AmqpClient::Channel::ptr_t channel_;
            std::mutex mutex_;
        public:
            OnePublishingConnection(ConnectionLocator const &l) 
                : channel_(createChannel(l))
                , mutex_()
            {
            }
            ~OnePublishingConnection() {
            }
            void publishOnExchange(std::string const &exchange, basic::ByteDataWithTopic &&data) {
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    auto msg = AmqpClient::BasicMessage::Create(data.content);
                    msg->DeliveryMode(AmqpClient::BasicMessage::dm_nonpersistent);
#if !((SIMPLEAMQPCLIENT_VERSION_MAJOR >= 3) || (SIMPLEAMQPCLIENT_VERSION_MAJOR == 2 && SIMPLEAMQPCLIENT_VERSION_MINOR >= 6))
                    msg->Expiration("1000");
#endif                    
                    channel_->BasicPublish(
                        exchange
                        , data.topic
                        , msg
                    );
                }
            }
            void publishOnQueue(std::string const &queue, AmqpClient::BasicMessage::ptr_t message) {
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    channel_->BasicPublish(
                        ""
                        , queue
                        , message
                    );
                }
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OnePublishingConnection>> publishingConnections_;
        
        class OneRPCQueueClientConnection {
        private:
            AmqpClient::Channel::ptr_t channel_;
            std::string rpcQueue_, localQueue_;
            std::function<void(bool, basic::ByteDataWithID &&)> callback_;
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
                        bool isFinal = (msg->Message()->ContentTypeIsSet() && msg->Message()->ContentType() == "final");
                        if (wireToUserHook_) {
                            auto d = (wireToUserHook_->hook)(basic::ByteDataView {std::string_view(msg->Message()->Body())});
                            if (d) {
                                callback_(isFinal, {corrID, std::move(d->content)});
                            }
                        } else {
                            callback_(isFinal, {corrID, msg->Message()->Body()});
                        }
                    }
                }
            }
        public:
            OneRPCQueueClientConnection(ConnectionLocator const &l, std::function<void(bool, basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, OnePublishingConnection *publishing)
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
            void sendRequest(basic::ByteDataWithID &&data) {
                AmqpClient::BasicMessage::ptr_t msg = AmqpClient::BasicMessage::Create(std::move(data.content));
                msg->CorrelationId(data.id);
                msg->ReplyTo(localQueue_);
                msg->DeliveryMode(persistent_?AmqpClient::BasicMessage::dm_persistent:AmqpClient::BasicMessage::dm_nonpersistent);
#if !((SIMPLEAMQPCLIENT_VERSION_MAJOR >= 3) || (SIMPLEAMQPCLIENT_VERSION_MAJOR == 2 && SIMPLEAMQPCLIENT_VERSION_MINOR >= 6))                
                msg->Expiration("5000");
#endif                
                publishing_->publishOnQueue(rpcQueue_, msg);           
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRPCQueueClientConnection>> rpcQueueClientConnections_;
        
        class OneRPCQueueServerConnection {
        private:
            AmqpClient::Channel::ptr_t channel_;
            std::string rpcQueue_;
            std::function<void(basic::ByteDataWithID &&)> callback_;
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
                            auto d = (wireToUserHook_->hook)(basic::ByteDataView {std::string_view(msg->Message()->Body())});
                            if (d) {
                                callback_({corrID, std::move(d->content)});
                            }
                        } else {
                            callback_({corrID, msg->Message()->Body()});
                        }
                    }                   
                }
            }
        public:
            OneRPCQueueServerConnection(ConnectionLocator const &l, std::function<void(basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, OnePublishingConnection *publishing)
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
            void sendReply(bool isFinal, basic::ByteDataWithID &&data) {
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
                if (isFinal) {
                    msg->ContentType("final");
                }
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
        OneRPCQueueClientConnection *createRpcQueueClientConnection(ConnectionLocator const &l, std::function<void(bool, basic::ByteDataWithID &&)> client, std::optional<WireToUserHook> wireToUserHook) {
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
        OneRPCQueueServerConnection *createRpcQueueServerConnection(ConnectionLocator const &l, std::function<void(basic::ByteDataWithID &&)> handler, std::optional<WireToUserHook> wireToUserHook) {
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
        ~RabbitMQComponentImpl() {
            std::lock_guard<std::mutex> _(mutex_);
            exchangeSubscriptionConnections_.clear();
            publishingConnections_.clear();
            rpcQueueClientConnections_.clear();
            rpcQueueServerConnections_.clear();
        }
        uint32_t addExchangeSubscriptionClient(ConnectionLocator const &locator,
            std::string const &topic,
            std::function<void(basic::ByteDataWithTopic &&)> client,
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
        std::function<void(basic::ByteDataWithTopic &&)> getExchangePublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
            auto *conn = publishingConnection(locator);
            auto id = locator.identifier();
            if (userToWireHook) {
                auto hook = userToWireHook->hook;
                return [id,conn,hook](basic::ByteDataWithTopic &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->publishOnExchange(id, {std::move(data.topic), std::move(x.content)});
                };
            } else {
                return [id,conn](basic::ByteDataWithTopic &&data) {
                    conn->publishOnExchange(id, std::move(data));
                };
            }
            
        }
        std::function<void(basic::ByteDataWithID &&)> setRPCQueueClient(ConnectionLocator const &locator,
            std::function<void(bool, basic::ByteDataWithID &&)> client,
            std::optional<ByteDataHookPair> hookPair) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcQueueClientConnection(locator, client, wireToUserHook);
            if (hookPair && hookPair->userToWire) {
                auto hook = hookPair->userToWire->hook;
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
        void removeRPCQueueClient(ConnectionLocator const &locator) {
            std::lock_guard<std::mutex> _(mutex_);
            rpcQueueClientConnections_.erase(locator);
        }
        std::function<void(bool, basic::ByteDataWithID &&)> setRPCQueueServer(ConnectionLocator const &locator,
            std::function<void(basic::ByteDataWithID &&)> server,
            std::optional<ByteDataHookPair> hookPair) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcQueueServerConnection(locator, server, wireToUserHook);
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
    };

    RabbitMQComponent::RabbitMQComponent() : impl_(std::make_unique<RabbitMQComponentImpl>()) {}
    RabbitMQComponent::~RabbitMQComponent() = default;

    uint32_t RabbitMQComponent::rabbitmq_addExchangeSubscriptionClient(ConnectionLocator const &locator,
        std::string const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addExchangeSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    void RabbitMQComponent::rabbitmq_removeExchangeSubscriptionClient(uint32_t id) {
        impl_->removeExchangeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&)> RabbitMQComponent::rabbitmq_getExchangePublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getExchangePublisher(locator, userToWireHook);
    }
    std::function<void(basic::ByteDataWithID &&)> RabbitMQComponent::rabbitmq_setRPCQueueClient(ConnectionLocator const &locator,
        std::function<void(bool, basic::ByteDataWithID &&)> client,
        std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCQueueClient(locator, client, hookPair);
    }
    void RabbitMQComponent::rabbitmq_removeRPCQueueClient(ConnectionLocator const &locator) {
        impl_->removeRPCQueueClient(locator);
    }
    std::function<void(bool, basic::ByteDataWithID &&)> RabbitMQComponent::rabbitmq_setRPCQueueServer(ConnectionLocator const &locator,
        std::function<void(basic::ByteDataWithID &&)> server,
        std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCQueueServer(locator, server, hookPair);
    }

} } } } }