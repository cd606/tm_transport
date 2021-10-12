#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>
#include <tm_kit/transport/TLSConfigurationComponent.hpp>

#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <vector>
#include <unordered_map>

#ifdef _MSC_VER
#include <winsock.h>
#else
#include <sys/time.h>
#endif

#if defined(__has_include)
    #if __has_include(<rabbitmq-c/amqp.h>)
        #include <rabbitmq-c/amqp.h>
        #include <rabbitmq-c/tcp_socket.h>
        #include <rabbitmq-c/ssl_socket.h>
    #else
        #include <amqp.h>
        #include <amqp_tcp_socket.h>
        #include <amqp_ssl_socket.h>
    #endif
#else
    #include <amqp.h>
    #include <amqp_tcp_socket.h>
    #include <amqp_ssl_socket.h>
#endif

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace rabbitmq {
    
    class RabbitMQComponentImpl {
    private:
        static amqp_connection_state_t createConnection(ConnectionLocator const &l, TLSClientConfigurationComponent const *config) {
            auto conn = amqp_new_connection();
            amqp_socket_t *socket = nullptr;
            int port = 0;

            auto sslInfo = config?(config->getConfigurationItem(TLSClientInfoKey {
                l.host(), l.port()
            })):std::nullopt;
            if (sslInfo) {
                socket = amqp_ssl_socket_new(conn);
                if (!socket) {
                    throw RabbitMQComponentException("Cannot open RabbitMQ socket to "+l.toPrintFormat());
                }
                amqp_ssl_socket_set_verify_peer(socket, 0);
                amqp_ssl_socket_set_verify_hostname(socket, 0);
                auto caCert = sslInfo->caCertificateFile;
                if (caCert != "") {
                    if (amqp_ssl_socket_set_cacert(socket, caCert.c_str()) != AMQP_STATUS_OK) {
                        throw RabbitMQComponentException("Cannot set RabbitMQ CA Cert for "+l.toPrintFormat());
                    }
                }
                auto clientCert = sslInfo->clientCertificateFile;
                auto clientKey = sslInfo->clientKeyFile;
                if (clientCert != "" && clientKey != "") {
                    if (amqp_ssl_socket_set_key(socket, clientCert.c_str(), clientKey.c_str()) != AMQP_STATUS_OK) {
                        throw RabbitMQComponentException("Cannot set RabbitMQ client key for "+l.toPrintFormat());
                    }
                }
                port = (l.port()==0?5671:l.port());
            } else {
                socket = amqp_tcp_socket_new(conn);
                if (!socket) {
                    throw RabbitMQComponentException("Cannot open RabbitMQ socket to "+l.toPrintFormat());
                }
                port = (l.port()==0?5672:l.port());
            }

            struct timeval tv {1, 0};
            auto status = amqp_socket_open_noblock(socket, l.host().c_str(), port, &tv);
            if (status != AMQP_STATUS_OK) {
                throw RabbitMQComponentException("Cannot connect RabbitMQ socket to "+l.toPrintFormat());
            }
            if (amqp_login(
                conn 
                , l.query("vhost", "/").c_str()
                , 0
                , 131072
                , 0
                , AMQP_SASL_METHOD_PLAIN
                , l.userName().c_str()
                , l.password().c_str()
            ).reply_type != AMQP_RESPONSE_NORMAL) {
                throw RabbitMQComponentException("Cannot log into RabbitMQ "+l.toPrintFormat());
            }
            amqp_channel_open(conn, 1);
            if (amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
                throw RabbitMQComponentException("Cannot open RabbitMQ channel for "+l.toPrintFormat());
            }
            return conn;
        }

        class OneExchangeSubscriptionConnection {
        private:
            amqp_connection_state_t connection_;
            ConnectionLocator locator_;
            std::function<void(basic::ByteDataWithTopic &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::thread th_;
            std::atomic<bool> running_;
            RabbitMQComponent::ExceptionPolicy exceptionPolicy_;
            void run(std::string const &tag) {
                while (running_) {
                    try {
                        amqp_rpc_reply_t res;
                        amqp_envelope_t envelop;

                        amqp_maybe_release_buffers(connection_);
                        struct timeval tv {1, 0};
                        res = amqp_consume_message(connection_, &envelop, &tv, 0);
                        if (res.reply_type == AMQP_RESPONSE_NORMAL) {
                            if (running_) {
                                if (wireToUserHook_) {
                                    auto b = (wireToUserHook_->hook)(basic::ByteDataView {std::string_view((char const *) envelop.message.body.bytes, envelop.message.body.len)});
                                    if (b) {
                                        callback_({ 
                                            std::string((char const *) envelop.routing_key.bytes, envelop.routing_key.len)
                                            , std::move(b->content)
                                        });
                                    }
                                } else {
                                    callback_({
                                        std::string((char const *) envelop.routing_key.bytes, envelop.routing_key.len)
                                        , std::string((char const *) envelop.message.body.bytes, envelop.message.body.len)
                                    });
                                }
                            }
                            amqp_destroy_envelope(&envelop);
                        }
                    } catch (std::exception &ex) {
                        switch (exceptionPolicy_) {
                        case RabbitMQComponent::ExceptionPolicy::Throw:
                            throw;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::IgnoreForWriteAndThrowForRead:
                            throw;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::IgnoreForWriteAndStopForRead:
                            std::cerr << "RabbitMQComponent exchange subscription client exception: " << ex.what() << "\n";
                            running_ = false;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::Ignore:
                            break;
                        default:
                            throw;
                            break;
                        }
                    }
                }
            }
        public:
            OneExchangeSubscriptionConnection(RabbitMQComponent::ExceptionPolicy exceptionPolicy, ConnectionLocator const &l, std::string const &topic, std::function<void(basic::ByteDataWithTopic &&)> callback, std::optional<WireToUserHook> wireToUserHook, TLSClientConfigurationComponent const *config)
                : connection_(createConnection(l, config))
                , locator_(l)
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , th_()
                , running_(true)
                , exceptionPolicy_(exceptionPolicy)
            {
                amqp_exchange_declare(
                    connection_
                    , 1
                    , amqp_cstring_bytes(l.identifier().c_str())
                    , amqp_cstring_bytes("topic")
                    , (l.query("passive", "false") == "true")
                    , (l.query("durable", "false") == "true")
                    , (l.query("auto_delete", "false") == "true")
                    , false
                    , amqp_empty_table
                );
                if (amqp_get_rpc_reply(connection_).reply_type != AMQP_RESPONSE_NORMAL) {
                    throw RabbitMQComponentException("Cannot declare RabbitMQ exchange for "+l.toPrintFormat());
                }
                auto q = amqp_queue_declare(
                    connection_
                    , 1
                    , amqp_cstring_bytes("")
                    , false
                    , false
                    , false
                    , false
                    , amqp_empty_table
                );
                if (amqp_get_rpc_reply(connection_).reply_type != AMQP_RESPONSE_NORMAL) {
                    throw RabbitMQComponentException("Cannot declare RabbitMQ queue for "+l.toPrintFormat());
                }
                std::string queueName {(char *) q->queue.bytes, q->queue.len};
                amqp_queue_bind(
                    connection_
                    , 1
                    , amqp_cstring_bytes(queueName.c_str())
                    , amqp_cstring_bytes(l.identifier().c_str())
                    , amqp_cstring_bytes(topic.c_str())
                    , amqp_empty_table 
                );
                if (amqp_get_rpc_reply(connection_).reply_type != AMQP_RESPONSE_NORMAL) {
                    throw RabbitMQComponentException("Cannot bind RabbitMQ queue for "+l.toPrintFormat());
                }
                amqp_basic_consume(
                    connection_
                    , 1 
                    , amqp_cstring_bytes(queueName.c_str())
                    , amqp_cstring_bytes(queueName.c_str())
                    , false
                    , false
                    , false
                    , amqp_empty_table
                );
                if (amqp_get_rpc_reply(connection_).reply_type != AMQP_RESPONSE_NORMAL) {
                    throw RabbitMQComponentException("Cannot consume on RabbitMQ queue for "+l.toPrintFormat());
                }
                th_ = std::thread(&OneExchangeSubscriptionConnection::run, this, queueName);
            }
            ~OneExchangeSubscriptionConnection() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
                amqp_channel_close(connection_, 1, AMQP_REPLY_SUCCESS);
                amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
                amqp_destroy_connection(connection_);
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
            ConnectionLocator const &connectionLocator() const {
                return locator_;
            }
        };
        std::unordered_map<uint32_t, std::unique_ptr<OneExchangeSubscriptionConnection>> exchangeSubscriptionConnections_;

        class OnePublishingConnection {
        private:
            amqp_connection_state_t connection_;
            std::mutex mutex_;
            RabbitMQComponent::ExceptionPolicy exceptionPolicy_;
        public:
            OnePublishingConnection(RabbitMQComponent::ExceptionPolicy exceptionPolicy, ConnectionLocator const &l, TLSClientConfigurationComponent const *config) 
                : connection_(createConnection(l, config))
                , mutex_()
                , exceptionPolicy_(exceptionPolicy)
            {
            }
            ~OnePublishingConnection() {
                amqp_channel_close(connection_, 1, AMQP_REPLY_SUCCESS);
                amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
                amqp_destroy_connection(connection_);
            }
            void publishOnExchange(std::string const &exchange, basic::ByteDataWithTopic &&data) {
                {
                    std::lock_guard<std::mutex> _(mutex_);

                    amqp_basic_properties_t props;
                    props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_EXPIRATION_FLAG;
                    props.delivery_mode = AMQP_DELIVERY_NONPERSISTENT;
                    props.expiration = amqp_cstring_bytes("1000");

                    amqp_bytes_t b {data.content.length(), const_cast<char *>(data.content.c_str())};
                    amqp_basic_publish(
                        connection_
                        , 1
                        , amqp_cstring_bytes(exchange.c_str())
                        , amqp_cstring_bytes(data.topic.c_str())
                        , false 
                        , false 
                        , &props
                        , b
                    );
                    try {     
                        auto reply = amqp_get_rpc_reply(connection_);
                        if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
                            throw std::runtime_error("Publishing on exchange");
                        }
                    } catch (std::exception &ex) {
                        switch (exceptionPolicy_) {
                        case RabbitMQComponent::ExceptionPolicy::Throw:
                            throw;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::IgnoreForWriteAndThrowForRead:
                        case RabbitMQComponent::ExceptionPolicy::IgnoreForWriteAndStopForRead:
                            std::cerr << "RabbitMQComponent publishing exception: " << ex.what() << "\n";
                            break;
                        case RabbitMQComponent::ExceptionPolicy::Ignore:
                            break;
                        default:
                            throw;
                            break;
                        }
                    }
                }
            }
            void publishOnQueue(std::string const &queue, amqp_basic_properties_t *props, std::string const &message) {
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    amqp_bytes_t b {message.length(), const_cast<char *>(message.c_str())};
                    amqp_basic_publish(
                        connection_
                        , 1
                        , amqp_cstring_bytes("")
                        , amqp_cstring_bytes(queue.c_str())
                        , false 
                        , false 
                        , props
                        , b
                    );
                    try {
                        auto reply = amqp_get_rpc_reply(connection_);
                        if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
                            throw std::runtime_error("Publishing on queue");
                        }
                    } catch (std::exception &ex) {
                        switch (exceptionPolicy_) {
                        case RabbitMQComponent::ExceptionPolicy::Throw:
                            throw;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::IgnoreForWriteAndThrowForRead:
                        case RabbitMQComponent::ExceptionPolicy::IgnoreForWriteAndStopForRead:
                            std::cerr << "RabbitMQComponent publishing exception: " << ex.what() << "\n";
                            break;
                        case RabbitMQComponent::ExceptionPolicy::Ignore:
                            break;
                        default:
                            throw;
                            break;
                        }
                    }
                }
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OnePublishingConnection>> publishingConnections_;
        
        class OneRPCQueueClientConnection {
        private:
            amqp_connection_state_t connection_;
            std::string rpcQueue_, localQueue_;
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
            OnePublishingConnection *publishing_;
            bool persistent_;
            RabbitMQComponent::ExceptionPolicy exceptionPolicy_;

            void run() {
                while (running_) {
                    try {
                        amqp_rpc_reply_t res;
                        amqp_envelope_t envelop;

                        amqp_maybe_release_buffers(connection_);
                        struct timeval tv {1, 0};
                        res = amqp_consume_message(connection_, &envelop, &tv, 0);
                        if (res.reply_type == AMQP_RESPONSE_NORMAL) {
                            if (running_) {
                                std::string corrID { (char *) envelop.message.properties.correlation_id.bytes, envelop.message.properties.correlation_id.len};
                                bool isFinal = (
                                    ((envelop.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) != 0)
                                    &&
                                    (std::string_view((char *) envelop.message.properties.content_type.bytes, envelop.message.properties.content_type.len) == "final")
                                );
                                {
                                    std::lock_guard<std::mutex> _(clientsMutex_);
                                    auto iter = idToClientMap_.find(corrID);
                                    if (iter != idToClientMap_.end()) {
                                        auto iter1 = clients_.find(iter->second);
                                        if (iter1 != clients_.end()) {
                                            if (iter1->second.wireToUserHook_) {
                                                auto d = (iter1->second.wireToUserHook_->hook)(basic::ByteDataView {std::string_view((char *) envelop.message.body.bytes, envelop.message.body.len)});
                                                if (d) {
                                                    iter1->second.callback_(isFinal, {corrID, std::move(d->content)});
                                                }
                                            } else {
                                                iter1->second.callback_(isFinal, {corrID, std::string((char *) envelop.message.body.bytes, envelop.message.body.len)});
                                            }
                                        }
                                        if (isFinal) {
                                            clientToIDMap_[iter->second].erase(corrID);
                                            idToClientMap_.erase(iter);
                                        }
                                    }
                                }
                            }
                            amqp_destroy_envelope(&envelop);
                        }
                    } catch (std::exception const &ex) {
                        switch (exceptionPolicy_) {
                        case RabbitMQComponent::ExceptionPolicy::Throw:
                            throw;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::IgnoreForWriteAndThrowForRead:
                            throw;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::IgnoreForWriteAndStopForRead:
                            std::cerr << "RabbitMQComponent RPC queue client exception: " << ex.what() << "\n";
                            running_ = false;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::Ignore:
                            break;
                        default:
                            throw;
                            break;
                        }
                    }
                }
            }
        public:
            OneRPCQueueClientConnection(RabbitMQComponent::ExceptionPolicy exceptionPolicy, ConnectionLocator const &l, OnePublishingConnection *publishing, TLSClientConfigurationComponent const *config)
                : connection_(createConnection(l, config))
                , rpcQueue_(l.identifier())
                , localQueue_("")
                , clientCounter_(0)
                , clients_()
                , clientToIDMap_()
                , idToClientMap_()
                , clientsMutex_()
                , th_()
                , running_(true)
                , publishing_(publishing)
                , persistent_(l.query("persistent", "false") == "true")
                , exceptionPolicy_(exceptionPolicy)
            {
                auto q = amqp_queue_declare(
                    connection_
                    , 1
                    , amqp_cstring_bytes("")
                    , false
                    , false
                    , false
                    , false
                    , amqp_empty_table
                );
                if (amqp_get_rpc_reply(connection_).reply_type != AMQP_RESPONSE_NORMAL) {
                    throw RabbitMQComponentException("Cannot declare RabbitMQ queue for "+l.toPrintFormat());
                }
                localQueue_ = std::string {(char *) q->queue.bytes, q->queue.len};
                amqp_basic_consume(
                    connection_
                    , 1 
                    , amqp_cstring_bytes(localQueue_.c_str())
                    , amqp_cstring_bytes(localQueue_.c_str())
                    , false
                    , false
                    , false
                    , amqp_empty_table
                );
                if (amqp_get_rpc_reply(connection_).reply_type != AMQP_RESPONSE_NORMAL) {
                    throw RabbitMQComponentException("Cannot consume on RabbitMQ queue for "+l.toPrintFormat());
                }
                th_ = std::thread(&OneRPCQueueClientConnection::run, this);
            }
            ~OneRPCQueueClientConnection() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
                amqp_channel_close(connection_, 1, AMQP_REPLY_SUCCESS);
                amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
                amqp_destroy_connection(connection_);
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
                amqp_basic_properties_t props;
                props._flags = AMQP_BASIC_CORRELATION_ID_FLAG | AMQP_BASIC_REPLY_TO_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_EXPIRATION_FLAG;
                props.correlation_id = amqp_cstring_bytes(data.id.c_str());
                props.reply_to = amqp_cstring_bytes(localQueue_.c_str());
                props.delivery_mode = (persistent_?AMQP_DELIVERY_PERSISTENT:AMQP_DELIVERY_NONPERSISTENT);
                props.expiration = amqp_cstring_bytes("5000");

                {
                    std::lock_guard<std::mutex> _(clientsMutex_);
                    clientToIDMap_[clientNumber].insert(data.id);
                    idToClientMap_[data.id] = clientNumber;
                }
       
                publishing_->publishOnQueue(rpcQueue_, &props, data.content);           
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRPCQueueClientConnection>> rpcQueueClientConnections_;
        
        class OneRPCQueueServerConnection {
        private:
            amqp_connection_state_t connection_;
            std::string rpcQueue_;
            std::function<void(basic::ByteDataWithID &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::unordered_map<std::string, std::string> replyQueueMap_;
            std::thread th_;
            std::mutex mutex_;
            std::atomic<bool> running_;
            OnePublishingConnection *publishing_;
            RabbitMQComponent::ExceptionPolicy exceptionPolicy_;
            void run() {
                while (running_) {
                    try {
                        amqp_rpc_reply_t res;
                        amqp_envelope_t envelop;

                        amqp_maybe_release_buffers(connection_);
                        struct timeval tv {1, 0};
                        res = amqp_consume_message(connection_, &envelop, &tv, 0);
                        if (res.reply_type == AMQP_RESPONSE_NORMAL) {
                            amqp_basic_ack(connection_, 1, envelop.delivery_tag, false);
                            if (running_) {
                                std::string corrID { (char *) envelop.message.properties.correlation_id.bytes, envelop.message.properties.correlation_id.len};
                                {
                                    std::lock_guard<std::mutex> _(mutex_);
                                    replyQueueMap_[corrID] = std::string {(char *) envelop.message.properties.reply_to.bytes, envelop.message.properties.reply_to.len};
                                }
                                if (wireToUserHook_) {
                                    auto d = (wireToUserHook_->hook)(basic::ByteDataView {std::string_view((char *) envelop.message.body.bytes, envelop.message.body.len)});
                                    if (d) {
                                        callback_({corrID, std::move(d->content)});
                                    }
                                } else {
                                    callback_({corrID, std::string((char *) envelop.message.body.bytes, envelop.message.body.len)});
                                }
                            }
                            amqp_destroy_envelope(&envelop);
                        }
                    } catch (std::exception &ex) {
                        switch (exceptionPolicy_) {
                        case RabbitMQComponent::ExceptionPolicy::Throw:
                            throw;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::IgnoreForWriteAndThrowForRead:
                            throw;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::IgnoreForWriteAndStopForRead:
                            std::cerr << "RabbitMQComponent RPC queue server exception: " << ex.what() << "\n";
                            running_ = false;
                            break;
                        case RabbitMQComponent::ExceptionPolicy::Ignore:
                            break;
                        default:
                            throw;
                            break;
                        }
                    }                   
                }
            }
        public:
            OneRPCQueueServerConnection(RabbitMQComponent::ExceptionPolicy exceptionPolicy, ConnectionLocator const &l, std::function<void(basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, OnePublishingConnection *publishing, TLSClientConfigurationComponent const *config)
                : connection_(createConnection(l, config))
                , rpcQueue_(l.identifier())
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , replyQueueMap_()
                , th_()
                , mutex_()
                , running_(true)
                , publishing_(publishing)
                , exceptionPolicy_(exceptionPolicy)
            {
                amqp_queue_declare(
                    connection_
                    , 1
                    , amqp_cstring_bytes(rpcQueue_.c_str())
                    , false //passive
                    , false //durable
                    , false //exclusive
                    , false //auto_delete
                    , amqp_empty_table
                );
                if (amqp_get_rpc_reply(connection_).reply_type != AMQP_RESPONSE_NORMAL) {
                    throw RabbitMQComponentException("Cannot declare RabbitMQ queue for "+l.toPrintFormat());
                }
                amqp_basic_consume(
                    connection_
                    , 1 
                    , amqp_cstring_bytes(rpcQueue_.c_str())
                    , amqp_cstring_bytes(rpcQueue_.c_str())
                    , true //no_local
                    , false //no_ack
                    , false
                    , amqp_empty_table
                );
                if (amqp_get_rpc_reply(connection_).reply_type != AMQP_RESPONSE_NORMAL) {
                    throw RabbitMQComponentException("Cannot consume on RabbitMQ queue for "+l.toPrintFormat());
                }
                amqp_basic_qos(
                    connection_
                    , 1
                    , 0
                    , 0
                    , false
                );
                if (amqp_get_rpc_reply(connection_).reply_type != AMQP_RESPONSE_NORMAL) {
                    throw RabbitMQComponentException("Cannot set QoS on RabbitMQ queue for "+l.toPrintFormat());
                }
                th_ = std::thread(&OneRPCQueueServerConnection::run, this);
            }
            ~OneRPCQueueServerConnection() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
                amqp_channel_close(connection_, 1, AMQP_REPLY_SUCCESS);
                amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
                amqp_destroy_connection(connection_);
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
                amqp_basic_properties_t props;
                if (isFinal) {
                    props._flags = AMQP_BASIC_CORRELATION_ID_FLAG | AMQP_BASIC_REPLY_TO_FLAG | AMQP_BASIC_CONTENT_TYPE_FLAG;
                } else {
                    props._flags = AMQP_BASIC_CORRELATION_ID_FLAG | AMQP_BASIC_REPLY_TO_FLAG;
                }
                
                props.correlation_id = amqp_cstring_bytes(data.id.c_str());
                props.reply_to = amqp_cstring_bytes(rpcQueue_.c_str());
                if (isFinal) {
                    props.content_type = amqp_cstring_bytes("final");
                }
                publishing_->publishOnQueue(replyQueue, &props, data.content);            
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneRPCQueueServerConnection>> rpcQueueServerConnections_;

        std::mutex mutex_;
        uint32_t counter_;

        RabbitMQComponent::ExceptionPolicy exceptionPolicy_;

        OnePublishingConnection *publishingConnection(ConnectionLocator const &l, TLSClientConfigurationComponent const *config) {
            std::lock_guard<std::mutex> _(mutex_);
            return publishingConnectionNoLock(l, config);
        }
        OnePublishingConnection *publishingConnectionNoLock(ConnectionLocator const &l, TLSClientConfigurationComponent const *config) {
            auto basicPortion = l.copyOfBasicPortionWithProperties();
            auto iter = publishingConnections_.find(basicPortion);
            if (iter == publishingConnections_.end()) {
                iter = publishingConnections_.insert(
                    {basicPortion, std::make_unique<OnePublishingConnection>(exceptionPolicy_, basicPortion, config)}
                ).first;
            }
            return iter->second.get();
        }
        OneRPCQueueClientConnection *createRpcQueueClientConnection(ConnectionLocator const &l, TLSClientConfigurationComponent const *config) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcQueueClientConnections_.find(l);
            if (iter == rpcQueueClientConnections_.end()) {
                iter = rpcQueueClientConnections_.insert(
                    {l, std::make_unique<OneRPCQueueClientConnection>(exceptionPolicy_, l, publishingConnectionNoLock(l, config), config)}
                ).first;
            }
            return iter->second.get();
        }
        OneRPCQueueServerConnection *createRpcQueueServerConnection(ConnectionLocator const &l, std::function<void(basic::ByteDataWithID &&)> handler, std::optional<WireToUserHook> wireToUserHook, TLSClientConfigurationComponent const *config) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcQueueServerConnections_.find(l);
            if (iter != rpcQueueServerConnections_.end()) {
                throw RabbitMQComponentException("Cannot create duplicate RPC Queue server connection for "+l.toSerializationFormat());
            }
            iter = rpcQueueServerConnections_.insert(
                {l, std::make_unique<OneRPCQueueServerConnection>(exceptionPolicy_, l, handler, wireToUserHook, publishingConnectionNoLock(l, config), config)}
            ).first;
            return iter->second.get();
        }
    public:
        RabbitMQComponentImpl(RabbitMQComponent::ExceptionPolicy exceptionPolicy) :
            exchangeSubscriptionConnections_()
            , publishingConnections_()
            , rpcQueueClientConnections_()
            , rpcQueueServerConnections_()
            , mutex_()
            , counter_(0)
            , exceptionPolicy_(exceptionPolicy)
        {}
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
            std::optional<WireToUserHook> wireToUserHook,
            TLSClientConfigurationComponent const *config) {
            std::lock_guard<std::mutex> _(mutex_);
            exchangeSubscriptionConnections_.insert(
                std::make_pair(
                    ++counter_
                    , std::make_unique<OneExchangeSubscriptionConnection>(
                        exceptionPolicy_, locator, topic, client, wireToUserHook, config
                    )
                )
            );
            return counter_;
        }
        void removeExchangeSubscriptionClient(uint32_t id) {
            std::lock_guard<std::mutex> _(mutex_);
            exchangeSubscriptionConnections_.erase(id);
        }
        std::function<void(basic::ByteDataWithTopic &&)> getExchangePublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook, TLSClientConfigurationComponent const *config) {
            auto *conn = publishingConnection(locator, config);
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
            std::optional<ByteDataHookPair> hookPair,
            TLSClientConfigurationComponent const *config,
            uint32_t *clientNumberOutput) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcQueueClientConnection(locator, config);
            auto clientNum = conn->addClient(client, wireToUserHook);
            if (clientNumberOutput) {
                *clientNumberOutput = clientNum;
            }
            if (hookPair && hookPair->userToWire) {
                auto hook = hookPair->userToWire->hook;
                return [conn,clientNum,hook](basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->sendRequest(clientNum, {data.id, std::move(x.content)});
                };
            } else {
                return [conn,clientNum](basic::ByteDataWithID &&data) {
                    conn->sendRequest(clientNum, std::move(data));
                };
            }
        }
        void removeRPCQueueClient(ConnectionLocator const &locator, uint32_t clientNumber) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcQueueClientConnections_.find(locator);
            if (iter != rpcQueueClientConnections_.end()) {
                if (iter->second->removeClient(clientNumber) == 0) {
                    rpcQueueClientConnections_.erase(iter);
                }
            }
        }
        std::function<void(bool, basic::ByteDataWithID &&)> setRPCQueueServer(ConnectionLocator const &locator,
            std::function<void(basic::ByteDataWithID &&)> server,
            std::optional<ByteDataHookPair> hookPair,
            TLSClientConfigurationComponent const *config) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcQueueServerConnection(locator, server, wireToUserHook, config);
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
            for (auto &item : exchangeSubscriptionConnections_) {
                retVal[item.second->connectionLocator()] = item.second->getThreadHandle();
            }
            for (auto &item : rpcQueueClientConnections_) {
                retVal[item.first] = item.second->getThreadHandle();
            }
            for (auto &item : rpcQueueServerConnections_) {
                retVal[item.first] = item.second->getThreadHandle();
            }
            return retVal;
        }
    };

    RabbitMQComponent::RabbitMQComponent(RabbitMQComponent::ExceptionPolicy exceptionPolicy) : impl_(std::make_unique<RabbitMQComponentImpl>(exceptionPolicy)) {}
    RabbitMQComponent::RabbitMQComponent(RabbitMQComponent &&) = default;
    RabbitMQComponent &RabbitMQComponent::operator=(RabbitMQComponent &&) = default;
    RabbitMQComponent::~RabbitMQComponent() = default;

    uint32_t RabbitMQComponent::rabbitmq_addExchangeSubscriptionClient(ConnectionLocator const &locator,
        std::string const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addExchangeSubscriptionClient(locator, topic, client, wireToUserHook, dynamic_cast<TLSClientConfigurationComponent const *>(this));
    }
    void RabbitMQComponent::rabbitmq_removeExchangeSubscriptionClient(uint32_t id) {
        impl_->removeExchangeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&)> RabbitMQComponent::rabbitmq_getExchangePublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getExchangePublisher(locator, userToWireHook, dynamic_cast<TLSClientConfigurationComponent const *>(this));
    }
    std::function<void(basic::ByteDataWithID &&)> RabbitMQComponent::rabbitmq_setRPCQueueClient(ConnectionLocator const &locator,
        std::function<void(bool, basic::ByteDataWithID &&)> client,
        std::optional<ByteDataHookPair> hookPair,
        uint32_t *clientNumberOutput) {
        return impl_->setRPCQueueClient(locator, client, hookPair, dynamic_cast<TLSClientConfigurationComponent const *>(this), clientNumberOutput);
    }
    void RabbitMQComponent::rabbitmq_removeRPCQueueClient(ConnectionLocator const &locator, uint32_t clientNumber) {
        impl_->removeRPCQueueClient(locator, clientNumber);
    }
    std::function<void(bool, basic::ByteDataWithID &&)> RabbitMQComponent::rabbitmq_setRPCQueueServer(ConnectionLocator const &locator,
        std::function<void(basic::ByteDataWithID &&)> server,
        std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCQueueServer(locator, server, hookPair, dynamic_cast<TLSClientConfigurationComponent const *>(this));
    }
    std::unordered_map<ConnectionLocator, std::thread::native_handle_type> RabbitMQComponent::rabbitmq_threadHandles() {
        return impl_->threadHandles();
    }

} } } } }