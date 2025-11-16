#include <thread>
#include <mutex>
#include <atomic>
#include <cstring>
#include <sstream>
#include <unordered_map>

#include <tm_kit/transport/nats/NATSComponent.hpp>
#include <tm_kit/transport/TLSConfigurationComponent.hpp>

#ifdef _MSC_VER
#include <winsock2.h>
#endif
#include <nats/nats.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace nats {
    class NATSComponentImpl {
    private:
        static void auth(ConnectionLocator const &locator, TLSClientConfigurationComponent *tlsConf, natsOptions *opts) {
            auto locatorCACert = locator.query("ca_cert", "");
            auto locatorClientCert = locator.query("client_cert", "");
            auto locatorClientKey = locator.query("client_key", "");

            if (locatorCACert != "" && locatorClientCert != "" && locatorClientKey != "") {
                natsOptions_SetSecure(opts, true);
                natsOptions_LoadCATrustedCertificates(opts, locatorCACert.c_str());
                natsOptions_SetCertificatesChain(opts, locatorClientCert.c_str(), locatorClientKey.c_str()); 
            } else {
                auto sslInfo = tlsConf?(tlsConf->getConfigurationItem(
                    TLSClientInfoKey {
                        locator.host(), locator.port()
                    }
                )):std::nullopt;

                if (sslInfo) {
                    natsOptions_SetSecure(opts, true);
                    natsOptions_LoadCATrustedCertificates(opts, sslInfo->caCertificateFile.c_str());
                    natsOptions_SetCertificatesChain(opts, sslInfo->clientCertificateFile.c_str(), sslInfo->clientKeyFile.c_str());                
                }
            }

            if (locator.password() != "") {
                natsOptions_SetUserInfo(opts, locator.userName().c_str(), locator.password().c_str());
            }
        }
        class OneNATSSubscription {
        private:
            ConnectionLocator locator_;
            std::string topic_;
            natsConnection *conn_;
            natsSubscription *sub_;
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
                try {
                    natsConnection_SubscribeSync(&sub_, conn_, topic_.c_str());
                    while (running_) {
                        natsMsg *msg = nullptr;
                        auto status = natsSubscription_NextMsg(&msg, sub_, 1);
                        if (status == NATS_TIMEOUT) {
                            continue;
                        }
                        if (status != NATS_OK) {
                            if (msg) {
                                natsMsg_Destroy(msg);
                            }
                            break;
                        }
                        if (!running_) {
                            if (msg) {
                                natsMsg_Destroy(msg);
                            }
                            break;
                        }
                        std::string topic {natsMsg_GetSubject(msg)};
                        std::string content {natsMsg_GetData(msg), (std::size_t) natsMsg_GetDataLength(msg)};

                        natsMsg_Destroy(msg);

                        if (!running_) {
                            break;
                        }
                        std::lock_guard<std::mutex> _(mutex_);
                        for (auto const &cb : clients_) {
                            callClient(cb, {topic, content});
                        }
                    }
                } catch (...) {}
            }
        public:
            OneNATSSubscription(ConnectionLocator const &locator, std::string const &topic, TLSClientConfigurationComponent *tlsConf) 
                : locator_(locator)
                , topic_(topic)
                , conn_(nullptr)
                , sub_(nullptr)
                , clients_()
                , th_()
                , mutex_()
                , running_(true)
            {
                natsOptions *opts = nullptr;
                natsOptions_Create(&opts);
                std::ostringstream oss;
                oss << "nats://" << locator.host() << ':' << (locator.port()==0?4222:locator.port());
                natsOptions_SetURL(opts, oss.str().c_str());
                NATSComponentImpl::auth(locator, tlsConf, opts);

                natsConnection_Connect(&conn_, opts);
                natsOptions_Destroy(opts);

                th_ = std::thread(&OneNATSSubscription::run, this);
                th_.detach();
            }
            ~OneNATSSubscription() {
                //std::cerr << this << ": being released\n";
                running_ = false;
                if (th_.joinable()) {
                    try {
                        th_.join();
                    } catch (std::system_error const &) {
                    }
                }
                if (sub_) {
                    natsSubscription_Destroy(sub_);
                    sub_ = nullptr;
                }
                if (conn_) {
                    natsConnection_Destroy(conn_);
                    conn_ = nullptr;
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
                //std::cerr << this << ": calling unsubscribe\n";
                running_ = false;
                if (th_.joinable()) {
                    try {
                        th_.join();
                    } catch (std::system_error const &) {
                    }
                }
                if (sub_) {
                    natsSubscription_Destroy(sub_);
                    sub_ = nullptr;
                }
                if (conn_) {
                    natsConnection_Destroy(conn_);
                    conn_ = nullptr;
                }
            }
            ConnectionLocator const &locator() const {
                return locator_;
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };
        
        std::unordered_map<ConnectionLocator, std::unordered_map<std::string, std::unique_ptr<OneNATSSubscription>>> subscriptions_;

        class OneNATSSender {
        private:
            natsConnection *conn_;
            std::mutex mutex_;
        public:
            OneNATSSender(ConnectionLocator const &locator, TLSClientConfigurationComponent *tlsConf)
                : conn_(nullptr), mutex_()
            {
                natsOptions *opts = nullptr;
                natsOptions_Create(&opts);
                std::ostringstream oss;
                oss << "nats://" << locator.host() << ':' << (locator.port()==0?4222:locator.port());
                natsOptions_SetURL(opts, oss.str().c_str());
                NATSComponentImpl::auth(locator, tlsConf, opts);

                natsConnection_Connect(&conn_, opts);
                natsOptions_Destroy(opts);
            }
            ~OneNATSSender() {
                if (conn_) {
                    natsConnection_Destroy(conn_);
                    conn_ = nullptr;
                }
            }
            void publish(basic::ByteDataWithTopic &&data) {
                std::lock_guard<std::mutex> _(mutex_);
                natsConnection_Publish(conn_, data.topic.c_str(), data.content.data(), data.content.length());
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneNATSSender>> senders_;

        class OneNATSRPCClientConnection {
        private:
            natsConnection *conn_;    
            std::string rpcTopic_;
            struct OneClientInfo {
                std::function<void(bool, basic::ByteDataWithID &&)> callback_;
                std::optional<WireToUserHook> wireToUserHook_;
                natsInbox *inbox_;
                natsSubscription *sub_;
                OneClientInfo(std::function<void(bool, basic::ByteDataWithID &&)> const &cb, std::optional<WireToUserHook> const &h) 
                    : callback_(cb), wireToUserHook_(h), inbox_(nullptr), sub_(nullptr)
                {
                    natsInbox_Create(&inbox_);
                }
                OneClientInfo(OneClientInfo const &o) = delete;
                OneClientInfo(OneClientInfo &&) = default;
                OneClientInfo &operator=(OneClientInfo const &) = delete;
                OneClientInfo &operator=(OneClientInfo &&) = default;
                ~OneClientInfo() {
                    natsInbox_Destroy(inbox_);
                    if (sub_) {
                        natsSubscription_Destroy(sub_);
                        sub_ = nullptr;
                    }
                }
            };
            uint32_t clientCounter_;
            std::unordered_map<uint32_t, std::shared_ptr<OneClientInfo>> clients_;
            std::unordered_map<uint32_t, std::unordered_set<std::string>> clientToIDMap_;
            std::unordered_map<std::string, uint32_t> idToClientMap_;
            std::mutex clientsMutex_;
            std::thread th_;
            std::atomic<bool> running_;

            static void onReply(natsConnection *nc, natsSubscription * /*unused*/, natsMsg *msg, void *closure) {
                OneNATSRPCClientConnection *client = (OneNATSRPCClientConnection *) closure;
                //std::cerr << "Got RPC reply\n";
                if (client->conn_ != nc) {
                    natsMsg_Destroy(msg);
                    return;
                }

                auto parseRes = basic::bytedata_utils::RunCBORDeserializer<std::tuple<bool,basic::ByteDataWithID>>::apply(std::string_view {natsMsg_GetData(msg), (std::size_t) natsMsg_GetDataLength(msg)}, 0);
                if (!parseRes || std::get<1>(*parseRes) != (std::size_t) natsMsg_GetDataLength(msg)) {
                    natsMsg_Destroy(msg);
                    return;
                }

                natsMsg_Destroy(msg);
                {
                    std::lock_guard<std::mutex> _(client->clientsMutex_);
                    std::string theID = std::get<1>(std::get<0>(*parseRes)).id;
                    //std::cerr << "---------- client receive " << theID << '\n';
                    auto iter = client->idToClientMap_.find(theID);
                    if (iter != client->idToClientMap_.end()) {
                        auto iter1 = client->clients_.find(iter->second);
                        if (iter1 != client->clients_.end()) {
                            if (iter1->second->wireToUserHook_) {
                                auto d = (iter1->second->wireToUserHook_->hook)(basic::ByteDataView {std::string_view(std::get<1>(std::get<0>(*parseRes)).content)});
                                if (d) {
                                    iter1->second->callback_(std::get<0>(std::get<0>(*parseRes)), {std::move(std::get<1>(std::get<0>(*parseRes)).id), std::move(d->content)});
                                }
                            } else {
                                iter1->second->callback_(std::get<0>(std::get<0>(*parseRes)), std::move(std::get<1>(std::get<0>(*parseRes))));
                            }
                        }
                        if (std::get<0>(std::get<0>(*parseRes))) {
                            client->clientToIDMap_[iter->second].erase(theID);
                            client->idToClientMap_.erase(iter);
                        }
                    }
                }
            }
        public:
            OneNATSRPCClientConnection(ConnectionLocator const &locator, TLSClientConfigurationComponent *tlsConf)
                : conn_(nullptr)
                , rpcTopic_(locator.identifier())
                , clientCounter_(0)
                , clients_()
                , clientToIDMap_()
                , idToClientMap_()
                , clientsMutex_()
                , th_()
                , running_(true)
            {
                natsOptions *opts = nullptr;
                natsOptions_Create(&opts);
                std::ostringstream oss;
                oss << "nats://" << locator.host() << ':' << (locator.port()==0?4222:locator.port());
                natsOptions_SetURL(opts, oss.str().c_str());
                NATSComponentImpl::auth(locator, tlsConf, opts);

                natsConnection_Connect(&conn_, opts);
                natsOptions_Destroy(opts);
            }
            ~OneNATSRPCClientConnection() {
                if (conn_) {
                    natsConnection_Destroy(conn_);
                    conn_ = nullptr;
                }
            }
            uint32_t addClient(std::function<void(bool, basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook) {
                std::lock_guard<std::mutex> _(clientsMutex_);
                clients_[++clientCounter_] = std::make_shared<OneClientInfo>(callback, wireToUserHook);
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
                if (conn_) {
                    natsConnection_Destroy(conn_);
                    conn_ = nullptr;
                }
            }
            void sendRequest(uint32_t clientNumber, basic::ByteDataWithID &&data) {
                natsInbox *inbox = nullptr;
                natsSubscription **replySub = nullptr;
                {
                    std::lock_guard<std::mutex> _(clientsMutex_);
                    clientToIDMap_[clientNumber].insert(data.id);
                    idToClientMap_[data.id] = clientNumber;
                    auto &c = clients_[clientNumber];
                    inbox = c->inbox_;
                    replySub = &(c->sub_);
                }
                //std::cerr << "client sending on id " << data.id << '\n';
                auto encodedData = basic::bytedata_utils::RunSerializer<basic::CBOR<basic::ByteDataWithID>>::apply({std::move(data)});
                //std::cerr << "reply to '" << inbox << "', publish to '" << rpcTopic_ << "', data length " << encodedData.length() << '\n';                
                if (!replySub || !(*replySub)) {
                    natsConnection_Subscribe(replySub, conn_, inbox, &OneNATSRPCClientConnection::onReply, (void *) this);
                }
                natsConnection_PublishRequest(conn_, rpcTopic_.c_str(), inbox, encodedData.c_str(), encodedData.length());
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneNATSRPCClientConnection>> rpcClientConnections_;

        class OneNATSRPCServerConnection {
        private:
            natsConnection *conn_;
            natsSubscription *sub_;
            std::string rpcTopic_;
            std::function<void(basic::ByteDataWithID &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::unordered_map<std::string, std::string> replyTopicMap_;
            std::thread th_;
            std::mutex mutex_;
            std::atomic<bool> running_;
            void run() {
                natsConnection_SubscribeSync(&sub_, conn_, rpcTopic_.c_str());
                while (running_) {
                    natsMsg *msg = nullptr;
                    auto status = natsSubscription_NextMsg(&msg, sub_, 1);
                    if (status == NATS_TIMEOUT) {
                        continue;
                    }
                    if (status != NATS_OK) {
                        if (msg) {
                            natsMsg_Destroy(msg);
                        }
                        break;
                    }
                    if (!running_) {
                        if (msg) {
                            natsMsg_Destroy(msg);
                        }
                        break;
                    }
                    std::string topic {natsMsg_GetSubject(msg)};
                    //std::cerr << "Got message on topic '" << topic << "'\n";
                    if (topic != rpcTopic_) {
                        natsMsg_Destroy(msg);
                        continue;
                    }

                    auto parseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithID>::apply(std::string_view {natsMsg_GetData(msg), (std::size_t) natsMsg_GetDataLength(msg)}, 0);
                    if (!parseRes || std::get<1>(*parseRes) != (std::size_t) natsMsg_GetDataLength(msg)) {
                        natsMsg_Destroy(msg);
                        continue;
                    }
                    //std::cerr << "Parsed correctly\n";
                    std::string reply {natsMsg_GetReply(msg)};
                    natsMsg_Destroy(msg);
                    if (!running_) {
                        break;
                    }
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        replyTopicMap_[std::get<0>(*parseRes).id] = reply;
                    }
                    if (wireToUserHook_) {
                        auto d = (wireToUserHook_->hook)(basic::ByteDataView {std::string_view(std::get<0>(*parseRes).content)});
                        if (d) {
                            callback_({std::move(std::get<0>(*parseRes).id), std::move(d->content)});
                        }
                    } else {
                        //std::cerr << "Calling callback\n";
                        callback_(std::move(std::get<0>(*parseRes)));
                    }
                }
            }
        public:
            OneNATSRPCServerConnection(ConnectionLocator const &locator, std::function<void(basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook, TLSClientConfigurationComponent *tlsConf)
                : conn_(nullptr)
                , sub_(nullptr)
                , rpcTopic_(locator.identifier())
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , th_()
                , mutex_()
                , running_(true)
            {
                natsOptions *opts = nullptr;
                natsOptions_Create(&opts);
                std::ostringstream oss;
                oss << "nats://" << locator.host() << ':' << (locator.port()==0?4222:locator.port());
                natsOptions_SetURL(opts, oss.str().c_str());
                NATSComponentImpl::auth(locator, tlsConf, opts);

                natsConnection_Connect(&conn_, opts);
                natsOptions_Destroy(opts);

                th_ = std::thread(&OneNATSRPCServerConnection::run, this);
                th_.detach();
            }
            ~OneNATSRPCServerConnection() {
                running_ = false;
                if (th_.joinable()) {
                    try {
                        th_.join();
                    } catch (std::system_error const &) {
                    }
                }
                if (sub_) {
                    natsSubscription_Destroy(sub_);
                    sub_ = nullptr;
                }
                if (conn_) {
                    natsConnection_Destroy(conn_);
                    conn_ = nullptr;
                }
            }
            void sendReply(bool isFinal, basic::ByteDataWithID &&data) {
                //std::cerr << "send reply! id is " << data.id << "\n";
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
                natsConnection_Publish(conn_, replyTopic.c_str(), encodedData.data(), encodedData.length());
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneNATSRPCServerConnection>> rpcServerConnections_;

        std::mutex mutex_;

        uint32_t counter_;
        std::unordered_map<uint32_t, OneNATSSubscription *> idToSubscriptionMap_;
        std::mutex idMutex_;

        TLSClientConfigurationComponent *tlsConf_;

        OneNATSSubscription *getOrStartSubscription(ConnectionLocator const &d, std::string const &topic) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            std::lock_guard<std::mutex> _(mutex_);
            auto subscriptionIter = subscriptions_.find(hostAndPort);
            if (subscriptionIter == subscriptions_.end()) {
                subscriptionIter = subscriptions_.insert({hostAndPort, std::unordered_map<std::string, std::unique_ptr<OneNATSSubscription>> {}}).first;
            }
            auto innerIter = subscriptionIter->second.find(topic);
            if (innerIter == subscriptionIter->second.end()) {
                innerIter = subscriptionIter->second.insert({topic, std::make_unique<OneNATSSubscription>(d, topic, tlsConf_)}).first;
            }
            return innerIter->second.get();
        }
        void potentiallyStopSubscription(OneNATSSubscription *p) {
            //std::cerr << "potentially stopping " << p << '\n';
            std::lock_guard<std::mutex> _(mutex_);
            if (p->checkWhetherNeedsToStop()) {
                //std::cerr << p << ": is being stopped\n";
                p->unsubscribe();
                //std::cerr << p << " is being removed from subscriptionn map '" << p->locator().toPrintFormat() << "'\n";
                ConnectionLocator hostAndPort {p->locator().host(), p->locator().port()};
                subscriptions_.erase(hostAndPort);
                //std::cerr << subscriptions_.size() << '\n';
            }
        }
        OneNATSSender *getOrStartSender(ConnectionLocator const &d) {
            std::lock_guard<std::mutex> _(mutex_);
            return getOrStartSenderNoLock(d);
        }
        OneNATSSender *getOrStartSenderNoLock(ConnectionLocator const &d) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            auto senderIter = senders_.find(hostAndPort);
            if (senderIter == senders_.end()) {
                senderIter = senders_.insert({hostAndPort, std::make_unique<OneNATSSender>(d, tlsConf_)}).first;
            }
            return senderIter->second.get();
        }
        OneNATSRPCClientConnection *createRpcClientConnection(ConnectionLocator const &l) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcClientConnections_.find(l);
            if (iter == rpcClientConnections_.end()) {
                iter = rpcClientConnections_.insert(
                    {l, std::make_unique<OneNATSRPCClientConnection>(l, tlsConf_)}
                ).first;
            }
            return iter->second.get();
        }
        OneNATSRPCServerConnection *createRpcServerConnection(ConnectionLocator const &l, std::function<void(basic::ByteDataWithID &&)> handler, std::optional<WireToUserHook> wireToUserHook) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcServerConnections_.find(l);
            if (iter != rpcServerConnections_.end()) {
                throw NATSComponentException("Cannot create duplicate RPC server connection for "+l.toSerializationFormat());
            }
            iter = rpcServerConnections_.insert(
                {l, std::make_unique<OneNATSRPCServerConnection>(l, handler, wireToUserHook, tlsConf_)}
            ).first;
            return iter->second.get();
        }
    public:
        NATSComponentImpl(TLSClientConfigurationComponent *tlsConf) 
            : subscriptions_(), senders_(), rpcClientConnections_(), rpcServerConnections_(), mutex_()
            , counter_(0), idToSubscriptionMap_(), idMutex_()
            , tlsConf_(tlsConf)
        { 
        }
        ~NATSComponentImpl() {
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
            OneNATSSubscription *p = nullptr;
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
            std::function<void(bool, basic::ByteDataWithID &&)> client,
            std::optional<ByteDataHookPair> hookPair,
            uint32_t *clientNumberOutput) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcClientConnection(locator);
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
            for (auto &item : rpcServerConnections_) {
                retVal[item.first] = item.second->getThreadHandle();
            }
            return retVal;
        }
    };

    NATSComponent::NATSComponent() : impl_(std::make_unique<NATSComponentImpl>(dynamic_cast<TLSClientConfigurationComponent *>(this))) {}
    NATSComponent::~NATSComponent() {
        nats_CloseAndWait(1000);
    }
    NATSComponent::NATSComponent(NATSComponent &&) = default;
    NATSComponent &NATSComponent::operator=(NATSComponent &&) = default;
    uint32_t NATSComponent::nats_addSubscriptionClient(ConnectionLocator const &locator,
        std::string const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    void NATSComponent::nats_removeSubscriptionClient(uint32_t id) {
        impl_->removeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&)> NATSComponent::nats_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook);
    }
    std::function<void(basic::ByteDataWithID &&)> NATSComponent::nats_setRPCClient(ConnectionLocator const &locator,
                        std::function<void(bool, basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair,
                        uint32_t *clientNumberOutput) {
        return impl_->setRPCClient(locator, client, hookPair, clientNumberOutput);
    }
    void NATSComponent::nats_removeRPCClient(ConnectionLocator const &locator, uint32_t clientNumber) {
        impl_->removeRPCClient(locator, clientNumber);
    }
    std::function<void(bool, basic::ByteDataWithID &&)> NATSComponent::nats_setRPCServer(ConnectionLocator const &locator,
                    std::function<void(basic::ByteDataWithID &&)> server,
                    std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCServer(locator, server, hookPair);
    }
    std::unordered_map<ConnectionLocator, std::thread::native_handle_type> NATSComponent::nats_threadHandles() {
        return impl_->threadHandles();
    }

} } } } }
