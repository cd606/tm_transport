#include <thread>
#include <mutex>
#include <cstring>
#include <unordered_map>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>

#include <tm_kit/transport/singlecast/SinglecastComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace singlecast {
    
    enum class SinglecastComponentTopicEncodingChoice {
        CBOR
        , Binary
        , BinaryAdHoc
    };

    SinglecastComponentTopicEncodingChoice parseEncodingChoice(std::string const &s) {
        if (s == "binary-adhoc") {
            return SinglecastComponentTopicEncodingChoice::BinaryAdHoc;
        } else if (s == "binary" || s == "raw-binary") {
            return SinglecastComponentTopicEncodingChoice::Binary;
        } else {
            return SinglecastComponentTopicEncodingChoice::CBOR;
        }
    }

    class SinglecastComponentImpl {
    private:
        class OneSinglecastSubscription {
        private:
            SinglecastComponentTopicEncodingChoice encodingChoice_;
            ConnectionLocator locator_;
            boost::asio::ip::udp::socket sock_;
            boost::asio::ip::udp::endpoint senderPoint_;
            std::array<char, 16*1024*1024> buffer_;
            struct ClientCB {
                uint32_t id;
                std::function<void(basic::ByteDataWithTopic &&)> cb;
                std::optional<WireToUserHook> hook;
            };
            std::vector<ClientCB> noFilterClients_;
            std::vector<std::tuple<std::string, ClientCB>> stringMatchClients_;
            std::vector<std::tuple<std::regex, ClientCB>> regexMatchClients_;
            std::optional<std::thread::native_handle_type> thHandle_;
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

            void handleReceive(boost::system::error_code const &err, size_t bytesReceived) {
                if (!running_) {
                    return;
                }
                if (!err) {
                    std::optional<std::tuple<basic::ByteDataWithTopic, std::size_t>> parseRes;
                    if (encodingChoice_ == SinglecastComponentTopicEncodingChoice::CBOR) {
                        parseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithTopic>::apply(std::string_view {buffer_.data(), bytesReceived}, 0);
                    } else if (encodingChoice_ == SinglecastComponentTopicEncodingChoice::Binary) {
                        parseRes = std::tuple<basic::ByteDataWithTopic, std::size_t> {
                            basic::ByteDataWithTopic {
                                .topic = std::string(),
                                .content = std::string(buffer_.data(), bytesReceived),
                            }
                            , bytesReceived
                        };
                    } else if (encodingChoice_ == SinglecastComponentTopicEncodingChoice::BinaryAdHoc) {
                        if (bytesReceived < sizeof(uint32_t)) {
                            parseRes = std::nullopt;
                        } else {
                            uint32_t topicLen;
                            std::memcpy(&topicLen, buffer_.data(), sizeof(uint32_t));
                            if (bytesReceived < topicLen+sizeof(uint32_t)) {
                                parseRes = std::nullopt;
                            } else {
                                basic::ByteDataWithTopic res;
                                res.topic.resize(topicLen);
                                std::memcpy(res.topic.data(), buffer_.data()+sizeof(uint32_t), topicLen);
                                res.content.resize(bytesReceived-sizeof(uint32_t)-topicLen);
                                std::memcpy(res.content.data(), buffer_.data()+sizeof(uint32_t)+topicLen, bytesReceived-sizeof(uint32_t)-topicLen);
                                parseRes = std::tuple<basic::ByteDataWithTopic, std::size_t> {
                                    std::move(res)
                                    , bytesReceived
                                };
                            }
                        }
                    } else {
                        return;
                    }
                    if (parseRes && std::get<1>(*parseRes) == bytesReceived) {
                        basic::ByteDataWithTopic data = std::move(std::get<0>(*parseRes));

                        std::lock_guard<std::mutex> _(mutex_);

                        const bool filterByTopic = (encodingChoice_ != SinglecastComponentTopicEncodingChoice::Binary);

                        if (noFilterClients_.size()+stringMatchClients_.size()+regexMatchClients_.size() <= 1) {
                            for (auto const &f : noFilterClients_) {
                                callClient(f, std::move(data));
                            }
                            for (auto const &f : stringMatchClients_) {
                                if (!filterByTopic || data.topic == std::get<0>(f)) {
                                    callClient(std::get<1>(f), std::move(data));
                                }
                            }
                            for (auto const &f : regexMatchClients_) {
                                if (!filterByTopic || std::regex_match(data.topic, std::get<0>(f))) {
                                    callClient(std::get<1>(f), std::move(data));
                                }
                            }    
                        } else {
                            for (auto const &f : noFilterClients_) {
                                callClient(f, basic::ByteDataWithTopic {data});
                            }
                            for (auto const &f : stringMatchClients_) {
                                if (!filterByTopic || data.topic == std::get<0>(f)) {
                                    callClient(std::get<1>(f), basic::ByteDataWithTopic {data});
                                }
                            }
                            for (auto const &f : regexMatchClients_) {
                                if (!filterByTopic || std::regex_match(data.topic, std::get<0>(f))) {
                                    callClient(std::get<1>(f), basic::ByteDataWithTopic {data});
                                }
                            }
                        }
                    }
                    sock_.async_receive_from(
                        boost::asio::buffer(buffer_.data(), buffer_.size())
                        , senderPoint_
                        , boost::bind(&OneSinglecastSubscription::handleReceive
                                    , this
                                    , boost::asio::placeholders::error
                                    , boost::asio::placeholders::bytes_transferred)
                    );   
                }
            }
        public:
#if BOOST_VERSION >= 108700        
            OneSinglecastSubscription(SinglecastComponentTopicEncodingChoice encodingChoice, boost::asio::io_context *service, ConnectionLocator const &locator) 
#else
            OneSinglecastSubscription(SinglecastComponentTopicEncodingChoice encodingChoice, boost::asio::io_service *service, ConnectionLocator const &locator) 
#endif            
                : encodingChoice_(encodingChoice), locator_(locator), sock_(*service), senderPoint_(), buffer_()
                , noFilterClients_(), stringMatchClients_(), regexMatchClients_()
                , thHandle_()
                , mutex_(), running_(true)
            {
                boost::asio::ip::udp::resolver resolver(*service);

                {
#if BOOST_VERSION >= 108700                    
                    boost::asio::ip::udp::endpoint listenPoint = resolver.resolve("0.0.0.0", std::to_string(locator.port())).begin()->endpoint();  
#else
                    boost::asio::ip::udp::resolver::query query("0.0.0.0", std::to_string(locator.port()));
                    boost::asio::ip::udp::endpoint listenPoint = resolver.resolve(query)->endpoint();
#endif                    
                    sock_.open(listenPoint.protocol());
                    sock_.set_option(boost::asio::ip::udp::socket::receive_buffer_size(16*1024*1024));
                    sock_.bind(listenPoint);
                }

                sock_.async_receive_from(
                    boost::asio::buffer(buffer_.data(), buffer_.size())
                    , senderPoint_
                    , boost::bind(&OneSinglecastSubscription::handleReceive
                                , this
                                , boost::asio::placeholders::error
                                , boost::asio::placeholders::bytes_transferred)
                );
            }
            ~OneSinglecastSubscription() {
                running_ = false;
                sock_.close();
            }
            ConnectionLocator const &locator() const {
                return locator_;
            }
            void addSubscription(
                uint32_t id
                , std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> const &topic
                , std::function<void(basic::ByteDataWithTopic &&)> handler
                , std::optional<WireToUserHook> wireToUserHook
            ) {
                std::lock_guard<std::mutex> _(mutex_);
                switch (topic.index()) {
                case 0:
                    noFilterClients_.push_back({id, handler, wireToUserHook});
                    break;
                case 1:
                    stringMatchClients_.push_back({std::get<std::string>(topic), {id, handler, wireToUserHook}});
                    break;
                case 2:
                    regexMatchClients_.push_back({std::get<std::regex>(topic), {id, handler, wireToUserHook}});
                    break;
                default:
                    break;
                }
            }
            void removeSubscription(uint32_t id) {
                std::lock_guard<std::mutex> _(mutex_);
                noFilterClients_.erase(
                    std::remove_if(
                        noFilterClients_.begin()
                        , noFilterClients_.end()
                        , [id](auto const &x) {
                            return x.id == id;
                        })
                    , noFilterClients_.end()
                );
                stringMatchClients_.erase(
                    std::remove_if(
                        stringMatchClients_.begin()
                        , stringMatchClients_.end()
                        , [id](auto const &x) {
                            return std::get<1>(x).id == id;
                        })
                    , stringMatchClients_.end()
                );
                regexMatchClients_.erase(
                    std::remove_if(
                        regexMatchClients_.begin()
                        , regexMatchClients_.end()
                        , [id](auto const &x) {
                            return std::get<1>(x).id == id;
                        })
                    , regexMatchClients_.end()
                );
            }
            bool checkWhetherNeedsToStop() {
                std::lock_guard<std::mutex> _(mutex_);
                if (noFilterClients_.empty() && stringMatchClients_.empty() && regexMatchClients_.empty()) {
                    running_ = false;
                    return true;
                } else {
                    return false;
                }
            }
            void setThreadHandle(std::thread::native_handle_type h) {
                std::lock_guard<std::mutex> _(mutex_);
                thHandle_ = h;
            }
            std::optional<std::thread::native_handle_type> getThreadHandle() {
                std::lock_guard<std::mutex> _(mutex_);
                return thHandle_;
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneSinglecastSubscription>> subscriptions_;
        
        class OneSinglecastSender {
        private:
            constexpr static std::size_t SAFE_PAYLOAD_SIZE = 1200;  // avoid IP fragmentation
            SinglecastComponentTopicEncodingChoice encodingChoice_;
            boost::asio::ip::udp::socket sock_;
            boost::asio::ip::udp::endpoint destination_;
            void handleSend(boost::system::error_code const &) {
            }
        public:
#if BOOST_VERSION >= 108700  
            OneSinglecastSender(SinglecastComponentTopicEncodingChoice encodingChoice, boost::asio::io_context *service, ConnectionLocator const &locator)
#else
            OneSinglecastSender(SinglecastComponentTopicEncodingChoice encodingChoice, boost::asio::io_service *service, ConnectionLocator const &locator)
#endif            
                : encodingChoice_(encodingChoice), sock_(*service), destination_()
            {
                boost::asio::ip::udp::resolver resolver(*service);
#if BOOST_VERSION >= 108700  
                destination_ = resolver.resolve(locator.host(), std::to_string(locator.port())).begin()->endpoint();              
#else
                boost::asio::ip::udp::resolver::query query(locator.host(), std::to_string(locator.port()));
                destination_ = resolver.resolve(query)->endpoint();
#endif                

                sock_.open(destination_.protocol());
                sock_.set_option(boost::asio::ip::udp::socket::send_buffer_size(16*1024*1024));
            }
            ~OneSinglecastSender() {
                sock_.close();
            }
            void publish(basic::ByteDataWithTopic &&data) {
                auto v = std::make_unique<std::string>();
                if (encodingChoice_ == SinglecastComponentTopicEncodingChoice::CBOR) {
                    *v = basic::bytedata_utils::RunCBORSerializer<basic::ByteDataWithTopic>::apply(data);
                    if (v->size() > SAFE_PAYLOAD_SIZE) {
                        return;
                    }
                } else if (encodingChoice_ == SinglecastComponentTopicEncodingChoice::Binary || encodingChoice_ == SinglecastComponentTopicEncodingChoice::BinaryAdHoc) {
                    auto dataSize = sizeof(uint32_t)+data.topic.length()+data.content.length();
                    if (dataSize > SAFE_PAYLOAD_SIZE) {
                        return;
                    }
                    v->resize(dataSize);
                    char *p = v->data();
                    uint32_t topicLen = (uint32_t) (data.topic.length());
                    std::memcpy(p, &topicLen, sizeof(uint32_t));
                    std::memcpy(p+sizeof(uint32_t), data.topic.data(), topicLen);
                    std::memcpy(p+sizeof(uint32_t)+topicLen, data.content.data(), data.content.length());
                } else {
                    return;
                }
                auto const_buffer = boost::asio::buffer(reinterpret_cast<const char *>(v->data()), v->size());
                sock_.async_send_to(
                    const_buffer
                    , destination_
                    , [v=std::move(v)](boost::system::error_code const &/*ec*/, std::size_t) {}
                );
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneSinglecastSender>> senders_;
#if BOOST_VERSION >= 108700        
        boost::asio::io_context senderService_;
#else
        boost::asio::io_service senderService_;
#endif        
        std::thread senderThread_;

        std::mutex mutex_;
        bool senderThreadStarted_;

        uint32_t counter_;
        std::unordered_map<uint32_t, OneSinglecastSubscription *> idToSubscriptionMap_;
        std::mutex idMutex_;

        OneSinglecastSubscription *getOrStartSubscription(ConnectionLocator const &d) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = subscriptions_.find(hostAndPort);
            if (iter == subscriptions_.end()) {
                auto choice = parseEncodingChoice(d.query("envelop", "cbor"));
                if (d.query("ignoreTopic", "false") == "true") {
                    choice = SinglecastComponentTopicEncodingChoice::Binary;
                }
#if BOOST_VERSION >= 108700
                std::unique_ptr<boost::asio::io_context> svc = std::make_unique<boost::asio::io_context>();
#else
                std::unique_ptr<boost::asio::io_service> svc = std::make_unique<boost::asio::io_service>();
#endif                
                iter = subscriptions_.insert({hostAndPort, std::make_unique<OneSinglecastSubscription>(choice, svc.get(), hostAndPort)}).first;
                std::thread th([svc=std::move(svc)] {
#if BOOST_VERSION >= 108700   
                    auto work_guard = boost::asio::make_work_guard(*svc);
#else
                    boost::asio::io_service::work work(*svc);
#endif                    
                    svc->run();
                });
                th.detach();
                iter->second->setThreadHandle(th.native_handle());
            }
            return iter->second.get();
        }
        void potentiallyStopSubscription(OneSinglecastSubscription *p) {
            std::lock_guard<std::mutex> _(mutex_);
            if (p->checkWhetherNeedsToStop()) {
                subscriptions_.erase(p->locator());
            }
        }
        OneSinglecastSender *getOrStartSender(ConnectionLocator const &d) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            std::lock_guard<std::mutex> _(mutex_);
            if (!senderThreadStarted_) {
                senderThread_ = std::thread([this]() {
#if BOOST_VERSION >= 108700                    
                    auto work_guard = boost::asio::make_work_guard(senderService_);
#else
                    boost::asio::io_service::work work(senderService_);
#endif                    
                    senderService_.run();
                });
                senderThreadStarted_ = true;
            }
            auto iter = senders_.find(hostAndPort);
            if (iter == senders_.end()) {
                auto choice = parseEncodingChoice(d.query("envelop", "cbor"));
                if (d.query("ignoreTopic", "false") == "true") {
                    choice = SinglecastComponentTopicEncodingChoice::Binary;
                }
                iter = senders_.insert({hostAndPort, std::make_unique<OneSinglecastSender>(choice, &senderService_, hostAndPort)}).first;
            }
            return iter->second.get();
        }
    public:
        SinglecastComponentImpl()
            : subscriptions_(), senders_(), senderService_(), senderThread_(), mutex_(), senderThreadStarted_(false)
            , counter_(0), idToSubscriptionMap_(), idMutex_()
        {            
        }
        ~SinglecastComponentImpl() {
            bool b;
            {
                std::lock_guard<std::mutex> _(mutex_);
                subscriptions_.clear();
                senders_.clear();
                b = senderThreadStarted_;
            }
            if (b) {
                senderService_.stop();
                try {
                    senderThread_.join();
                } catch (std::system_error const &) {
                }
            }
        }
        uint32_t addSubscriptionClient(ConnectionLocator const &locator,
            std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> const &topic,
            std::function<void(basic::ByteDataWithTopic &&)> client,
            std::optional<WireToUserHook> wireToUserHook) {
            auto *p = getOrStartSubscription(locator);
            {
                std::lock_guard<std::mutex> _(idMutex_);
                ++counter_;
                p->addSubscription(counter_, topic, client, wireToUserHook);
                idToSubscriptionMap_[counter_] = p;
                return counter_;
            }
        }
        void removeSubscriptionClient(uint32_t id) {
            OneSinglecastSubscription *p = nullptr;
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
        std::unordered_map<ConnectionLocator, std::thread::native_handle_type> threadHandles() {
            std::unordered_map<ConnectionLocator, std::thread::native_handle_type> retVal;
            std::lock_guard<std::mutex> _(mutex_);
            if (senderThreadStarted_) {
                retVal[ConnectionLocator()] = senderThread_.native_handle();
            }
            for (auto &item : subscriptions_) {
                auto h = item.second->getThreadHandle();
                if (h) {
                    retVal[item.first] = *h;
                }
            }
            return retVal;
        }
    };

    SinglecastComponent::SinglecastComponent() : impl_(std::make_unique<SinglecastComponentImpl>()) {}
    SinglecastComponent::~SinglecastComponent() {}
    uint32_t SinglecastComponent::singlecast_addSubscriptionClient(ConnectionLocator const &locator,
        std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    void SinglecastComponent::singlecast_removeSubscriptionClient(uint32_t id) {
        impl_->removeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&)> SinglecastComponent::singlecast_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook);
    }
    std::unordered_map<ConnectionLocator, std::thread::native_handle_type> SinglecastComponent::singlecast_threadHandles() {
        return impl_->threadHandles();
    }
} } } } }