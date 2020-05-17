#include <thread>
#include <mutex>
#include <cstring>
#include <unordered_map>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>

#include <tm_kit/transport/multicast/MulticastComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace multicast {
    class MulticastComponentImpl {
    private:
        class OneMulticastSubscription {
        private:
            boost::asio::ip::udp::socket sock_;
            boost::asio::ip::udp::endpoint senderPoint_;
            std::array<char, 16*1024*1024> buffer_;
            struct ClientCB {
                std::function<void(basic::ByteDataWithTopic &&)> cb;
                std::optional<WireToUserHook> hook;
            };
            std::vector<ClientCB> noFilterClients_;
            std::vector<std::tuple<std::string, ClientCB>> stringMatchClients_;
            std::vector<std::tuple<std::regex, ClientCB>> regexMatchClients_;
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

            void handleReceive(boost::system::error_code const &err, size_t bytesReceived) {
                if (!err) {
                    bool good = true;
                    uint32_t topicLen = 0;
                    char const *p = buffer_.data();
                    if (bytesReceived < sizeof(uint32_t)) {
                        good = false;
                    } else {
                        std::memcpy(&topicLen, p, sizeof(uint32_t));
                        p += sizeof(uint32_t);
                        bytesReceived -= sizeof(uint32_t);
                    }
                    std::string topic;
                    if (good) {
                        if (topicLen == 0 || topicLen > bytesReceived) {
                            good = false;
                        } else {
                            topic = std::string {p, p+topicLen};
                            p += topicLen;
                            bytesReceived -= topicLen;
                        }
                    }
                    if (good) {
                        std::lock_guard<std::mutex> _(mutex_);
                        
                        const std::string content {p, p+bytesReceived};
                        for (auto const &f : noFilterClients_) {
                            callClient(f, {topic, content});
                        }
                        for (auto const &f : stringMatchClients_) {
                            if (topic == std::get<0>(f)) {
                                callClient(std::get<1>(f), {topic, content});
                            }
                        }
                        for (auto const &f : regexMatchClients_) {
                            if (std::regex_match(topic, std::get<0>(f))) {
                                callClient(std::get<1>(f), {topic, content});
                            }
                        }
                    }  
                    sock_.async_receive_from(
                        boost::asio::buffer(buffer_.data(), buffer_.size())
                        , senderPoint_
                        , boost::bind(&OneMulticastSubscription::handleReceive
                                    , this
                                    , boost::asio::placeholders::error
                                    , boost::asio::placeholders::bytes_transferred)
                    );   
                }
            }
        public:
            OneMulticastSubscription(boost::asio::io_service *service, ConnectionLocator const &locator) 
                : sock_(*service), senderPoint_(), buffer_()
                , noFilterClients_(), stringMatchClients_(), regexMatchClients_()
                , mutex_()
            {
                boost::asio::ip::udp::resolver resolver(*service);

                {
                    boost::asio::ip::udp::resolver::query query("0.0.0.0", std::to_string(locator.port()));
                    boost::asio::ip::udp::endpoint listenPoint = resolver.resolve(query)->endpoint();
                    sock_.open(listenPoint.protocol());
                    sock_.set_option(boost::asio::ip::udp::socket::reuse_address(true));
                    sock_.set_option(boost::asio::ip::udp::socket::receive_buffer_size(16*1024*1024));
                    sock_.bind(listenPoint);
                }
                
                {
                    boost::asio::ip::udp::resolver::query query(locator.host(), std::to_string(locator.port()));
                    sock_.set_option(boost::asio::ip::multicast::join_group(
                        resolver.resolve(query)->endpoint().address()
                    ));
                }

                sock_.async_receive_from(
                    boost::asio::buffer(buffer_.data(), buffer_.size())
                    , senderPoint_
                    , boost::bind(&OneMulticastSubscription::handleReceive
                                , this
                                , boost::asio::placeholders::error
                                , boost::asio::placeholders::bytes_transferred)
                );
            }
            ~OneMulticastSubscription() = default;
            void addSubscription(
                std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> const &topic
                , std::function<void(basic::ByteDataWithTopic &&)> handler
                , std::optional<WireToUserHook> wireToUserHook
            ) {
                std::lock_guard<std::mutex> _(mutex_);
                switch (topic.index()) {
                case 0:
                    noFilterClients_.push_back({handler, wireToUserHook});
                    break;
                case 1:
                    stringMatchClients_.push_back({std::get<std::string>(topic), {handler, wireToUserHook}});
                    break;
                case 2:
                    regexMatchClients_.push_back({std::get<std::regex>(topic), {handler, wireToUserHook}});
                    break;
                default:
                    break;
                }
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneMulticastSubscription>> subscriptions_;
        
        class OneMulticastSender {
        private:
            boost::asio::ip::udp::socket sock_;
            boost::asio::ip::udp::endpoint destination_;
            std::array<char, 16*1024*1024> buffer_;
            std::mutex mutex_;
            int ttl_;
            void handleSend(boost::system::error_code const &) {
            }
        public:
            OneMulticastSender(boost::asio::io_service *service, ConnectionLocator const &locator)
                : sock_(*service), destination_(), buffer_(), mutex_(), ttl_(0)
            {
                boost::asio::ip::udp::resolver resolver(*service);
                boost::asio::ip::udp::resolver::query query(locator.host(), std::to_string(locator.port()));
                destination_ = resolver.resolve(query)->endpoint();

                sock_.open(destination_.protocol());
                sock_.set_option(boost::asio::ip::udp::socket::reuse_address(true));
                sock_.set_option(boost::asio::ip::udp::socket::send_buffer_size(16*1024*1024));
            }
            void publish(basic::ByteDataWithTopic &&data, int ttl) {
                std::lock_guard<std::mutex> _(mutex_);
                char *p = buffer_.data();
                uint32_t topicLen = (uint32_t) data.topic.length();
                std::memcpy(p, &topicLen, sizeof(uint32_t));
                p += sizeof(uint32_t);
                std::memcpy(p, data.topic.c_str(), topicLen);
                p += topicLen;
                std::memcpy(p, data.content.c_str(), data.content.length());
                p += data.content.length();
                if (ttl != ttl_) {
                    sock_.set_option(boost::asio::ip::multicast::hops(ttl));
                    ttl_ = ttl;
                }
                sock_.async_send_to(
                    boost::asio::buffer(buffer_.data(), p-buffer_.data())
                    , destination_
                    , boost::bind(&OneMulticastSender::handleSend, this, boost::asio::placeholders::error)
                );
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneMulticastSender>> senders_;
        boost::asio::io_service senderService_;
        std::thread senderThread_;

        std::mutex mutex_;
        bool senderThreadStarted_;

        OneMulticastSubscription *getOrStartSubscription(ConnectionLocator const &d) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = subscriptions_.find(hostAndPort);
            if (iter == subscriptions_.end()) {
                std::unique_ptr<boost::asio::io_service> svc = std::make_unique<boost::asio::io_service>();
                iter = subscriptions_.insert({hostAndPort, std::make_unique<OneMulticastSubscription>(svc.get(), hostAndPort)}).first;
                std::thread th([svc=std::move(svc)] {
                    boost::asio::io_service::work work(*svc);
                    svc->run();
                });
                th.detach();
            }
            return iter->second.get();
        }
        OneMulticastSender *getOrStartSender(ConnectionLocator const &d) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            std::lock_guard<std::mutex> _(mutex_);
            if (!senderThreadStarted_) {
                senderThread_ = std::thread([this]() {
                    boost::asio::io_service::work work(senderService_);
                    senderService_.run();
                });
                senderThreadStarted_ = true;
            }
            auto iter = senders_.find(hostAndPort);
            if (iter == senders_.end()) {
                iter = senders_.insert({hostAndPort, std::make_unique<OneMulticastSender>(&senderService_, hostAndPort)}).first;
            }
            return iter->second.get();
        }
    public:
        MulticastComponentImpl()
            : subscriptions_(), senders_(), senderService_(), senderThread_(), mutex_(), senderThreadStarted_(false) {            
        }
        ~MulticastComponentImpl() {
            bool b;
            {
                std::lock_guard<std::mutex> _(mutex_);
                b = senderThreadStarted_;
            }
            if (b) {
                senderService_.stop();
                senderThread_.join();
            }
        }
        void addSubscriptionClient(ConnectionLocator const &locator,
            std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> const &topic,
            std::function<void(basic::ByteDataWithTopic &&)> client,
            std::optional<WireToUserHook> wireToUserHook) {
            auto *p = getOrStartSubscription(locator);
            p->addSubscription(topic, client, wireToUserHook);
        }
        std::function<void(basic::ByteDataWithTopic &&, int)> getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
            auto *p = getOrStartSender(locator);
            if (userToWireHook) {
                auto hook = userToWireHook->hook;
                return [p,hook](basic::ByteDataWithTopic &&data, int ttl) {
                    auto w = hook(basic::ByteData {std::move(data.content)});
                    p->publish({std::move(data.topic), std::move(w.content)}, ttl);
                };
            } else {
                return [p](basic::ByteDataWithTopic &&data, int ttl) {
                    p->publish(std::move(data), ttl);
                };
            }
        }
    };

    MulticastComponent::MulticastComponent() : impl_(std::make_unique<MulticastComponentImpl>()) {}
    MulticastComponent::~MulticastComponent() {}
    void MulticastComponent::multicast_addSubscriptionClient(ConnectionLocator const &locator,
        std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        impl_->addSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    std::function<void(basic::ByteDataWithTopic &&, int)> MulticastComponent::multicast_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook);
    }
} } } } }