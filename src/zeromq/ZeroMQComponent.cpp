#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cstring>
#include <sstream>
#include <unordered_map>
#if __has_include(<cppzmq/zmq.hpp>)
    #include <cppzmq/zmq.hpp>
#else
    #include <zmq.hpp>
#endif

#include <tm_kit/transport/zeromq/ZeroMQComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace zeromq {
    class ZeroMQComponentImpl {
    private:
        class OneZeroMQSubscription {
        private:
            ConnectionLocator locator_;
            std::array<char, 16*1024*1024> buffer_;
            struct ClientCB {
                uint32_t id;
                std::function<void(basic::ByteDataWithTopic &&)> cb;
                std::optional<WireToUserHook> hook;
            };
            std::vector<ClientCB> noFilterClients_;
            std::vector<std::tuple<std::string, ClientCB>> stringMatchClients_;
            std::vector<std::tuple<std::regex, ClientCB>> regexMatchClients_;
            std::mutex mutex_;
            std::thread th_;
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

            void run(ConnectionLocator const &locator, zmq::context_t *p_ctx) {
                zmq::socket_t sock(*p_ctx, zmq::socket_type::sub);
                sock.set(zmq::sockopt::rcvtimeo, 1000);

                std::ostringstream oss;
                if (locator.host() == "inproc" || locator.host() == "ipc") {
                    oss << locator.host() << "://" << locator.identifier();
                } else {
                    oss << "tcp://" << locator.host() << ":" << locator.port();
                }
                sock.connect(oss.str());
                sock.set(zmq::sockopt::subscribe, "");

                while (running_) {
                    auto res = sock.recv(
                        zmq::mutable_buffer(buffer_.data(), 16*1024*1024)
                    );
                    
                    if (!running_) {
                        break;
                    }

                    if (!res) {
                        continue;
                    }
                    if (res->truncated()) {
                        continue;
                    }

                    if (!running_) {
                        break;
                    }
                    
                    auto parseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithTopic>::apply(std::string_view {buffer_.data(), res->size}, 0);
                    if (parseRes && std::get<1>(*parseRes) == res->size) {
                        basic::ByteDataWithTopic data = std::move(std::get<0>(*parseRes));

                        std::lock_guard<std::mutex> _(mutex_);
                        
                        for (auto const &f : noFilterClients_) {
                            callClient(f, basic::ByteDataWithTopic {data});
                        }
                        for (auto const &f : stringMatchClients_) {
                            if (data.topic == std::get<0>(f)) {
                                callClient(std::get<1>(f), basic::ByteDataWithTopic {data});
                            }
                        }
                        for (auto const &f : regexMatchClients_) {
                            if (std::regex_match(data.topic, std::get<0>(f))) {
                                callClient(std::get<1>(f), basic::ByteDataWithTopic {data});
                            }
                        }
                    }  
                }

                sock.close();
            }
        public:
            OneZeroMQSubscription(ConnectionLocator const &locator, zmq::context_t *p_ctx) 
                : locator_(locator), buffer_()
                , noFilterClients_(), stringMatchClients_(), regexMatchClients_()
                , mutex_(), th_(), running_(true)
            {
                th_ = std::thread(&OneZeroMQSubscription::run, this, locator, p_ctx);
            }
            ~OneZeroMQSubscription() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
            }
            void addSubscription(
                uint32_t id
                , std::variant<ZeroMQComponent::NoTopicSelection, std::string, std::regex> const &topic
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
            ConnectionLocator const &locator() const {
                return locator_;
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneZeroMQSubscription>> subscriptions_;
        
        class OneZeroMQSender {
        private:
            std::mutex mutex_;
            zmq::socket_t sock_;
        public:
            OneZeroMQSender(ConnectionLocator const &locator, zmq::context_t *p_ctx)
                : mutex_(), sock_(*p_ctx, zmq::socket_type::pub)
            {
                std::ostringstream oss;
                if (locator.host() == "inproc" || locator.host() == "ipc") {
                    oss << locator.host() << "://" << locator.identifier();
                } else {
                    oss << "tcp://*:" << locator.port();
                }
                sock_.bind(oss.str());
            }
            ~OneZeroMQSender() {
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    sock_.close();
                }
            }
            void publish(basic::ByteDataWithTopic &&data) {    
                auto v = basic::bytedata_utils::RunCBORSerializer<basic::ByteDataWithTopic>::apply(data);

                {
                    std::lock_guard<std::mutex> _(mutex_);
                    sock_.send(
                        zmq::const_buffer(reinterpret_cast<char const *>(v.data()), v.size())
                        , zmq::send_flags::dontwait
                    );
                } 
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneZeroMQSender>> senders_;

        std::mutex mutex_;
        zmq::context_t ctx_;

        uint32_t counter_;
        std::unordered_map<uint32_t, OneZeroMQSubscription *> idToSubscriptionMap_;
        std::mutex idMutex_;

        OneZeroMQSubscription *getOrStartSubscription(ConnectionLocator const &d) {
            ConnectionLocator hostAndPortAndIdentifier {d.host(), d.port(), "", "", d.identifier()};
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = subscriptions_.find(hostAndPortAndIdentifier);
            if (iter == subscriptions_.end()) {
                iter = subscriptions_.insert({hostAndPortAndIdentifier, std::make_unique<OneZeroMQSubscription>(hostAndPortAndIdentifier, &ctx_)}).first;
            }
            return iter->second.get();
        }
        void potentiallyStopSubscription(OneZeroMQSubscription *p) {
            std::lock_guard<std::mutex> _(mutex_);
            if (p->checkWhetherNeedsToStop()) {
                subscriptions_.erase(p->locator());
            }
        }
        OneZeroMQSender *getOrStartSender(ConnectionLocator const &d) {
            ConnectionLocator hostAndPortAndIdentifier {d.host(), d.port(), "", "", d.identifier()};
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = senders_.find(hostAndPortAndIdentifier);
            if (iter == senders_.end()) {
                iter = senders_.insert({hostAndPortAndIdentifier, std::make_unique<OneZeroMQSender>(hostAndPortAndIdentifier, &ctx_)}).first;
            }
            return iter->second.get();
        }
    public:
        ZeroMQComponentImpl()
            : subscriptions_(), senders_(), mutex_(), ctx_()
            , counter_(0), idToSubscriptionMap_(), idMutex_()
        {            
        }
        ~ZeroMQComponentImpl() {
            std::lock_guard<std::mutex> _(mutex_);
            subscriptions_.clear();
            senders_.clear();
        }
        uint32_t addSubscriptionClient(ConnectionLocator const &locator,
            std::variant<ZeroMQComponent::NoTopicSelection, std::string, std::regex> const &topic,
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
            OneZeroMQSubscription *p = nullptr;
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
            for (auto &item : subscriptions_) {
                retVal[item.first] = item.second->getThreadHandle();
            }
            return retVal;
        }
    };

    ZeroMQComponent::ZeroMQComponent() : impl_(std::make_unique<ZeroMQComponentImpl>()) {}
    ZeroMQComponent::~ZeroMQComponent() = default;
    uint32_t ZeroMQComponent::zeroMQ_addSubscriptionClient(ConnectionLocator const &locator,
        std::variant<ZeroMQComponent::NoTopicSelection, std::string, std::regex> const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    void ZeroMQComponent::zeroMQ_removeSubscriptionClient(uint32_t id) {
        impl_->removeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&)> ZeroMQComponent::zeroMQ_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook);
    }
    std::unordered_map<ConnectionLocator, std::thread::native_handle_type> ZeroMQComponent::zeromq_threadHandles() {
        return impl_->threadHandles();
    }
} } } } }
