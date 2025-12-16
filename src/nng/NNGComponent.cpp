#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cstring>
#include <sstream>
#include <unordered_map>
#include <nngpp/nngpp.h>
#include <nngpp/socket.h>
#include <nngpp/protocol/pub0.h>
#include <nngpp/protocol/sub0.h>

#include <tm_kit/transport/nng/NNGComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace nng {
    class NNGComponentImpl {
    private:
        class OneNNGSubscription {
        private:
            ConnectionLocator locator_;
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

            void run(ConnectionLocator const &locator) {
                auto sock = ::nng::sub::open();
                nng_setopt(sock.get(), NNG_OPT_SUB_SUBSCRIBE, "", 0);
                sock.set_opt_ms(NNG_OPT_RECVTIMEO, 1000);

                std::ostringstream oss;
                if (locator.host() == "ipc") {
                    oss << locator.host() << "://" << locator.identifier();
                } else {
                    oss << "tcp://" << locator.host() << ":" << locator.port();
                }
                std::string locatorStr = oss.str();

                while (running_) {
                    bool notConnected = true;
                    int reconnectWaitMillis = 1;
                    while (running_ && notConnected) {
                        try {
                            sock.dial(locatorStr.c_str());
                            notConnected = false;
                        } catch (std::exception const &) {
                            notConnected = true;
                            std::this_thread::sleep_for(std::chrono::milliseconds(reconnectWaitMillis));
                            if (reconnectWaitMillis < 1024) {
                                reconnectWaitMillis *= 2;
                            }
                        }
                    }
                    if (!running_) {
                        break;
                    }
            
                    int missedCount = 0;
                    while (running_ && missedCount < 10) {
                        char *data = nullptr;
                        std::size_t sz;
                        int r = nng_recv(sock.get(), &data, &sz, NNG_FLAG_ALLOC);
                        if (r == NNG_ETIMEDOUT) {
                            ++missedCount;
                            if (data) {
                                nng_free(data, sz);
                            }
                            continue;
                        } else if (r != 0) {
                            break;
                        } 

                        if (!data) {
                            continue;
                        }

                        auto parseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithTopic>::apply(std::string_view {data, sz}, 0);
                        nng_free(data, sz);

                        if (!running_) {
                            break;
                        }
                        if (parseRes && std::get<1>(*parseRes) == sz) {
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
                }
                sock.~socket();
            }
        public:
            OneNNGSubscription(ConnectionLocator const &locator) 
                : locator_(locator)
                , noFilterClients_(), stringMatchClients_(), regexMatchClients_()
                , mutex_(), th_(), running_(true)
            {
                th_ = std::thread(&OneNNGSubscription::run, this, locator);
            }
            ~OneNNGSubscription() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
            }
            void addSubscription(
                uint32_t id
                , std::variant<NNGComponent::NoTopicSelection, std::string, std::regex> const &topic
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
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneNNGSubscription>> subscriptions_;
        
        class OneNNGSender {
        private:
            std::mutex mutex_;
            ::nng::socket sock_;
        public:
            OneNNGSender(ConnectionLocator const &locator)
                : mutex_(), sock_(::nng::pub::open())
            {
                std::ostringstream oss;
                if (locator.host() == "ipc") {
                    oss << locator.host() << "://" << locator.identifier();
                } else {
                    oss << "tcp://*:" << locator.port();
                }
                sock_.listen(oss.str().c_str());
            }
            ~OneNNGSender() {
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    sock_.~socket();
                }
            }
            void publish(basic::ByteDataWithTopic &&data) {    
                auto v = basic::bytedata_utils::RunCBORSerializer<basic::ByteDataWithTopic>::apply(data);

                {
                    std::lock_guard<std::mutex> _(mutex_);
                    sock_.send(
                        ::nng::view(reinterpret_cast<char const *>(v.data()), v.size())
                        , ::nng::flag::nonblock
                    );
                } 
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneNNGSender>> senders_;

        std::mutex mutex_;

        uint32_t counter_;
        std::unordered_map<uint32_t, OneNNGSubscription *> idToSubscriptionMap_;
        std::mutex idMutex_;

        OneNNGSubscription *getOrStartSubscription(ConnectionLocator const &d) {
            ConnectionLocator hostAndPortAndIdentifier {d.host(), d.port(), "", "", d.identifier()};
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = subscriptions_.find(hostAndPortAndIdentifier);
            if (iter == subscriptions_.end()) {
                iter = subscriptions_.insert({hostAndPortAndIdentifier, std::make_unique<OneNNGSubscription>(hostAndPortAndIdentifier)}).first;
            }
            return iter->second.get();
        }
        void potentiallyStopSubscription(OneNNGSubscription *p) {
            std::lock_guard<std::mutex> _(mutex_);
            if (p->checkWhetherNeedsToStop()) {
                subscriptions_.erase(p->locator());
            }
        }
        OneNNGSender *getOrStartSender(ConnectionLocator const &d) {
            ConnectionLocator hostAndPortAndIdentifier {d.host(), d.port(), "", "", d.identifier()};    
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = senders_.find(hostAndPortAndIdentifier);
            if (iter == senders_.end()) {
                iter = senders_.insert({hostAndPortAndIdentifier, std::make_unique<OneNNGSender>(hostAndPortAndIdentifier)}).first;
            }
            return iter->second.get();
        }
    public:
        NNGComponentImpl()
            : subscriptions_(), senders_(), mutex_()
            , counter_(0), idToSubscriptionMap_(), idMutex_()
        {            
        }
        ~NNGComponentImpl() {
            std::lock_guard<std::mutex> _(mutex_);
            subscriptions_.clear();
            senders_.clear();
        }
        uint32_t addSubscriptionClient(ConnectionLocator const &locator,
            std::variant<NNGComponent::NoTopicSelection, std::string, std::regex> const &topic,
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
            OneNNGSubscription *p = nullptr;
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

    NNGComponent::NNGComponent() : impl_(std::make_unique<NNGComponentImpl>()) {}
    NNGComponent::~NNGComponent() = default;
    uint32_t NNGComponent::nng_addSubscriptionClient(ConnectionLocator const &locator,
        std::variant<NNGComponent::NoTopicSelection, std::string, std::regex> const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    void NNGComponent::nng_removeSubscriptionClient(uint32_t id) {
        impl_->removeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&)> NNGComponent::nng_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook);
    }
    std::unordered_map<ConnectionLocator, std::thread::native_handle_type> NNGComponent::nng_threadHandles() {
        return impl_->threadHandles();
    }
} } } } }