#include <thread>
#include <mutex>
#include <cstring>
#include <unordered_map>
#include <cstdlib>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>

#include <tm_kit/transport/multicast/MulticastComponent.hpp>
#include "InterfaceToIP.hpp"

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace multicast {
    
    enum class MulticastComponentTopicEncodingChoice {
        CBOR
        , BinaryAdHoc
    };

    MulticastComponentTopicEncodingChoice parseEncodingChoice(std::string const &s) {
        if (s == "binary" || s == "binary-adhoc") {
            return MulticastComponentTopicEncodingChoice::BinaryAdHoc;
        } else {
            return MulticastComponentTopicEncodingChoice::CBOR;
        }
    }

    class MulticastComponentImpl {
    private:
        class OneMulticastSubscription {
        private:
            MulticastComponentTopicEncodingChoice encodingChoice_;
            ConnectionLocator locator_;
            boost::asio::ip::udp::socket sock_;
            boost::asio::ip::udp::endpoint senderPoint_;
            boost::asio::ip::address mcastAddr_;
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
                    if (encodingChoice_ == MulticastComponentTopicEncodingChoice::CBOR) {
                        parseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithTopic>::apply(std::string_view {buffer_.data(), bytesReceived}, 0);
                    } else {
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
                    }
                    if (parseRes && std::get<1>(*parseRes) == bytesReceived) {
                        basic::ByteDataWithTopic data = std::move(std::get<0>(*parseRes));

                        std::lock_guard<std::mutex> _(mutex_);
                        
                        for (auto const &f : noFilterClients_) {
                            callClient(f, std::move(data));
                        }
                        for (auto const &f : stringMatchClients_) {
                            if (data.topic == std::get<0>(f)) {
                                callClient(std::get<1>(f), std::move(data));
                            }
                        }
                        for (auto const &f : regexMatchClients_) {
                            if (std::regex_match(data.topic, std::get<0>(f))) {
                                callClient(std::get<1>(f), std::move(data));
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
#if BOOST_VERSION >= 108700        
            OneMulticastSubscription(MulticastComponentTopicEncodingChoice encodingChoice, boost::asio::io_context *service, ConnectionLocator const &locator, std::string const &interface) 
#else
            OneMulticastSubscription(MulticastComponentTopicEncodingChoice encodingChoice, boost::asio::io_service *service, ConnectionLocator const &locator, std::string const &interface) 
#endif            
                : encodingChoice_(encodingChoice), locator_(locator), sock_(*service), senderPoint_(), mcastAddr_(), buffer_()
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
                    sock_.set_option(boost::asio::ip::udp::socket::reuse_address(true));
                    sock_.set_option(boost::asio::ip::udp::socket::receive_buffer_size(16*1024*1024));
                    //sock_.set_option(boost::asio::ip::multicast::enable_loopback(true));
                    sock_.bind(listenPoint);
                }
                
                {
#if BOOST_VERSION >= 108700   
                    mcastAddr_ = resolver.resolve(locator.host(), std::to_string(locator.port())).begin()->endpoint().address();                 
#else
                    boost::asio::ip::udp::resolver::query query(locator.host(), std::to_string(locator.port()));
                    mcastAddr_ = resolver.resolve(query)->endpoint().address();
#endif                    
                    if (interface != "") {
                        auto interfaceAddr = getAddressForInterface(interface);
                        if (interfaceAddr != "") {
                            sock_.set_option(boost::asio::ip::multicast::join_group(
                                mcastAddr_.to_v4()
#if BOOST_VERSION >= 108700   
                                , boost::asio::ip::make_address(interfaceAddr).to_v4()                             
#else
                                , boost::asio::ip::address::from_string(interfaceAddr).to_v4()
#endif                                
                            ));
                        } else {
                            sock_.set_option(boost::asio::ip::multicast::join_group(
                                mcastAddr_.to_v4()
                            ));
                        }
                    } else {
                        sock_.set_option(boost::asio::ip::multicast::join_group(
                            mcastAddr_.to_v4()
                        ));
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
            ~OneMulticastSubscription() {
                running_ = false;
                sock_.close();
            }
            ConnectionLocator const &locator() const {
                return locator_;
            }
            void addSubscription(
                uint32_t id
                , std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> const &topic
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
                    sock_.set_option(boost::asio::ip::multicast::leave_group(
                        mcastAddr_
                    ));
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
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneMulticastSubscription>> subscriptions_;

        class SenderBufferPool {
            static constexpr std::size_t CACHELINE_SIZE = 64;
            inline std::size_t align_up(std::size_t n, std::size_t alignment) {
                return (n + alignment - 1) & ~(alignment - 1);
            }
        public:
            struct Buffer {
                char* data;
            };

            struct BufferDeleter {
                SenderBufferPool *pool;
                BufferDeleter(SenderBufferPool *pool) : pool(pool) {}
                void operator()(Buffer *p) const noexcept { pool->deallocate(p); }
            };

            using BufferPtr = std::unique_ptr<Buffer, BufferDeleter>;

            SenderBufferPool(std::size_t chunkSize, std::size_t chunkCount)
                : chunkSize_(chunkSize)
                , chunkCount_(chunkCount)
            {
                std::size_t blockStride = align_up(chunkSize, CACHELINE_SIZE); // align to cacheline
                std::size_t totalSize = blockStride * chunkCount;
                void *ptr = aligned_alloc(CACHELINE_SIZE, totalSize);
                if (!ptr)
                    throw std::bad_alloc();
                storage_ = static_cast<char *>(ptr);
                blockStride_ = blockStride;
                for (std::size_t i = 0; i < chunkCount; ++i) {
                    freeList_.push_back(storage_ + i * blockStride_);
                }
            }

            ~SenderBufferPool()
            {
                free(storage_);
            }

            BufferPtr allocate()
            {
                std::lock_guard<std::mutex> _(mtx_);
                if (freeList_.empty())
                    return BufferPtr(nullptr, BufferDeleter(this));
                char *p = freeList_.back();
                freeList_.pop_back();
                return BufferPtr(new Buffer{p}, BufferDeleter(this));
            }

            void deallocate(Buffer *p)
            {
                if (!p->data) {
                    return;
                }
                std::lock_guard<std::mutex> _(mtx_);
                freeList_.push_back(p->data);
            }

        private:
            char *storage_ {nullptr};
            std::size_t chunkSize_;
            std::size_t chunkCount_;
            std::size_t blockStride_;
            std::vector<char *> freeList_;
            std::mutex mtx_;
        };

        class OneMulticastSender {
        private:
            constexpr static std::size_t SAFE_PAYLOAD_SIZE = 1200;  // avoid IP fragmentation
            MulticastComponentTopicEncodingChoice encodingChoice_;
            boost::asio::ip::udp::socket sock_;
            boost::asio::ip::udp::endpoint destination_;
#ifdef MULTICAST_COMPONENT_SENDER_USE_ZERO_COPY_BUFFER
            SenderBufferPool senderBufferPool_;
#endif
            std::mutex mutex_;
            int ttl_;

        public:
#if BOOST_VERSION >= 108700
            OneMulticastSender(MulticastComponentTopicEncodingChoice encodingChoice, boost::asio::io_context *service, ConnectionLocator const &locator, std::string const &interface)
#else
            OneMulticastSender(MulticastComponentTopicEncodingChoice encodingChoice, boost::asio::io_service *service, ConnectionLocator const &locator, std::string const &interface)
#endif            
                : encodingChoice_(encodingChoice)
                , sock_(*service)
                , destination_()
#ifdef MULTICAST_COMPONENT_SENDER_USE_ZERO_COPY_BUFFER
                , senderBufferPool_(SAFE_PAYLOAD_SIZE, 1024)
#endif
                , mutex_()
                , ttl_(0)
            {
                boost::asio::ip::udp::resolver resolver(*service);
                boost::system::error_code ec;
#if BOOST_VERSION >= 108700
                auto iter = resolver.resolve(locator.host(), std::to_string(locator.port()), ec);
#else
                boost::asio::ip::udp::resolver::query query(locator.host(), std::to_string(locator.port()));
                auto iter = resolver.resolve(query, ec);
#endif
                if (ec || iter == boost::asio::ip::udp::resolver::iterator()) {
                    throw std::runtime_error(std::string("OneMulticastSender resolve locator failed: ") + ec.message());
                }
                destination_ = *iter;

                sock_.open(destination_.protocol(), ec);
                if (ec) {
                    throw std::runtime_error(std::string("OneMulticastSender open socket failed: ") + ec.message());
                }
                sock_.set_option(boost::asio::ip::udp::socket::reuse_address(true));
                sock_.set_option(boost::asio::ip::udp::socket::send_buffer_size(16*1024*1024));
                //sock_.set_option(boost::asio::ip::multicast::enable_loopback(true));

                if (interface != "") {
                    auto interfaceAddr = getAddressForInterface(interface);
                    if (interfaceAddr != "") {
                        #if BOOST_VERSION >= 108700                            
                        auto addr = boost::asio::ip::make_address(interfaceAddr).to_v4();
#else
                        auto addr = boost::asio::ip::address::from_string(interfaceAddr).to_v4();
#endif
                        boost::asio::ip::udp::endpoint local(addr, 0);
                        sock_.bind(local, ec);
                        if (ec) {
                            throw std::runtime_error(std::string("OneMulticastSender bind local address failed: ") + ec.message());
                        }
                    }
                }

                if (interface != "") {
                    auto interfaceAddr = getAddressForInterface(interface);
                    //std::cerr << "interface " << interface << " has address '" << interfaceAddr << "'\n";
                    if (interfaceAddr != "") {
                        sock_.set_option(boost::asio::ip::multicast::outbound_interface(
#if BOOST_VERSION >= 108700                            
                            boost::asio::ip::make_address(interfaceAddr).to_v4()
#else
                            boost::asio::ip::address::from_string(interfaceAddr).to_v4()
#endif                            
                        ));
                    }
                }
            }
            ~OneMulticastSender() {
                sock_.close();
            }
            void publish(basic::ByteDataWithTopic &&data, int ttl) {
#ifdef MULTICAST_COMPONENT_SENDER_USE_ZERO_COPY_BUFFER
                auto buffer = senderBufferPool_.allocate();
                if (!buffer->data) {
                    // std::cerr << "sender buffer is full" << std::endl;
                    return;
                }
                char* pbuffer = buffer->data;
#else
                auto buffer = std::make_unique<std::string>();
                buffer->resize(SAFE_PAYLOAD_SIZE);
                char* pbuffer = buffer->data();
#endif
                std::size_t dataSize = 0;
                if (encodingChoice_ == MulticastComponentTopicEncodingChoice::CBOR) {
                    dataSize = basic::bytedata_utils::RunCBORSerializer<basic::ByteDataWithTopic>::calculateSize(data);
                    if (dataSize > SAFE_PAYLOAD_SIZE) {
                        // std::cerr << "data size " << dataSize << " is larger than safe payload size " << SAFE_PAYLOAD_SIZE << std::endl;
                        return;
                    }
                    if (basic::bytedata_utils::RunCBORSerializer<basic::ByteDataWithTopic>::apply(data, pbuffer) != dataSize) {
                        return;
                    }
                } else {
                    dataSize = sizeof(uint32_t)+data.topic.length()+data.content.length();
                    if (dataSize > SAFE_PAYLOAD_SIZE) {
                        // std::cerr << "data size " << dataSize << " is larger than safe payload size " << SAFE_PAYLOAD_SIZE << std::endl;
                        return;
                    }
                    uint32_t topicLen = (uint32_t) (data.topic.length());
                    std::memcpy(pbuffer, &topicLen, sizeof(uint32_t));
                    std::memcpy(pbuffer+sizeof(uint32_t), data.topic.data(), topicLen);
                    std::memcpy(pbuffer+sizeof(uint32_t)+topicLen, data.content.data(), data.content.length());
                }
                std::lock_guard<std::mutex> _(mutex_);
                if (ttl != ttl_) {
                    sock_.set_option(boost::asio::ip::multicast::hops(ttl));
                    ttl_ = ttl;
                }
                auto const_buffer = boost::asio::buffer(reinterpret_cast<const char *>(pbuffer), dataSize);
                sock_.async_send_to(
                    const_buffer
                    , destination_
                    , [buffer = std::move(buffer)](boost::system::error_code const &ec, std::size_t) mutable {
                        buffer.reset();
                    }
                );
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneMulticastSender>> senders_;
#if BOOST_VERSION >= 108700        
        boost::asio::io_context senderService_;
#else
        boost::asio::io_service senderService_;
#endif
        std::thread senderThread_;

        std::mutex mutex_;
        bool senderThreadStarted_;

        uint32_t counter_;
        std::unordered_map<uint32_t, OneMulticastSubscription *> idToSubscriptionMap_;
        std::mutex idMutex_;

        OneMulticastSubscription *getOrStartSubscription(ConnectionLocator const &d) {
            ConnectionLocator hostAndPort {d.host(), d.port()};
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = subscriptions_.find(hostAndPort);
            if (iter == subscriptions_.end()) {
                auto choice = parseEncodingChoice(d.query("envelop", "cbor"));
                auto interface = d.query("interface", "");
#if BOOST_VERSION >= 108700    
                std::unique_ptr<boost::asio::io_context> svc = std::make_unique<boost::asio::io_context>();            
#else
                std::unique_ptr<boost::asio::io_service> svc = std::make_unique<boost::asio::io_service>();
#endif                
                iter = subscriptions_.insert({hostAndPort, std::make_unique<OneMulticastSubscription>(choice, svc.get(), hostAndPort, interface)}).first;
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
        void potentiallyStopSubscription(OneMulticastSubscription *p) {
            std::lock_guard<std::mutex> _(mutex_);
            if (p->checkWhetherNeedsToStop()) {
                subscriptions_.erase(p->locator());
            }
        }
        OneMulticastSender *getOrStartSender(ConnectionLocator const &d) {
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
                auto interface = d.query("interface", "");
                iter = senders_.insert({hostAndPort, std::make_unique<OneMulticastSender>(choice, &senderService_, hostAndPort, interface)}).first;
            }
            return iter->second.get();
        }
    public:
        MulticastComponentImpl()
            : subscriptions_(), senders_(), senderService_(), senderThread_(), mutex_(), senderThreadStarted_(false)
            , counter_(0), idToSubscriptionMap_(), idMutex_()
        {            
        }
        ~MulticastComponentImpl() {
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
            std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> const &topic,
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
            OneMulticastSubscription *p = nullptr;
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

    MulticastComponent::MulticastComponent() : impl_(std::make_unique<MulticastComponentImpl>()) {}
    MulticastComponent::~MulticastComponent() {}
    uint32_t MulticastComponent::multicast_addSubscriptionClient(ConnectionLocator const &locator,
        std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    void MulticastComponent::multicast_removeSubscriptionClient(uint32_t id) {
        impl_->removeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&, int)> MulticastComponent::multicast_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook);
    }
    std::unordered_map<ConnectionLocator, std::thread::native_handle_type> MulticastComponent::multicast_threadHandles() {
        return impl_->threadHandles();
    }
} } } } }
