#include <thread>
#include <mutex>
#include <cstring>
#include <unordered_map>

#ifdef _MSC_VER
#include <boost/interprocess/managed_windows_shared_memory.hpp>
#else
#include <boost/interprocess/managed_shared_memory.hpp>
#endif
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>

#include <boost/date_time/posix_time/posix_time.hpp>

#include <tm_kit/transport/shared_memory_broadcast/SharedMemoryBroadcastComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace shared_memory_broadcast {
        
    struct SharedMemoryItem {
        std::ptrdiff_t dataOffsetFromClientList;
        std::atomic<std::ptrdiff_t> nextOffsetFromClientList;
        SharedMemoryItem() : dataOffsetFromClientList(0), nextOffsetFromClientList(0) {}
    };

    struct SharedMemoryBroadcastClientRecord {
        std::atomic<int64_t> latestHeartbeat;
        std::atomic<std::ptrdiff_t> tailOffsetFromClientList;
        boost::interprocess::interprocess_condition cond;

        SharedMemoryBroadcastClientRecord()
            : latestHeartbeat(static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()))
              , tailOffsetFromClientList(0)
              , cond()
        {}
    };

    struct SharedMemoryBroadcastClientListItem {
        char id[37];
        std::atomic<bool> active;
        std::atomic<std::ptrdiff_t> next;

        SharedMemoryBroadcastClientListItem() : active(true), next(0) {
            std::memset(id, 0, 37);
        }
    };

    class SharedMemoryBroadcastComponentImpl {
    private:
        class OneSharedMemoryBroadcastSubscription {
        private:
#ifdef _MSC_VER
            boost::interprocess::managed_windows_shared_memory mem_;
#else
            boost::interprocess::managed_shared_memory mem_;
#endif
            ConnectionLocator locator_;
            std::string recordIDInSharedMem_;
            SharedMemoryBroadcastClientListItem *clientListHead_;
            SharedMemoryItem *heads_;
            std::ptrdiff_t headOffsets_[2];
            int headIdx_;
            SharedMemoryBroadcastClientRecord *recordInSharedMem_;
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

            void handleData(basic::ByteDataWithTopic &&data) {
                if (!running_) {
                    return;
                }
                std::lock_guard<std::mutex> _(mutex_);

                if (noFilterClients_.size()+stringMatchClients_.size()+regexMatchClients_.size() == 1) {
                    if (noFilterClients_.size() == 1) {
                        callClient(noFilterClients_.front(), std::move(data));
                    } else if (stringMatchClients_.size() == 1) {
                        if (data.topic == std::get<0>(stringMatchClients_.front())) {
                            callClient(std::get<1>(stringMatchClients_.front()), std::move(data));
                        }
                    } else {
                        if (std::regex_match(data.topic, std::get<0>(regexMatchClients_.front()))) {
                            callClient(std::get<1>(regexMatchClients_.front()), std::move(data));
                        }
                    }
                } else {
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
            void run() {
                std::size_t dataSize = 0;
                char *data = nullptr;
                boost::interprocess::interprocess_mutex m;

                while (running_) {
                    boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(m);
                    recordInSharedMem_->cond.timed_wait(lock, boost::posix_time::second_clock::universal_time()+boost::posix_time::seconds(1));
                    if (!running_) {
                        lock.unlock();
                        break;
                    }
                    recordInSharedMem_->latestHeartbeat.store(static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()));
                    std::ptrdiff_t newV = headOffsets_[1-headIdx_];
                    int oldHeadIdx = headIdx_;
                    headIdx_ = 1-headIdx_;
                    while (true) {
                        std::ptrdiff_t oldV = recordInSharedMem_->tailOffsetFromClientList.load();
                        if (recordInSharedMem_->tailOffsetFromClientList.compare_exchange_strong(
                            oldV, newV
                        )) {
                            break;
                        }
                    }
                    lock.unlock();

                    auto *p = &heads_[oldHeadIdx];
                    auto n = p->nextOffsetFromClientList.load();
                    if (n != 0) {
                        p = reinterpret_cast<SharedMemoryItem *>(
                            reinterpret_cast<char *>(clientListHead_)+n
                        );
                        while (true) {
                            if (p->dataOffsetFromClientList != 0) {
                                data = reinterpret_cast<char *>(clientListHead_)+p->dataOffsetFromClientList;
                                std::memcpy(&dataSize, data, sizeof(std::size_t));
                                auto parseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithTopic>::apply(std::string_view {data+sizeof(std::size_t), dataSize}, 0);
                                if (parseRes && std::get<1>(*parseRes) == dataSize) {
                                    handleData(std::move(std::get<0>(*parseRes)));
                                }
                                mem_.destroy_ptr(data);
                            }
                            n = p->nextOffsetFromClientList.load();
                            mem_.destroy_ptr(p);
                            if (n == 0) {
                                break;
                            }
                            p = reinterpret_cast<SharedMemoryItem *>(
                                reinterpret_cast<char *>(clientListHead_)+n
                            );
                        }
                    }
                    heads_[oldHeadIdx].nextOffsetFromClientList.store(0);
                }
            }
        public:
            OneSharedMemoryBroadcastSubscription(ConnectionLocator const &locator, std::size_t memSize) 
                : 
                    mem_(
                        boost::interprocess::open_or_create
                        , locator.identifier().c_str()
                        , memSize
                    )
                    , locator_(locator)
                    , recordIDInSharedMem_(boost::lexical_cast<std::string>(boost::uuids::random_generator()()))
                    , clientListHead_(
                        mem_.find_or_construct<SharedMemoryBroadcastClientListItem>
                            ("client_list_head")()
                    )
                    , heads_(
                        mem_.construct<SharedMemoryItem>(boost::interprocess::anonymous_instance)[2]()
                    )
                    , headOffsets_()
                    , headIdx_(0)
                    , recordInSharedMem_(
                        mem_.find_or_construct<SharedMemoryBroadcastClientRecord>
                            (recordIDInSharedMem_.c_str())()
                    )
                    , noFilterClients_()
                    , stringMatchClients_()
                    , regexMatchClients_()
                    , mutex_()
                    , th_()
                    , running_(true)
            {
                for (auto ii=0; ii<2; ++ii) {
                    headOffsets_[ii] = reinterpret_cast<char *>(&heads_[ii])-reinterpret_cast<char *>(clientListHead_);
                }
                recordInSharedMem_->tailOffsetFromClientList = headOffsets_[0];

                SharedMemoryBroadcastClientListItem *clientItem =
                    mem_.construct<SharedMemoryBroadcastClientListItem>(boost::interprocess::anonymous_instance)();
                std::memcpy(clientItem->id, recordIDInSharedMem_.data(), 36);
                clientItem->id[36] = '\0';

                auto *p = clientListHead_;
                while (true) {
                    while (true) {
                        auto n = p->next.load();
                        if (n == 0) {
                            break;
                        }
                        p = reinterpret_cast<SharedMemoryBroadcastClientListItem *>(
                            reinterpret_cast<char *>(p)+n
                        );
                    }
                    auto diff = reinterpret_cast<char *>(clientItem)-reinterpret_cast<char *>(p);
                    std::ptrdiff_t expected = 0;
                    if (p->next.compare_exchange_strong(
                        expected, diff
                    )) {
                        break;
                    }
                }    
                th_ = std::thread(&OneSharedMemoryBroadcastSubscription::run, this);
                th_.detach();  
            }
            ~OneSharedMemoryBroadcastSubscription() {
                running_ = false;
                try {
                    th_.join();
                } catch (std::system_error const &) {
                }
                recordInSharedMem_->latestHeartbeat.store(0);
                mem_.destroy_ptr(recordInSharedMem_);
                for (int ii=0; ii<2; ++ii) {
                    auto *p = &heads_[ii];
                    auto n = p->nextOffsetFromClientList.load();
                    if (n == 0) {
                        break;
                    }
                    p = reinterpret_cast<SharedMemoryItem *>(
                        reinterpret_cast<char *>(clientListHead_)+n
                    );
                    n = p->nextOffsetFromClientList.load();
                    while (true) {
                        if (p->dataOffsetFromClientList != 0) {
                            auto *data = reinterpret_cast<char *>(clientListHead_)+p->dataOffsetFromClientList;
                            mem_.destroy_ptr(data);
                        }
                        mem_.destroy_ptr(p);
                        if (n == 0) {
                            break;
                        }
                        p = reinterpret_cast<SharedMemoryItem *>(
                            reinterpret_cast<char *>(clientListHead_)+n
                        );
                        n = p->nextOffsetFromClientList.load();
                    }
                }
                mem_.destroy_ptr(heads_);
            }
            ConnectionLocator const &locator() const {
                return locator_;
            }
            void addSubscription(
                uint32_t id
                , std::variant<SharedMemoryBroadcastComponent::NoTopicSelection, std::string, std::regex> const &topic
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
        };
        std::unordered_map<ConnectionLocator, std::unique_ptr<OneSharedMemoryBroadcastSubscription>> subscriptions_;
        
        class OneSharedMemoryBroadcastSender {
        private:
#ifdef _MSC_VER
            boost::interprocess::managed_windows_shared_memory mem_;
#else
            boost::interprocess::managed_shared_memory mem_;
#endif
            ConnectionLocator locator_;
            SharedMemoryBroadcastClientListItem *clientListHead_;

            void releaseClientData(SharedMemoryBroadcastClientListItem *p) {
                auto *clientRec = mem_.find<SharedMemoryBroadcastClientRecord>(p->id).first;
                if (clientRec != nullptr) {
                    try {
                        auto *q = reinterpret_cast<SharedMemoryItem *>(
                            reinterpret_cast<char *>(clientListHead_)+clientRec->tailOffsetFromClientList.load()
                        );
                        auto n = q->nextOffsetFromClientList.load();
                        while (true) {
                            if (q->dataOffsetFromClientList != 0) {
                                auto *data = reinterpret_cast<char *>(clientListHead_)+q->dataOffsetFromClientList;
                                mem_.destroy_ptr(data);
                                mem_.destroy_ptr(q);
                            }
                            if (n == 0) {
                                break;
                            }
                            q = reinterpret_cast<SharedMemoryItem *>(
                                reinterpret_cast<char *>(clientListHead_)+n
                            );
                            n = q->nextOffsetFromClientList.load();
                        }
                        //It is not a good idea to try release clientRec itself
                        //because of the ipc object involved, so we just leave it be
                        mem_.destroy_ptr(p);
                    } catch (...) {}
                } else {
                    try {
                        mem_.destroy_ptr(p);
                    } catch (...) {}
                }
            }
        public:
            OneSharedMemoryBroadcastSender(ConnectionLocator const &locator, std::size_t memSize)
                : 
                    mem_(
                        boost::interprocess::open_or_create
                        , locator.identifier().c_str()
                        , memSize
                    )
                    , locator_(locator)
                    , clientListHead_(
                        mem_.find_or_construct<SharedMemoryBroadcastClientListItem>
                            ("client_list_head")()
                    )
            {
            }
            ~OneSharedMemoryBroadcastSender() {
            }
            void publish(basic::ByteDataWithTopic &&data) {
                auto *p = clientListHead_;
                if (p->next.load() == 0) {
                    return;
                }

                auto now = static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
                std::string v = basic::bytedata_utils::RunCBORSerializer<basic::ByteDataWithTopic>::apply(data);
                std::size_t vSize = v.length();

                bool hasInactive = false;

                while (true) {
                    auto n = p->next.load();
                    if (n == 0) {
                        break;
                    }
                    p = reinterpret_cast<SharedMemoryBroadcastClientListItem *>(
                            reinterpret_cast<char *>(p)+n
                        );
                    if (!(p->active.load())) {
                        hasInactive = true;
                        continue;
                    }
                    auto *clientRec = mem_.find<SharedMemoryBroadcastClientRecord>(p->id).first;
                    if (!clientRec) {
                        p->active.store(false);
                        hasInactive = true;
                        continue;
                    }
                    if (clientRec->latestHeartbeat.load()+3000 < now) {
                        //stale client
                        p->active.store(false);
                        hasInactive = true;
                    } else {
                        char *dataP = mem_.construct<char>(boost::interprocess::anonymous_instance)[vSize+sizeof(std::size_t)]();
                        std::memcpy(dataP, &vSize, sizeof(std::size_t));
                        std::memcpy(dataP+sizeof(std::size_t), v.data(), vSize);
                        std::ptrdiff_t offset = dataP-reinterpret_cast<char *>(clientListHead_);
                        SharedMemoryItem *newItem = mem_.construct<SharedMemoryItem>(boost::interprocess::anonymous_instance)();
                        newItem->dataOffsetFromClientList = offset;
                        newItem->nextOffsetFromClientList = 0;
                        offset = reinterpret_cast<char *>(newItem)-reinterpret_cast<char *>(clientListHead_);
                        auto *tail = reinterpret_cast<SharedMemoryItem *>(
                            reinterpret_cast<char *>(clientListHead_)+clientRec->tailOffsetFromClientList.load()
                        );
                        while (true) {
                            while (true) {
                                auto n = tail->nextOffsetFromClientList.load();
                                if (n == 0) {
                                    break;
                                }
                                std::ptrdiff_t old = clientRec->tailOffsetFromClientList.load();
                                if (!clientRec->tailOffsetFromClientList.compare_exchange_strong(
                                    old, n
                                )) {
                                    continue;
                                }
                                tail = reinterpret_cast<SharedMemoryItem *>(
                                    reinterpret_cast<char *>(clientListHead_)+n
                                );
                            }
                            n = 0;
                            if (tail->nextOffsetFromClientList.compare_exchange_strong(
                                n, offset
                            )) {
                                break;
                            }
                        }
                        clientRec->cond.notify_one();
                    }
                }

                if (hasInactive) {
                    p = clientListHead_;
                    auto *q = p;
                    std::ptrdiff_t n;
                    if ((n = p->next.load()) != 0) {
                        q = reinterpret_cast<SharedMemoryBroadcastClientListItem *>(
                            reinterpret_cast<char *>(p)+n
                        );
                        while (true) {
                            if (!(q->active.load())) {
                                std::ptrdiff_t pNext = p->next.load();
                                std::ptrdiff_t qNext = q->next.load();
                                std::ptrdiff_t newVal;
                                if (qNext == 0) {
                                    newVal = 0;
                                } else {
                                    newVal = pNext+qNext;
                                }
                                if (p->next.compare_exchange_strong(
                                    pNext, newVal
                                )) {
                                    auto *oldQ = q;
                                    if (qNext == 0) {
                                        releaseClientData(oldQ);
                                        break;
                                    }
                                    q = reinterpret_cast<SharedMemoryBroadcastClientListItem *>(
                                        reinterpret_cast<char *>(q)+qNext
                                    );
                                    releaseClientData(oldQ);
                                } else {
                                    //somebody else is modifying, break
                                    break;
                                }
                            } else {
                                std::ptrdiff_t qNext = q->next.load();
                                if (qNext == 0) {
                                    break;
                                }
                                p = q;
                                q = reinterpret_cast<SharedMemoryBroadcastClientListItem *>(
                                        reinterpret_cast<char *>(q)+qNext
                                    );
                            }
                        }
                    }
                }
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneSharedMemoryBroadcastSender>> senders_;
        std::mutex mutex_;

        uint32_t counter_;
        std::unordered_map<uint32_t, OneSharedMemoryBroadcastSubscription *> idToSubscriptionMap_;
        std::mutex idMutex_;

        OneSharedMemoryBroadcastSubscription *getOrStartSubscription(ConnectionLocator const &d) {
            ConnectionLocator idOnly {"", 0, "", "", d.identifier()};
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = subscriptions_.find(idOnly);
            if (iter == subscriptions_.end()) {
                auto memSize = std::stoul(d.query("size", std::to_string(4*1024*1024*1024L)));
                iter = subscriptions_.insert({idOnly, std::make_unique<OneSharedMemoryBroadcastSubscription>(idOnly, memSize)}).first;
            }
            return iter->second.get();
        }
        void potentiallyStopSubscription(OneSharedMemoryBroadcastSubscription *p) {
            std::lock_guard<std::mutex> _(mutex_);
            if (p->checkWhetherNeedsToStop()) {
                subscriptions_.erase(p->locator());
            }
        }
        OneSharedMemoryBroadcastSender *getOrStartSender(ConnectionLocator const &d) {
            ConnectionLocator idOnly {"", 0, "", "", d.identifier()};
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = senders_.find(idOnly);
            if (iter == senders_.end()) {
                auto memSize = std::stoul(d.query("size", std::to_string(4*1024*1024*1024L)));
                iter = senders_.insert({idOnly, std::make_unique<OneSharedMemoryBroadcastSender>(idOnly, memSize)}).first;
            }
            return iter->second.get();
        }
    public:
        SharedMemoryBroadcastComponentImpl()
            : subscriptions_(), senders_(), mutex_()
            , counter_(0), idToSubscriptionMap_(), idMutex_()
        {            
        }
        ~SharedMemoryBroadcastComponentImpl() {
            std::lock_guard<std::mutex> _(mutex_);
            subscriptions_.clear();
            senders_.clear();
        }
        uint32_t addSubscriptionClient(ConnectionLocator const &locator,
            std::variant<SharedMemoryBroadcastComponent::NoTopicSelection, std::string, std::regex> const &topic,
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
            OneSharedMemoryBroadcastSubscription *p = nullptr;
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
    };

    SharedMemoryBroadcastComponent::SharedMemoryBroadcastComponent() : impl_(std::make_unique<SharedMemoryBroadcastComponentImpl>()) {}
    SharedMemoryBroadcastComponent::~SharedMemoryBroadcastComponent() {}
    uint32_t SharedMemoryBroadcastComponent::shared_memory_broadcast_addSubscriptionClient(ConnectionLocator const &locator,
        std::variant<SharedMemoryBroadcastComponent::NoTopicSelection, std::string, std::regex> const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook) {
        return impl_->addSubscriptionClient(locator, topic, client, wireToUserHook);
    }
    void SharedMemoryBroadcastComponent::shared_memory_broadcast_removeSubscriptionClient(uint32_t id) {
        impl_->removeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&)> SharedMemoryBroadcastComponent::shared_memory_broadcast_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook);
    }
} } } } }