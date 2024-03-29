#ifndef TM_KIT_TRANSPORT_LOCK_FREE_IN_BOOST_SHARED_MEMORY_SHARED_CHAIN_HPP_
#define TM_KIT_TRANSPORT_LOCK_FREE_IN_BOOST_SHARED_MEMORY_SHARED_CHAIN_HPP_

#include <tm_kit/basic/simple_shared_chain/ChainReader.hpp>
#include <tm_kit/basic/simple_shared_chain/ChainWriter.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>
#include <atomic>
#include <ctime>

#ifdef _MSC_VER
#include <boost/interprocess/managed_windows_shared_memory.hpp>
#else
#include <boost/interprocess/managed_shared_memory.hpp>
#endif
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace lock_free_in_memory_shared_chain {
    struct SharedMemoryChainComponent {};

    class LockFreeInBoostSharedMemoryChainException : public std::runtime_error {
    public:
        LockFreeInBoostSharedMemoryChainException(std::string const &s) : std::runtime_error(s) {}
    };

    enum class BoostSharedMemoryChainFastRecoverSupport {
        ByName, ByOffset
    };

    template <class T, bool ForceSeparate=false>
    using ActualNodeData = std::conditional_t<(!ForceSeparate && std::is_trivially_copyable_v<T>),T,std::ptrdiff_t>;
   
    template <class T, bool ForceSeparate=false>
    struct BoostSharedMemoryStorageItem {
        ActualNodeData<T,ForceSeparate> data;
        std::atomic<std::ptrdiff_t> next;
        BoostSharedMemoryStorageItem() : data(), next(0) {
        }
        BoostSharedMemoryStorageItem(ActualNodeData<T,ForceSeparate> &&d) : data(std::move(d)), next(0) {
        }
    };
    
    template <class T, bool ForceSeparate=false>
    using ParsedNodeData = std::conditional_t<(!ForceSeparate && std::is_trivially_copyable_v<T>),basic::VoidStruct,std::optional<T>>;
    template <class T, BoostSharedMemoryChainFastRecoverSupport FRS, bool ForceSeparate=false>
    struct BoostSharedMemoryChainItem {};
    template <class T, bool ForceSeparate>
    struct BoostSharedMemoryChainItem<T, BoostSharedMemoryChainFastRecoverSupport::ByName, ForceSeparate> {
        BoostSharedMemoryStorageItem<T,ForceSeparate> *ptr;
        std::string id;
        ParsedNodeData<T,ForceSeparate> parsed;
        T const *actualData() const {
            if constexpr (!ForceSeparate && std::is_trivially_copyable_v<T>) {
                if (ptr) {
                    return &(ptr->data);
                } else {
                    return nullptr;
                }
            } else {
                if (parsed) {
                    return &(*parsed);
                } else {
                    return nullptr;
                }
            }
        }
    };
    template <class T, bool ForceSeparate>
    struct BoostSharedMemoryChainItem<T, BoostSharedMemoryChainFastRecoverSupport::ByOffset, ForceSeparate> {
        BoostSharedMemoryStorageItem<T, ForceSeparate> *ptr;
        std::ptrdiff_t offset;
        ParsedNodeData<T, ForceSeparate> parsed;
        T const *actualData() const {
            if constexpr (!ForceSeparate && std::is_trivially_copyable_v<T>) {
                if (ptr) {
                    return &(ptr->data);
                } else {
                    return nullptr;
                }
            } else {
                if (parsed) {
                    return &(*parsed);
                } else {
                    return nullptr;
                }
            }
        }
    };

    struct BoostSharedMemoryChain_EmptyLock {
        BoostSharedMemoryChain_EmptyLock(
            std::string const &name
            , std::string const &suffix
#ifdef _MSC_VER
            , boost::interprocess::managed_windows_shared_memory *mem
#else
            , boost::interprocess::managed_shared_memory *mem
#endif
        ) {}
        static constexpr void lock() {}
        static constexpr void unlock() {}
    };
    struct BoostSharedMemoryChain_NamedMutexLock {
    private:
        boost::interprocess::named_mutex mutex_;
    public:
        BoostSharedMemoryChain_NamedMutexLock(
            std::string const &name
            , std::string const &suffix
#ifdef _MSC_VER
            , boost::interprocess::managed_windows_shared_memory *mem
#else
            , boost::interprocess::managed_shared_memory *mem
#endif
        ) : mutex_(
                boost::interprocess::open_or_create
                , (name+"_"+suffix).c_str()
            )
        {}
        void lock() {
            mutex_.lock();
        }
        void unlock() {
            mutex_.unlock();
        }
    };
    struct BoostSharedMemoryChain_SpinLock {
    private:
        uint32_t myPid_;
#ifdef _MSC_VER
        boost::interprocess::managed_windows_shared_memory *mem_;
#else
        boost::interprocess::managed_shared_memory *mem_;
#endif
        std::atomic<uint64_t> *numberDispenser_;
        std::atomic<uint64_t> *serviceRegistrar_;
        std::atomic<uint64_t> *ownerRegistrar_;
    public:
        BoostSharedMemoryChain_SpinLock(
            std::string const &name
            , std::string const &suffix
#ifdef _MSC_VER
            , boost::interprocess::managed_windows_shared_memory *mem
#else
            , boost::interprocess::managed_shared_memory *mem
#endif
        ) :
            myPid_(static_cast<uint32_t>(infra::pid_util::getpid()))
            , mem_(mem)
            , numberDispenser_(
                mem_->find_or_construct<std::atomic<uint64_t>>((name+"_"+suffix+"_number_dispenser").c_str())(0)
            )
            , serviceRegistrar_(
                mem_->find_or_construct<std::atomic<uint64_t>>((name+"_"+suffix+"_queue_registrar").c_str())(0)
            )
            , ownerRegistrar_(
                mem_->find_or_construct<std::atomic<uint64_t>>((name+"_"+suffix+"_owner_registrar").c_str())(0)
            )
        {}
        void lock() {
            static std::hash<std::thread::id> hasher;
            uint64_t myNumber = 0;
            bool acquired = false;
            uint16_t count = 0; 
            uint64_t lastOwner = 0;
            uint64_t myThread = static_cast<uint64_t>(hasher(std::this_thread::get_id()) & static_cast<uint64_t>(0xfffffffful));
            uint64_t myPidAndThread = ((static_cast<uint64_t>(myPid_) << 32) | myThread);
            while (true) {
                myNumber = numberDispenser_->fetch_add(1);
                acquired = false;
                count = 0;
                lastOwner = 0;
                while (true) {
                    uint64_t currentlyServing = serviceRegistrar_->load();
                    if (currentlyServing > myNumber) {
                        //we got skipped, start again
                        break;
                    } else if (currentlyServing == myNumber) {
                        //our turn
                        if (serviceRegistrar_->compare_exchange_strong(
                            currentlyServing, myNumber
                        )) {
                            uint64_t currentOwner = ownerRegistrar_->load();
                            if (ownerRegistrar_->compare_exchange_strong(
                                currentOwner, myPidAndThread
                            )) {
                                acquired = true;
                                break;
                            } else {
                                count = 0;
                                continue;
                            }
                            break;
                        }
                    } else {
                        //optionally check if the process in front has quit
                        ++count;
                        if (count >= 10000) {
                            uint64_t currentOwner = ownerRegistrar_->load();
                            if (lastOwner != currentOwner) {
                                lastOwner = currentOwner;
                                count = 0;
                                continue;
                            } else {
                                uint32_t currentOwnerPid = static_cast<uint32_t>(currentOwner >> 32);
                                if (currentOwnerPid != 0 && infra::pid_util::pidIsRunning(currentOwnerPid)) {
                                    count = 0;
                                    continue;
                                } else {
                                    if (serviceRegistrar_->compare_exchange_strong(
                                        currentlyServing, myNumber
                                    )) {
                                        if (ownerRegistrar_->compare_exchange_strong(
                                            currentOwner, myPidAndThread
                                        )) {
                                            acquired = true;
                                            break;
                                        } else {
                                            count = 0;
                                            continue;
                                        }
                                    } else {
                                        count = 0;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                if (acquired) {
                    break;
                }
            }
        }
        void unlock() {
            ownerRegistrar_->store(0);
            serviceRegistrar_->fetch_add(1);
        }
    };
    
    enum class BoostSharedMemoryChainExtraDataProtectionStrategy {
        DontSupportExtraData
        , LockFreeAndWasteMemory
        , MutexProtected
        , SpinLockProtected
        , Unsafe
    };

    template <BoostSharedMemoryChainExtraDataProtectionStrategy EDPS>
    struct IPCMutexWrapperResolver {
        using TheType = BoostSharedMemoryChain_EmptyLock;
    };
    template <>
    struct IPCMutexWrapperResolver<BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected> {
        using TheType = BoostSharedMemoryChain_NamedMutexLock;
    };
    template <>
    struct IPCMutexWrapperResolver<BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected> {
        using TheType = BoostSharedMemoryChain_SpinLock;
    };

    template <BoostSharedMemoryChainExtraDataProtectionStrategy EDPS>
    using IPCMutexWrapper = typename IPCMutexWrapperResolver<EDPS>::TheType;

    template <BoostSharedMemoryChainExtraDataProtectionStrategy EDPS>
    struct IPCMutexWrapperGuard {
    private:
        IPCMutexWrapper<EDPS> *wrapper_;
    public:
        IPCMutexWrapperGuard(IPCMutexWrapper<EDPS> *wrapper) : wrapper_(wrapper) {
            wrapper_->lock();
        }
        ~IPCMutexWrapperGuard() {
            wrapper_->unlock();
        }
    };

    enum class BoostSharedMemoryChainDataLockStrategy {
        None 
        , NamedMutex 
        , SpinLock
    };

    template <BoostSharedMemoryChainDataLockStrategy DLS>
    struct BoostSharedMemoryChainDataLockResolver {};

    template <>
    struct BoostSharedMemoryChainDataLockResolver<BoostSharedMemoryChainDataLockStrategy::None> {
        using TheType = BoostSharedMemoryChain_EmptyLock;
    };
    template <>
    struct BoostSharedMemoryChainDataLockResolver<BoostSharedMemoryChainDataLockStrategy::NamedMutex> {
        using TheType = BoostSharedMemoryChain_NamedMutexLock;
    };
    template <>
    struct BoostSharedMemoryChainDataLockResolver<BoostSharedMemoryChainDataLockStrategy::SpinLock> {
        using TheType = BoostSharedMemoryChain_SpinLock;
    };

    template <BoostSharedMemoryChainDataLockStrategy DLS>
    using BoostSharedMemoryChainDataLock = typename BoostSharedMemoryChainDataLockResolver<DLS>::TheType;

    template <
        class T
        , BoostSharedMemoryChainFastRecoverSupport FRS
        , BoostSharedMemoryChainExtraDataProtectionStrategy EDPS = BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData
        , bool ForceSeparate=false
        , BoostSharedMemoryChainDataLockStrategy DLS = BoostSharedMemoryChainDataLockStrategy::None
    >
    class LockFreeInBoostSharedMemoryChain {};

    template <class T, BoostSharedMemoryChainExtraDataProtectionStrategy EDPS, bool ForceSeparate, BoostSharedMemoryChainDataLockStrategy DLS>
    class LockFreeInBoostSharedMemoryChain<
        T
        , BoostSharedMemoryChainFastRecoverSupport::ByName
        , EDPS 
        , ForceSeparate
        , DLS
    > {
    private:
#ifdef _MSC_VER
        boost::interprocess::managed_windows_shared_memory mem_;
#else
        boost::interprocess::managed_shared_memory mem_;
#endif
        IPCMutexWrapper<EDPS> mutex_;
        BoostSharedMemoryStorageItem<T, ForceSeparate> *head_;
        std::optional<ByteDataHookPair> hookPair_;
        BoostSharedMemoryChainDataLock<DLS> dataLock_;

        boost::interprocess::interprocess_condition *notificationCond_;
    public:
        using StorageIDType = std::string;
        using DataType = T;
        static constexpr BoostSharedMemoryChainFastRecoverSupport FastRecoverySupport = BoostSharedMemoryChainFastRecoverSupport::ByName;
        static constexpr BoostSharedMemoryChainExtraDataProtectionStrategy ExtraDataProtectionStrategy = EDPS;
        using ItemType = BoostSharedMemoryChainItem<T,BoostSharedMemoryChainFastRecoverSupport::ByName,ForceSeparate>;
        static constexpr bool SupportsExtraData = true;
        static constexpr bool DataLockIsTrivial = (DLS == BoostSharedMemoryChainDataLockStrategy::None);
    private:
        inline ItemType fromIDAndPtr(std::string const &id, BoostSharedMemoryStorageItem<T, ForceSeparate> *ptr) {
            if constexpr (!ForceSeparate && std::is_trivially_copyable_v<T>) {
                return ItemType {
                    ptr
                    , id
                    , ParsedNodeData<T, ForceSeparate> {}
                };
            } else {
                std::optional<T> t {T {}};
                if (ptr) {
                    if (ptr->data == 0) {
                        return ItemType {
                            ptr
                            , id
                            , ParsedNodeData<T, ForceSeparate> {}
                        };
                    }
                    char const *dataPtr = reinterpret_cast<char const *>(ptr)+ptr->data;
                    std::size_t sz;
                    std::memcpy(&sz, dataPtr, sizeof(std::size_t));
                    if constexpr (ForceSeparate) {
                        if (hookPair_ && hookPair_->wireToUser) {
                            auto parsed = hookPair_->wireToUser->hook(
                                basic::ByteDataView {std::string_view {dataPtr+sizeof(std::size_t), sz}}
                            );
                            if (parsed) {
                                if (basic::bytedata_utils::RunDeserializer<T>::applyInPlace(*t, std::string_view {parsed->content})) {
                                    return ItemType {
                                        ptr
                                        , id
                                        , std::move(t)
                                    };
                                } else {
                                    return ItemType {
                                        ptr
                                        , id
                                        , ParsedNodeData<T, ForceSeparate> {}
                                    };
                                }
                            } else {
                                return ItemType {
                                    ptr
                                    , id
                                    , ParsedNodeData<T, ForceSeparate> {}
                                };
                            }
                        } else {
                            if (basic::bytedata_utils::RunDeserializer<T>::applyInPlace(*t, std::string_view {dataPtr+sizeof(std::size_t), sz})) {
                                return ItemType {
                                    ptr
                                    , id
                                    , std::move(t)
                                };
                            } else {
                                return ItemType {
                                    ptr
                                    , id
                                    , ParsedNodeData<T, ForceSeparate> {}
                                };
                            }
                        }
                    } else {
                        if (basic::bytedata_utils::RunDeserializer<T>::applyInPlace(*t, std::string_view {dataPtr+sizeof(std::size_t), sz})) {
                            return ItemType {
                                ptr
                                , id
                                , std::move(t)
                            };
                        } else {
                            return ItemType {
                                ptr
                                , id
                                , ParsedNodeData<T, ForceSeparate> {}
                            };
                        }
                    }
                } else {
                    return ItemType {
                        ptr
                        , id
                        , ParsedNodeData<T, ForceSeparate> {}
                    };
                }
            };
        }
    public:
        LockFreeInBoostSharedMemoryChain(std::string const &name, std::size_t sharedMemorySize, std::optional<ByteDataHookPair> hookPair=std::nullopt, bool useNotification=false) :
            mem_(
                boost::interprocess::open_or_create
                , name.c_str()
                , sharedMemorySize
            )
            , mutex_(name, "extra_data", &mem_)
            , head_(nullptr)
            , hookPair_(ForceSeparate?hookPair:std::nullopt)
            , dataLock_(name, "chain_data", &mem_)
            , notificationCond_(nullptr)
        {
            if constexpr (ForceSeparate || !std::is_trivially_copyable_v<T>) {
                head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T, ForceSeparate>>("head")();
                head_->data = 0;
            } else {
                head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T, ForceSeparate>>(boost::interprocess::unique_instance)();
            }
            if (useNotification) {
                notificationCond_ = mem_.find_or_construct<boost::interprocess::interprocess_condition>(boost::interprocess::unique_instance)();
            }
        }
        ItemType head(void *) {
            return fromIDAndPtr("", head_);
        }
        ItemType loadUntil(void *env, StorageIDType const &id) {
            if (id == "" || id == "head") {
                return head(env);
            }
            return fromIDAndPtr(
                id, mem_.find<BoostSharedMemoryStorageItem<T, ForceSeparate>>(id.c_str()).first
            );
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            if (!current.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("FetchNext on nullptr");
            }
            auto next = current.ptr->next.load(std::memory_order_acquire);
            if (next != 0) {
                auto *p = std::launder(reinterpret_cast<BoostSharedMemoryStorageItem<T, ForceSeparate> *>(reinterpret_cast<char *>(current.ptr)+next));
                return fromIDAndPtr(
                    mem_.get_instance_name(p)
                    , p
                );
            } else {
                return std::nullopt;
            }
        }
        bool appendAfter(ItemType const &current, ItemType &&toBeWritten) {
            if (!current.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter on nullptr");
            }
            if (!toBeWritten.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append nullptr");
            }
            if (toBeWritten.ptr->next != 0) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append an item with non-zero next");
            }
            std::ptrdiff_t x = 0;
            bool ret = std::atomic_compare_exchange_strong<std::ptrdiff_t>(
                &(current.ptr->next)
                , &x
                , (reinterpret_cast<char const *>(toBeWritten.ptr)-reinterpret_cast<char const *>(current.ptr))
            );
            if (!ret) {
                destroyItem(std::move(toBeWritten));
            }
            if (notificationCond_) {
                notificationCond_->notify_all();
            }
            return ret;
        }
        bool appendAfter(ItemType const &current, std::vector<ItemType> &&toBeWritten) {
            if (toBeWritten.empty()) {
                return true;
            }
            if (toBeWritten.size() == 1) {
                return appendAfter(current, std::move(toBeWritten[0]));
            }
            if (!current.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter on nullptr");
            }
            if (!toBeWritten[0].ptr) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append nullptr");
            }
            if (toBeWritten.back().ptr->next != 0) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append a last item with non-zero next");
            }
            std::ptrdiff_t x = 0;
            bool ret = std::atomic_compare_exchange_strong<std::ptrdiff_t>(
                &(current.ptr->next)
                , &x
                , (reinterpret_cast<char const *>(toBeWritten[0].ptr)-reinterpret_cast<char const *>(current.ptr))
            );
            if (!ret) {
                destroyItem(std::move(toBeWritten[0]));
            }
            if (notificationCond_) {
                notificationCond_->notify_all();
            }
            return ret;
        }
        template <class ExtraData>
        void saveExtraData(std::string const &key, ExtraData const &data) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports storing extra data only if enabled in template signature");
            if constexpr (!ForceSeparate && std::is_trivially_copyable_v<ExtraData>) {
                if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                    auto *dataPtr = mem_.construct<ExtraData>(boost::interprocess::anonymous_instance)(data);
                    auto *loc = mem_.find_or_construct<std::atomic<std::ptrdiff_t>>(key.c_str())(0);
                    std::ptrdiff_t newVal = reinterpret_cast<char *>(dataPtr)-reinterpret_cast<char *>(loc);
                    loc->store(newVal, std::memory_order_release);
                } else if constexpr (
                    EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                    || 
                    EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
                ) {
                    IPCMutexWrapperGuard<EDPS> _(&mutex_);
                    *(mem_.find_or_construct<ExtraData>(key.c_str())()) = data;
                } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe) {
                    *(mem_.find_or_construct<ExtraData>(key.c_str())()) = data;
                }
            } else {
                auto encoded = basic::bytedata_utils::RunSerializer<ExtraData>::apply(data);
                saveExtraBytes(key, basic::ByteData {std::move(encoded)});
            }
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports loading extra data only if enabled in template signature");
            if constexpr (!ForceSeparate && std::is_trivially_copyable_v<ExtraData>) {
                if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                    auto *loc = mem_.find<std::atomic<std::ptrdiff_t>>(key.c_str()).first;
                    if (loc) {
                        auto diff = loc->load(std::memory_order_acquire);
                        auto *dataPtr = std::launder(reinterpret_cast<ExtraData *>(reinterpret_cast<char *>(loc)+diff));
                        return *dataPtr;
                    } else {
                        return std::nullopt;
                    }
                } else if constexpr (
                    EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                    || 
                    EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
                ) {
                    IPCMutexWrapperGuard<EDPS> _(&mutex_);
                    auto *p = mem_.find<ExtraData>(key.c_str()).first;
                    if (p) {
                        return *p;
                    } else {
                        return std::nullopt;
                    }
                } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe) {
                    auto *p = mem_.find<ExtraData>(key.c_str()).first;
                    if (p) {
                        return *p;
                    } else {
                        return std::nullopt;
                    }
                } else {
                    return std::nullopt;
                }
            } else {
                auto b = loadExtraBytes(key);
                if (b) {
                    std::optional<ExtraData> e {ExtraData {}};
                    if (basic::bytedata_utils::RunDeserializer<ExtraData>::applyInPlace(*e, std::string_view {b->content})) {
                        return e;
                    } else {
                        return std::nullopt;
                    }
                } else {
                    return std::nullopt;
                }
            }
        }
        void saveExtraBytes(std::string const &key, basic::ByteData &&data) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports storing extra data only if enabled in template signature");
            if (hookPair_ && hookPair_->userToWire) {
                saveExtraBytes_internal(key, hookPair_->userToWire->hook(std::move(data)));
            } else {
                saveExtraBytes_internal(key, std::move(data));
            }
        }
    private:
        void saveExtraBytes_internal(std::string const &key, basic::ByteData &&data) {
            if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                std::size_t sz = data.content.length();
                auto *dataPtr = mem_.construct<char>(boost::interprocess::anonymous_instance)[sz+sizeof(std::size_t)]();
                std::memcpy(dataPtr+sizeof(std::size_t), data.content.data(), sz);
                std::memcpy(dataPtr, reinterpret_cast<char const *>(&sz), sizeof(std::size_t));
                auto *loc = mem_.find_or_construct<std::atomic<std::ptrdiff_t>>(key.c_str())(0);
                std::ptrdiff_t newVal = dataPtr-reinterpret_cast<char *>(loc);
                loc->store(newVal, std::memory_order_release);
            } else if constexpr (
                    EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                    || 
                    EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
            ) {
                std::size_t sz = data.content.length();
                IPCMutexWrapperGuard<EDPS> _(&mutex_);
                auto oldDataPtr = mem_.find<char>(key.c_str());
                char *dataPtr;
                if (oldDataPtr.first) {
                    if (oldDataPtr.second != sz) {
                        mem_.destroy_ptr(oldDataPtr.first);
                        dataPtr = mem_.construct<char>(key.c_str())[sz]();
                    } else {
                        dataPtr = oldDataPtr.first;
                    }
                } else {
                    dataPtr = mem_.construct<char>(key.c_str())[sz]();
                }
                std::memcpy(dataPtr, data.content.data(), sz);
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe) {
                std::size_t sz = data.content.length();
                auto oldDataPtr = mem_.find<char>(key.c_str());
                char *dataPtr;
                if (oldDataPtr.first) {
                    if (oldDataPtr.second != sz) {
                        mem_.destroy_ptr(oldDataPtr.first);
                        dataPtr = mem_.construct<char>(key.c_str())[sz]();
                    } else {
                        dataPtr = oldDataPtr.first;
                    }
                } else {
                    dataPtr = mem_.construct<char>(key.c_str())[sz]();
                }
                std::memcpy(dataPtr, data.content.data(), sz);
            }
        }
    public:
        std::optional<basic::ByteData> loadExtraBytes(std::string const &key) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports loading extra data only if enabled in template signature");
            auto b = loadExtraBytes_internal(key);
            if (hookPair_ && hookPair_->wireToUser) {
                if (b) {
                    return hookPair_->wireToUser->hook(*b);
                } else {
                    return std::nullopt;
                }
            } else {
                if (b) {
                    return basic::ByteData {std::string(b->content)};
                } else {
                    return std::nullopt;
                }
            }
        }
    private:
        std::optional<basic::ByteDataView> loadExtraBytes_internal(std::string const &key) {
            if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                auto *loc = mem_.find<std::atomic<std::ptrdiff_t>>(key.c_str()).first;
                if (loc) {
                    auto diff = loc->load(std::memory_order_acquire);
                    char *dataPtr = reinterpret_cast<char *>(loc)+diff;
                    std::size_t sz;
                    std::memcpy(reinterpret_cast<char *>(&sz), dataPtr, sizeof(std::size_t));
                    return basic::ByteDataView {
                        std::string_view {dataPtr+sizeof(std::size_t), sz}
                    };
                } else {
                    return std::nullopt;
                }
            } else if constexpr (
                EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                || 
                EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
            ) {
                IPCMutexWrapperGuard<EDPS> _(&mutex_);
                auto p = mem_.find<char>(key.c_str());
                if (p.first) {
                    return basic::ByteDataView {
                        std::string_view {p.first, p.second}
                    };
                } else {
                    return std::nullopt;
                }
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe) {
                auto p = mem_.find<char>(key.c_str());
                if (p.first) {
                    return basic::ByteDataView {
                        std::string_view {p.first, p.second}
                    };
                } else {
                    return std::nullopt;
                }
            } else {
                return std::nullopt;
            }
        }
    public:
        template <class Env>
        static StorageIDType newStorageID() {
            return Env::id_to_string(Env::new_id());
        }
        template <class Env>
        static std::string newStorageIDAsString() {
            return newStorageID<Env>();
        }
        ItemType formChainItem(StorageIDType const &id, T &&data) {
            if constexpr (!ForceSeparate && std::is_trivially_copyable_v<T>) {
                return fromIDAndPtr(
                    id
                    , mem_.construct<BoostSharedMemoryStorageItem<T, ForceSeparate>>(id.c_str())(std::move(data))
                );
            } else {
                std::string enc;
                if constexpr (ForceSeparate) {
                    if (hookPair_ && hookPair_->userToWire) {
                        enc = hookPair_->userToWire->hook(
                            basic::ByteData {basic::bytedata_utils::RunSerializer<T>::apply(data)}
                        ).content;
                    } else {
                        enc = basic::bytedata_utils::RunSerializer<T>::apply(data);
                    }
                } else {
                    enc = basic::bytedata_utils::RunSerializer<T>::apply(data);
                }
                std::size_t sz = enc.length();
                auto *ptr = mem_.construct<BoostSharedMemoryStorageItem<T, ForceSeparate>>(id.c_str())(0);
                auto *dataPtr = mem_.construct<char>(boost::interprocess::anonymous_instance)[sz+sizeof(std::size_t)]();
                std::memcpy(dataPtr, reinterpret_cast<char const *>(&sz), sizeof(std::size_t));
                std::memcpy(dataPtr+sizeof(std::size_t), enc.data(), sz);
                ptr->data = (dataPtr-reinterpret_cast<char *>(ptr));
                return {
                    ptr
                    , id
                    , std::move(data)
                };
            }
        }
        std::vector<ItemType> formChainItems(std::vector<std::tuple<StorageIDType, T>> &&data) {
            std::vector<ItemType> ret;
            bool first = true;
            for (auto &&x : data) {
                auto *p = (first?nullptr:&(ret.back()));
                auto newItem = formChainItem(std::get<0>(x), std::move(std::get<1>(x)));
                if (p) {
                    p->ptr->next.store(reinterpret_cast<char const *>(newItem.ptr)-reinterpret_cast<char const *>(p->ptr));
                }
                ret.push_back(newItem);
                first = false;
            }
            return ret;
        }
        void destroyPtr(decltype(((ItemType *) nullptr)->ptr) ptr) {
            if (ptr) {
                if (ptr->next != 0) {
                    destroyPtr(std::launder(reinterpret_cast<decltype(ptr)>(reinterpret_cast<char *>(ptr)+ptr->next)));
                }
                if constexpr (ForceSeparate || !std::is_trivially_copyable_v<T>) {
                    if (ptr->data != 0) {
                        mem_.destroy_ptr(reinterpret_cast<char const *>(ptr)+ptr->data);
                    }
                }
                mem_.destroy_ptr(ptr);
            }
        }
        void destroyItem(ItemType &&p) {
            destroyPtr(p.ptr);
        }
        static StorageIDType extractStorageID(ItemType const &p) {
            return p.id;
        }
        static T const *extractData(ItemType const &p) {
            return p.actualData();
        }
        static std::string_view extractStorageIDStringView(ItemType const &p) {
            return std::string_view {p.id};
        }
        void acquireLock() {
            dataLock_.lock();
        }
        void releaseLock() {
            dataLock_.unlock();
        }
        void waitForUpdate(std::chrono::system_clock::duration const &d) {
            if (notificationCond_) {
                boost::interprocess::interprocess_mutex mut;
                boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(mut);
                notificationCond_->timed_wait(
                    lock
                    , boost::posix_time::microsec_clock::universal_time()
                      +boost::posix_time::microseconds(
                          std::chrono::duration_cast<std::chrono::microseconds>(d).count()
                      )
                );
            } else {
                std::this_thread::sleep_for(d);
            }
        }
    };

    template <class T, BoostSharedMemoryChainExtraDataProtectionStrategy EDPS, bool ForceSeparate, BoostSharedMemoryChainDataLockStrategy DLS>
    class LockFreeInBoostSharedMemoryChain<
        T
        , BoostSharedMemoryChainFastRecoverSupport::ByOffset
        , EDPS 
        , ForceSeparate
        , DLS
    > {
    private:
#ifdef _MSC_VER
        boost::interprocess::managed_windows_shared_memory mem_;
#else
        boost::interprocess::managed_shared_memory mem_;
#endif
        IPCMutexWrapper<EDPS> mutex_;
        BoostSharedMemoryStorageItem<T, ForceSeparate> *head_;
        std::optional<ByteDataHookPair> hookPair_;
        BoostSharedMemoryChainDataLock<DLS> dataLock_;
        boost::interprocess::interprocess_condition *notificationCond_;
    public:
        using StorageIDType = std::ptrdiff_t;
        using DataType = T;
        static constexpr BoostSharedMemoryChainFastRecoverSupport FastRecoverySupport = BoostSharedMemoryChainFastRecoverSupport::ByOffset;
        static constexpr BoostSharedMemoryChainExtraDataProtectionStrategy ExtraDataProtectionStrategy = EDPS;
        using ItemType = BoostSharedMemoryChainItem<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset,ForceSeparate>;
        static constexpr bool SupportsExtraData = true;
        static constexpr bool DataLockIsTrivial = (DLS == BoostSharedMemoryChainDataLockStrategy::None);
    private:
        inline ItemType fromPtr(BoostSharedMemoryStorageItem<T, ForceSeparate> *ptr) const {
            if constexpr (!ForceSeparate && std::is_trivially_copyable_v<T>) {
                return ItemType {
                    ptr
                    , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                    , ParsedNodeData<T, ForceSeparate> {}
                };
            } else {
                std::optional<T> t {T {}};
                if (ptr) {
                    if (ptr->data == 0) {
                        return ItemType {
                            ptr
                            , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                            , ParsedNodeData<T, ForceSeparate> {}
                        };
                    }
                    char const *dataPtr = reinterpret_cast<char const *>(ptr)+ptr->data;
                    std::size_t sz;
                    std::memcpy(&sz, dataPtr, sizeof(std::size_t));
                    if constexpr (ForceSeparate) {
                        if (hookPair_ && hookPair_->wireToUser) {
                            auto parsed = hookPair_->wireToUser->hook(
                                basic::ByteDataView {std::string_view {dataPtr+sizeof(std::size_t), sz}}
                            );
                            if (parsed) {
                                if (basic::bytedata_utils::RunDeserializer<T>::applyInPlace(*t, std::string_view {parsed->content})) {
                                    return ItemType {
                                        ptr
                                        , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                                        , std::move(t)
                                    };
                                } else {
                                    return ItemType {
                                        ptr
                                        , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                                        , ParsedNodeData<T, ForceSeparate> {}
                                    };
                                }
                            } else {
                                return ItemType {
                                    ptr
                                    , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                                    , ParsedNodeData<T, ForceSeparate> {}
                                };
                            }
                        } else {
                            if (basic::bytedata_utils::RunDeserializer<T>::applyInPlace(*t, std::string_view {dataPtr+sizeof(std::size_t), sz})) {
                                return ItemType {
                                    ptr
                                    , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                                    , std::move(t)
                                };
                            } else {
                                return ItemType {
                                    ptr
                                    , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                                    , ParsedNodeData<T, ForceSeparate> {}
                                };
                            }
                        }
                    } else {
                        if (basic::bytedata_utils::RunDeserializer<T>::applyInPlace(*t, std::string_view {dataPtr+sizeof(std::size_t), sz})) {
                            return ItemType {
                                ptr
                                , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                                , std::move(t)
                            };
                        } else {
                            return ItemType {
                                ptr
                                , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                                , ParsedNodeData<T, ForceSeparate> {}
                            };
                        }
                    }
                } else {
                    return ItemType {
                        ptr
                        , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                        , ParsedNodeData<T, ForceSeparate> {}
                    };
                }
            }
        }
    public:
        LockFreeInBoostSharedMemoryChain(std::string const &name, std::size_t sharedMemorySize, std::optional<ByteDataHookPair> hookPair=std::nullopt, bool useNotification=false) :
            mem_(
                boost::interprocess::open_or_create
                , name.c_str()
                , sharedMemorySize
            )
            , mutex_(name, "extra_data", &mem_)
            , head_(nullptr)
            , hookPair_(ForceSeparate?hookPair:std::nullopt)
            , dataLock_(name, "chain_data", &mem_)
            , notificationCond_(nullptr)
        {
            if constexpr (ForceSeparate || !std::is_trivially_copyable_v<T>) {
                head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T, ForceSeparate>>("head")();
                head_->data = 0;
            } else {
                head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T, ForceSeparate>>(boost::interprocess::unique_instance)();
            }
            if (useNotification) {
                notificationCond_ = mem_.find_or_construct<boost::interprocess::interprocess_condition>(boost::interprocess::unique_instance)();
            }
        }
        ItemType head(void *) {
            return fromPtr(
                head_
            );
        }
        ItemType loadUntil(void *env, StorageIDType const &id) {
            return fromPtr(
                std::launder(reinterpret_cast<BoostSharedMemoryStorageItem<T, ForceSeparate> *>(reinterpret_cast<char *>(head_)+id))
            );
        }
        ItemType loadUntil(void *env, std::string const &id) {
            if (id == "") {
                return head(env);
            }
            if (id.length() < sizeof(StorageIDType)) {
                throw std::runtime_error("LockFreeInBoostSharedMemoryChain(using offset)::loadUntil: ID must have sufficient length");
            }
            StorageIDType storageID;
            std::memcpy(reinterpret_cast<char *>(&storageID), id.data(), sizeof(StorageIDType));
            return loadUntil(env, storageID);
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            if (!current.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("FetchNext on nullptr");
            }
            auto next = current.ptr->next.load(std::memory_order_acquire);
            if (next != 0) {
                return fromPtr(
                    std::launder(reinterpret_cast<BoostSharedMemoryStorageItem<T, ForceSeparate> *>(reinterpret_cast<char *>(current.ptr)+next))
                );
            } else {
                return std::nullopt;
            }
        }
        bool appendAfter(ItemType const &current, ItemType &&toBeWritten) {
            if (!current.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter on nullptr");
            }
            if (!toBeWritten.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append nullptr");
            }
            if (toBeWritten.ptr->next != 0) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append an item with non-zero next");
            }
            std::ptrdiff_t x = 0;
            bool ret = std::atomic_compare_exchange_strong<std::ptrdiff_t>(
                &(current.ptr->next)
                , &x
                , (reinterpret_cast<char const *>(toBeWritten.ptr)-reinterpret_cast<char const *>(current.ptr))
            );
            if (!ret) {
                destroyItem(std::move(toBeWritten));
            }
            if (notificationCond_) {
                notificationCond_->notify_all();
            }
            return ret;
        }
        bool appendAfter(ItemType const &current, std::vector<ItemType> &&toBeWritten) {
            if (toBeWritten.empty()) {
                return true;
            }
            if (toBeWritten.size() == 1) {
                return appendAfter(current, std::move(toBeWritten[0]));
            }
            if (!current.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter on nullptr");
            }
            if (!toBeWritten[0].ptr) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append nullptr");
            }
            if (toBeWritten.back().ptr->next != 0) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append a last item with non-zero next");
            }
            std::ptrdiff_t x = 0;
            bool ret = std::atomic_compare_exchange_strong<std::ptrdiff_t>(
                &(current.ptr->next)
                , &x
                , (reinterpret_cast<char const *>(toBeWritten[0].ptr)-reinterpret_cast<char const *>(current.ptr))
            );
            if (!ret) {
                destroyItem(std::move(toBeWritten[0]));
            }
            if (notificationCond_) {
                notificationCond_->notify_all();
            }
            return ret;
        }
        template <class ExtraData>
        void saveExtraData(std::string const &key, ExtraData const &data) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports storing extra data only if enabled in template signature");
            if constexpr (!ForceSeparate && std::is_trivially_copyable_v<ExtraData>) {
                if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                    auto *dataPtr = mem_.construct<ExtraData>(boost::interprocess::anonymous_instance)(data);
                    auto *loc = mem_.find_or_construct<std::atomic<std::ptrdiff_t>>(key.c_str())(0);
                    std::ptrdiff_t newVal = reinterpret_cast<char *>(dataPtr)-reinterpret_cast<char *>(loc);
                    loc->store(newVal, std::memory_order_release);
                } else if constexpr (
                    EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                    || 
                    EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
                ) {
                    IPCMutexWrapperGuard<EDPS> _(&mutex_);
                    *(mem_.find_or_construct<ExtraData>(key.c_str())()) = data;
                } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe) {
                    *(mem_.find_or_construct<ExtraData>(key.c_str())()) = data;
                }
            } else {
                auto encoded = basic::bytedata_utils::RunSerializer<ExtraData>::apply(data);
                saveExtraBytes(key, basic::ByteData {std::move(encoded)});
            }
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports loading extra data only if enabled in template signature");
            if constexpr (!ForceSeparate && std::is_trivially_copyable_v<ExtraData>) {
                if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                    auto *loc = mem_.find<std::atomic<std::ptrdiff_t>>(key.c_str()).first;
                    if (loc) {
                        auto diff = loc->load(std::memory_order_acquire);
                        auto *dataPtr = std::launder(reinterpret_cast<ExtraData *>(reinterpret_cast<char *>(loc)+diff));
                        return *dataPtr;
                    } else {
                        return std::nullopt;
                    }
                } else if constexpr (
                    EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                    || 
                    EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
                ) {
                    IPCMutexWrapperGuard<EDPS> _(&mutex_);
                    auto *p = mem_.find<ExtraData>(key.c_str()).first;
                    if (p) {
                        return *p;
                    } else {
                        return std::nullopt;
                    }
                } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe) {
                    auto *p = mem_.find<ExtraData>(key.c_str()).first;
                    if (p) {
                        return *p;
                    } else {
                        return std::nullopt;
                    }
                } else {
                    return std::nullopt;
                }
            } else {
                auto b = loadExtraBytes(key);
                if (b) {
                    std::optional<ExtraData> e {ExtraData {}};
                    if (basic::bytedata_utils::RunDeserializer<ExtraData>::applyInPlace(*e, b->content)) {
                        return e;
                    } else {
                        return std::nullopt;
                    }
                } else {
                    return std::nullopt;
                }
            }
        }
        void saveExtraBytes(std::string const &key, basic::ByteData &&data) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports storing extra data only if enabled in template signature");
            if (hookPair_ && hookPair_->userToWire) {
                saveExtraBytes_internal(key, hookPair_->userToWire->hook(std::move(data)));
            } else {
                saveExtraBytes_internal(key, std::move(data));
            }
        }
    private:
        void saveExtraBytes_internal(std::string const &key, basic::ByteData &&data) {
            if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                std::size_t sz = data.content.length();
                auto *dataPtr = mem_.construct<char>(boost::interprocess::anonymous_instance)[sz+sizeof(std::size_t)]();
                std::memcpy(dataPtr+sizeof(std::size_t), data.content.data(), sz);
                std::memcpy(dataPtr, reinterpret_cast<char const *>(&sz), sizeof(std::size_t));
                auto *loc = mem_.find_or_construct<std::atomic<std::ptrdiff_t>>(key.c_str())(0);
                std::ptrdiff_t newVal = dataPtr-reinterpret_cast<char *>(loc);
                loc->store(newVal, std::memory_order_release);
            } else if constexpr (
                EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                || 
                EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
            ) {
                std::size_t sz = data.content.length();
                IPCMutexWrapperGuard<EDPS> _(&mutex_);
                auto oldDataPtr = mem_.find<char>(key.c_str());
                char *dataPtr;
                if (oldDataPtr.first) {
                    if (oldDataPtr.second != sz) {
                        mem_.destroy_ptr(oldDataPtr.first);
                        dataPtr = mem_.construct<char>(key.c_str())[sz]();
                    } else {
                        dataPtr = oldDataPtr.first;
                    }
                } else {
                    dataPtr = mem_.construct<char>(key.c_str())[sz]();
                }
                std::memcpy(dataPtr, data.content.data(), sz);
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe) {
                std::size_t sz = data.content.length();
                auto oldDataPtr = mem_.find<char>(key.c_str());
                char *dataPtr;
                if (oldDataPtr.first) {
                    if (oldDataPtr.second != sz) {
                        mem_.destroy_ptr(oldDataPtr.first);
                        dataPtr = mem_.construct<char>(key.c_str())[sz]();
                    } else {
                        dataPtr = oldDataPtr.first;
                    }
                } else {
                    dataPtr = mem_.construct<char>(key.c_str())[sz]();
                }
                std::memcpy(dataPtr, data.content.data(), sz);
            }
        }
    public:
        std::optional<basic::ByteData> loadExtraBytes(std::string const &key) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports loading extra data only if enabled in template signature");
            auto b = loadExtraBytes_internal(key);
            if (hookPair_ && hookPair_->wireToUser) {
                if (b) {
                    return hookPair_->wireToUser->hook(*b);
                } else {
                    return std::nullopt;
                }
            } else {
                if (b) {
                    return basic::ByteData {std::string(b->content)};
                } else {
                    return std::nullopt;
                }
            }
        }
    private:
        std::optional<basic::ByteDataView> loadExtraBytes_internal(std::string const &key) {
            if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                auto *loc = mem_.find<std::atomic<std::ptrdiff_t>>(key.c_str()).first;
                if (loc) {
                    auto diff = loc->load(std::memory_order_acquire);
                    char *dataPtr = reinterpret_cast<char *>(loc)+diff;
                    std::size_t sz;
                    std::memcpy(reinterpret_cast<char *>(&sz), dataPtr, sizeof(std::size_t));
                    return basic::ByteDataView {
                        std::string_view {dataPtr+sizeof(std::size_t), sz}
                    };
                } else {
                    return std::nullopt;
                }
            } else if constexpr (
                EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                || 
                EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
            ) {
                IPCMutexWrapperGuard<EDPS> _(&mutex_);
                auto p = mem_.find<char>(key.c_str());
                if (p.first) {
                    return basic::ByteDataView {
                        std::string_view {p.first, p.second}
                    };
                } else {
                    return std::nullopt;
                }
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe) {
                auto p = mem_.find<char>(key.c_str());
                if (p.first) {
                    return basic::ByteDataView {
                        std::string_view {p.first, p.second}
                    };
                } else {
                    return std::nullopt;
                }
            } else {
                return std::nullopt;
            }
        }
    public:
        template <class Env>
        static StorageIDType newStorageID() {
            return 0;
        }
        template <class Env>
        static std::string newStorageIDAsString() {
            return "";
        }
        ItemType formChainItem(StorageIDType const &notUsed, T &&data) {
            if constexpr (!ForceSeparate && std::is_trivially_copyable_v<T>) {
                return fromPtr(
                    mem_.construct<BoostSharedMemoryStorageItem<T, ForceSeparate>>(boost::interprocess::anonymous_instance)(std::move(data))
                );
            } else {
                std::string enc;
                if constexpr (ForceSeparate) {
                    if (hookPair_ && hookPair_->userToWire) {
                        enc = hookPair_->userToWire->hook(
                            basic::ByteData {basic::bytedata_utils::RunSerializer<T>::apply(data)}
                        ).content;
                    } else {
                        enc = basic::bytedata_utils::RunSerializer<T>::apply(data);
                    }
                } else {
                    enc = basic::bytedata_utils::RunSerializer<T>::apply(data);
                }
                std::size_t sz = enc.length();
                auto *ptr = mem_.construct<BoostSharedMemoryStorageItem<T, ForceSeparate>>(boost::interprocess::anonymous_instance)(0);
                auto *dataPtr = mem_.construct<char>(boost::interprocess::anonymous_instance)[sz+sizeof(std::size_t)]();
                std::memcpy(dataPtr, reinterpret_cast<char const *>(&sz), sizeof(std::size_t));
                std::memcpy(dataPtr+sizeof(std::size_t), enc.data(), sz);
                ptr->data = (dataPtr-reinterpret_cast<char *>(ptr));
                return {
                    ptr
                    , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                    , std::move(data)
                };
            }
        }
        ItemType formChainItem(std::string const &notUsed, T &&data) {
            return formChainItem((StorageIDType) 0, std::move(data));
        }
        std::vector<ItemType> formChainItems(std::vector<std::tuple<std::string, T>> &&data) {
            std::vector<ItemType> ret;
            bool first = true;
            for (auto &&x : data) {
                auto *p = (first?nullptr:&(ret.back()));
                auto newItem = formChainItem(std::get<0>(x), std::move(std::get<1>(x)));
                if (p) {
                    p->ptr->next.store(reinterpret_cast<char const *>(newItem.ptr)-reinterpret_cast<char const *>(p->ptr));
                }
                ret.push_back(newItem);
                first = false;
            }
            return ret;
        }
        void destroyPtr(decltype(((ItemType *) nullptr)->ptr) ptr) {
            if (ptr) {
                if (ptr->next != 0) {
                    destroyPtr(std::launder(reinterpret_cast<decltype(ptr)>(reinterpret_cast<char *>(ptr)+ptr->next)));
                }
                if constexpr (ForceSeparate || !std::is_trivially_copyable_v<T>) {
                    if (ptr->data != 0) {
                        mem_.destroy_ptr(reinterpret_cast<char const *>(ptr)+ptr->data);
                    }
                }
                mem_.destroy_ptr(ptr);
            }
        }
        void destroyItem(ItemType &&p) {
            destroyPtr(p.ptr);
        }
        static StorageIDType extractStorageID(ItemType const &p) {
            return p.offset;
        }
        static T const *extractData(ItemType const &p) {
            return p.actualData();
        }
        static std::string_view extractStorageIDStringView(ItemType const &p) {
            return std::string_view(
                reinterpret_cast<char const *>(&p.offset)
                , sizeof(StorageIDType)
            );
        }
        void acquireLock() {
            dataLock_.lock();
        }
        void releaseLock() {
            dataLock_.unlock();
        }
        void waitForUpdate(std::chrono::system_clock::duration const &d) {
            if (notificationCond_) {
                boost::interprocess::interprocess_mutex mut;
                boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(mut);
                notificationCond_->timed_wait(
                    lock
                    , boost::posix_time::microsec_clock::universal_time()
                      +boost::posix_time::microseconds(
                          std::chrono::duration_cast<std::chrono::microseconds>(d).count()
                      )
                );
            } else {
                std::this_thread::sleep_for(d);
            }
        }
    };

}}}}}

#endif