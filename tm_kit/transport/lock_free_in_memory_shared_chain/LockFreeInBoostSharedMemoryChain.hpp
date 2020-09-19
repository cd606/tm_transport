#ifndef TM_KIT_TRANSPORT_LOCK_FREE_IN_BOOST_SHARED_MEMORY_SHARED_CHAIN_HPP_
#define TM_KIT_TRANSPORT_LOCK_FREE_IN_BOOST_SHARED_MEMORY_SHARED_CHAIN_HPP_

#include <tm_kit/basic/simple_shared_chain/ChainReader.hpp>
#include <tm_kit/basic/simple_shared_chain/ChainWriter.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <atomic>

#ifdef _MSC_VER
#include <boost/interprocess/managed_windows_shared_memory.hpp>
#else
#include <boost/interprocess/managed_shared_memory.hpp>
#endif
#include <boost/interprocess/sync/named_mutex.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace lock_free_in_memory_shared_chain {
    class LockFreeInBoostSharedMemoryChainException : public std::runtime_error {
    public:
        LockFreeInBoostSharedMemoryChainException(std::string const &s) : std::runtime_error(s) {}
    };

    enum class BoostSharedMemoryChainFastRecoverSupport {
        Enabled, Disabled
    };

    template <class T, BoostSharedMemoryChainFastRecoverSupport FRS>
    struct BoostSharedMemoryStorageItem {};
    
    template <class T>
    struct BoostSharedMemoryStorageItem<T, BoostSharedMemoryChainFastRecoverSupport::Enabled> {
        char id[36]; //for uuid, please notice that the end '\0' is not included
        T data;
        std::atomic<std::ptrdiff_t> next;
        BoostSharedMemoryStorageItem() : data(), next(0) {
            std::memset(id, 0, 36);
        }
        BoostSharedMemoryStorageItem(std::string const &s, T &&d) : data(std::move(d)), next(0) {
            std::memset(id, 0, 36);
            std::memcpy(id, s.c_str(), std::min<std::size_t>(36, s.length()));
        }
    };
    template <class T>
    struct BoostSharedMemoryStorageItem<T, BoostSharedMemoryChainFastRecoverSupport::Disabled> {
        T data;
        std::atomic<std::ptrdiff_t> next;
        BoostSharedMemoryStorageItem() : data(), next(0) {
        }
        BoostSharedMemoryStorageItem(T &&d) : data(std::move(d)), next(0) {
        }
    };
    
    template <class T, BoostSharedMemoryChainFastRecoverSupport FRS>
    using BoostSharedMemoryChainItem = BoostSharedMemoryStorageItem<T,FRS> *;

    enum class BoostSharedMemoryChainExtraDataProtectionStrategy {
        DontSupportExtraData
        , LockFreeAndWasteMemory
        , MutexProtected
        , Unsafe
    };

    template <BoostSharedMemoryChainExtraDataProtectionStrategy EDPS>
    class IPCMutexWrapper {
    public:
        IPCMutexWrapper(std::string const &) noexcept {}
        void lock() noexcept {}
        void unlock() noexcept {}
    };
    template <>
    struct IPCMutexWrapper<BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected> {
    private:
        boost::interprocess::named_mutex mutex_;
    public:
        IPCMutexWrapper(std::string const &name) :
            mutex_(
                boost::interprocess::open_or_create
                , (name+"_mutex").c_str()
            )
        {}
        void lock() {
            mutex_.lock();
        }
        void unlock() {
            mutex_.unlock();
        }
    };
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

    template <
        class T
        , BoostSharedMemoryChainFastRecoverSupport FRS
        , class Enable=std::enable_if_t<std::is_standard_layout_v<T>, void>
    > struct LockFreeInBoostSharedMemoryChainBase {};
    
    template <
        class T
        , BoostSharedMemoryChainFastRecoverSupport FRS
        , BoostSharedMemoryChainExtraDataProtectionStrategy EDPS = BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData
        , class Enable=std::enable_if_t<std::is_standard_layout_v<T>, void>
    >
    class LockFreeInBoostSharedMemoryChain {};

    template <class T, BoostSharedMemoryChainExtraDataProtectionStrategy EDPS>
    class LockFreeInBoostSharedMemoryChain<
        T
        , BoostSharedMemoryChainFastRecoverSupport::Enabled
        , EDPS 
        , void
    > : public LockFreeInBoostSharedMemoryChainBase<T,BoostSharedMemoryChainFastRecoverSupport::Enabled,void> {
    private:
        IPCMutexWrapper<EDPS> mutex_;
#ifdef _MSC_VER
        boost::interprocess::managed_windows_shared_memory mem_;
#else
        boost::interprocess::managed_shared_memory mem_;
#endif
        BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::Enabled> *head_;
    public:
        using ItemType = BoostSharedMemoryChainItem<T,BoostSharedMemoryChainFastRecoverSupport::Enabled>;
        LockFreeInBoostSharedMemoryChain(std::string const &name, std::size_t sharedMemorySize) :
            mutex_(name) 
            , mem_(
                boost::interprocess::open_or_create
                , name.c_str()
                , sharedMemorySize
            )
            , head_(nullptr)
        {
            head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::Enabled>>(boost::interprocess::unique_instance)();
        }
        ItemType head(void *) {
            return head_;
        }
        ItemType loadUntil(void *, std::string const &id) {
            if (id == "" || id == "head") {
                return head_;
            }
            return mem_.find<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::Enabled>>(id.c_str()).first;
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            if (!current) {
                throw LockFreeInBoostSharedMemoryChainException("FetchNext on nullptr");
            }
            auto next = current->next.load(std::memory_order_acquire);
            if (next != 0) {
                return (current+next);
            } else {
                return std::nullopt;
            }
        }
        bool appendAfter(ItemType const &current, ItemType &&toBeWritten) {
            if (!current) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter on nullptr");
            }
            if (!toBeWritten) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append nullptr");
            }
            if (toBeWritten->next != 0) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append an item with non-zero next");
            }
            std::ptrdiff_t x = 0;
            return std::atomic_compare_exchange_strong<std::ptrdiff_t>(
                &(current->next)
                , &x
                , (toBeWritten-current)
            );
        }
        template <class ExtraData>
        void saveExtraData(std::string const &key, ExtraData const &data) {
            static_assert(std::is_standard_layout_v<ExtraData>, "LockFreeInBoostSharedMemoryChain only supports storing standard layout extraData objects");
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports storing extra data only if enabled in template signature");
            if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                auto *dataPtr = mem_.construct<ExtraData>(boost::interprocess::anonymous_instance)(data);
                auto *loc = mem_.find_or_construct<std::atomic<std::ptrdiff_t>>(key.c_str())(0);
                std::ptrdiff_t newVal = reinterpret_cast<char *>(dataPtr)-reinterpret_cast<char *>(loc);
                loc->store(newVal, std::memory_order_release);
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected) {
                IPCMutexWrapperGuard<EDPS> _(&mutex_);
                *(mem_.find_or_construct<ExtraData>(key.c_str())()) = data;
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe) {
                *(mem_.find_or_construct<ExtraData>(key.c_str())()) = data;
            }
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            static_assert(std::is_standard_layout_v<ExtraData>, "LockFreeInBoostSharedMemoryChain only supports loading standard layout extraData objects");
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports loading extra data only if enabled in template signature");
            if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                auto *loc = mem_.find<std::atomic<std::ptrdiff_t>>(key.c_str()).first;
                if (loc) {
                    auto diff = loc->load(std::memory_order_acquire);
                    auto *dataPtr = reinterpret_cast<ExtraData *>(reinterpret_cast<char *>(loc)+diff);
                    return *dataPtr;
                } else {
                    return std::nullopt;
                }
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected) {
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
        }
        ItemType createItemFromData(std::string const &id, T &&data) {
            return mem_.construct<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::Enabled>>(id.c_str())(id, std::move(data));
        }
        void destroyItem(ItemType &p) {
            mem_.destroy_ptr(p);
        }
    };

    template <class T, BoostSharedMemoryChainExtraDataProtectionStrategy EDPS>
    class LockFreeInBoostSharedMemoryChain<
        T
        , BoostSharedMemoryChainFastRecoverSupport::Disabled
        , EDPS 
        , void
    > : public LockFreeInBoostSharedMemoryChainBase<T,BoostSharedMemoryChainFastRecoverSupport::Disabled,void> {
    private:
        IPCMutexWrapper<EDPS> mutex_;
#ifdef _MSC_VER
        boost::interprocess::managed_windows_shared_memory mem_;
#else
        boost::interprocess::managed_shared_memory mem_;
#endif
        BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::Disabled> *head_;
    public:
        using ItemType = BoostSharedMemoryChainItem<T,BoostSharedMemoryChainFastRecoverSupport::Disabled>;
        LockFreeInBoostSharedMemoryChain(std::string const &name, std::size_t sharedMemorySize) :
            mutex_(name) 
            , mem_(
                boost::interprocess::open_or_create
                , name.c_str()
                , sharedMemorySize
            )
            , head_(nullptr)
        {
            head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::Disabled>>(boost::interprocess::unique_instance)();
        }
        ItemType head(void *) {
            return head_;
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            if (!current) {
                throw LockFreeInBoostSharedMemoryChainException("FetchNext on nullptr");
            }
            auto next = current->next.load(std::memory_order_acquire);
            if (next != 0) {
                return (current+next);
            } else {
                return std::nullopt;
            }
        }
        bool appendAfter(ItemType const &current, ItemType &&toBeWritten) {
            if (!current) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter on nullptr");
            }
            if (!toBeWritten) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append nullptr");
            }
            if (toBeWritten->next != 0) {
                throw LockFreeInBoostSharedMemoryChainException("AppendAfter trying to append an item with non-zero next");
            }
            std::ptrdiff_t x = 0;
            return std::atomic_compare_exchange_strong<std::ptrdiff_t>(
                &(current->next)
                , &x
                , (toBeWritten-current)
            );
        }
        template <class ExtraData>
        void saveExtraData(std::string const &key, ExtraData const &data) {
            static_assert(std::is_standard_layout_v<ExtraData>, "LockFreeInBoostSharedMemoryChain only supports storing standard layout extraData objects");
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports storing extra data only if enabled in template signature");
            if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                auto *dataPtr = mem_.construct<ExtraData>(boost::interprocess::anonymous_instance)(data);
                auto *loc = mem_.find_or_construct<std::atomic<std::ptrdiff_t>>(key.c_str())(0);
                std::ptrdiff_t newVal = reinterpret_cast<char *>(dataPtr)-reinterpret_cast<char *>(loc);
                loc->store(newVal, std::memory_order_release);
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected) {
                IPCMutexWrapperGuard<EDPS> _(&mutex_);
                *(mem_.find_or_construct<ExtraData>(key.c_str())()) = data;
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe) {
                *(mem_.find_or_construct<ExtraData>(key.c_str())()) = data;
            }
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            static_assert(std::is_standard_layout_v<ExtraData>, "LockFreeInBoostSharedMemoryChain only supports loading standard layout extraData objects");
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports loading extra data only if enabled in template signature");
            if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                auto *loc = mem_.find<std::atomic<std::ptrdiff_t>>(key.c_str()).first;
                if (loc) {
                    auto diff = loc->load(std::memory_order_acquire);
                    auto *dataPtr = reinterpret_cast<ExtraData *>(reinterpret_cast<char *>(loc)+diff);
                    return *dataPtr;
                } else {
                    return std::nullopt;
                }
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected) {
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
        }
        ItemType createItemFromData(std::string const &id, T &&data) {
            return mem_.construct<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::Disabled>>(boost::interprocess::anonymous_instance)(std::move(data));
        }
        void destroyItem(ItemType &p) {
            mem_.destroy_ptr(p);
        }
    };

}}}}}

#endif