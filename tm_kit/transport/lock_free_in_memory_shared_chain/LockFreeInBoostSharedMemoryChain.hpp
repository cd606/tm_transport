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
        ByName, ByOffset
    };

    template <class T>
    using ActualNodeData = std::conditional_t<std::is_trivially_copyable_v<T>,T,std::ptrdiff_t>;

    template <class T, BoostSharedMemoryChainFastRecoverSupport FRS>
    struct BoostSharedMemoryStorageItem {};
    
    template <class T>
    #ifdef _MSC_VER
    __declspec(align(1))
    #endif
    struct BoostSharedMemoryStorageItem<T, BoostSharedMemoryChainFastRecoverSupport::ByName> {
        char id[36]; //for uuid, please notice that the end '\0' is not included
        ActualNodeData<T> data;
        std::atomic<std::ptrdiff_t> next;
        BoostSharedMemoryStorageItem() : data(), next(0) {
            std::memset(id, 0, 36);
        }
        BoostSharedMemoryStorageItem(std::string const &s, ActualNodeData<T> &&d) : data(std::move(d)), next(0) {
            std::memset(id, 0, 36);
            std::memcpy(id, s.c_str(), std::min<std::size_t>(36, s.length()));
        }
    }
    #ifndef _MSC_VER
    __attribute__ ((aligned (1)))
    #endif
    ;
    template <class T>
    #ifdef _MSC_VER
    __declspec(align(1))
    #endif
    struct BoostSharedMemoryStorageItem<T, BoostSharedMemoryChainFastRecoverSupport::ByOffset> {
        ActualNodeData<T> data;
        std::atomic<std::ptrdiff_t> next;
        BoostSharedMemoryStorageItem() : data(), next(0) {
        }
        BoostSharedMemoryStorageItem(ActualNodeData<T> &&d) : data(std::move(d)), next(0) {
        }
    }
    #ifndef _MSC_VER
    __attribute__ ((aligned (1)))
    #endif
    ;
    
    template <class T>
    using ParsedNodeData = std::conditional_t<std::is_trivially_copyable_v<T>,basic::VoidStruct,std::optional<T>>;
    template <class T, BoostSharedMemoryChainFastRecoverSupport FRS>
    struct BoostSharedMemoryChainItem {};
    template <class T>
    struct BoostSharedMemoryChainItem<T, BoostSharedMemoryChainFastRecoverSupport::ByName> {
        BoostSharedMemoryStorageItem<T, BoostSharedMemoryChainFastRecoverSupport::ByName> *ptr;
        ParsedNodeData<T> parsed;
        T const *actualData() const {
            if constexpr (std::is_trivially_copyable_v<T>) {
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
    template <class T>
    struct BoostSharedMemoryChainItem<T, BoostSharedMemoryChainFastRecoverSupport::ByOffset> {
        BoostSharedMemoryStorageItem<T, BoostSharedMemoryChainFastRecoverSupport::ByOffset> *ptr;
        std::ptrdiff_t offset;
        ParsedNodeData<T> parsed;
        T const *actualData() const {
            if constexpr (std::is_trivially_copyable_v<T>) {
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
    > struct LockFreeInBoostSharedMemoryChainBase {};
    
    template <
        class T
        , BoostSharedMemoryChainFastRecoverSupport FRS
        , BoostSharedMemoryChainExtraDataProtectionStrategy EDPS = BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData
    >
    class LockFreeInBoostSharedMemoryChain {};

    template <class T, BoostSharedMemoryChainExtraDataProtectionStrategy EDPS>
    class LockFreeInBoostSharedMemoryChain<
        T
        , BoostSharedMemoryChainFastRecoverSupport::ByName
        , EDPS 
    > : public LockFreeInBoostSharedMemoryChainBase<T,BoostSharedMemoryChainFastRecoverSupport::ByName> {
    private:
        IPCMutexWrapper<EDPS> mutex_;
#ifdef _MSC_VER
        boost::interprocess::managed_windows_shared_memory mem_;
#else
        boost::interprocess::managed_shared_memory mem_;
#endif
        BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByName> *head_;
    public:
        static constexpr BoostSharedMemoryChainFastRecoverSupport FastRecoverySupport = BoostSharedMemoryChainFastRecoverSupport::ByName;
        static constexpr BoostSharedMemoryChainExtraDataProtectionStrategy ExtraDataProtectionStrategy = EDPS;
        using ItemType = BoostSharedMemoryChainItem<T,BoostSharedMemoryChainFastRecoverSupport::ByName>;
    private:
        static inline ItemType fromPtr(BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByName> *ptr) {
            if constexpr (std::is_trivially_copyable_v<T>) {
                return ItemType {
                    ptr
                    , ParsedNodeData<T> {}
                };
            } else {
                if (ptr) {
                    if (ptr->data == 0) {
                        return ItemType {
                            ptr
                            , ParsedNodeData<T> {}
                        };
                    }
                    char const *dataPtr = reinterpret_cast<char const *>(ptr)+ptr->data;
                    std::size_t sz;
                    std::memcpy(&sz, dataPtr, sizeof(std::size_t));
                    return ItemType {
                        ptr
                        , basic::bytedata_utils::RunDeserializer<T>::apply(std::string_view {dataPtr+sizeof(std::size_t), sz})
                    };
                } else {
                    return ItemType {
                        ptr
                        , ParsedNodeData<T> {}
                    };
                }
            };
        }
    public:
        LockFreeInBoostSharedMemoryChain(std::string const &name, std::size_t sharedMemorySize) :
            mutex_(name) 
            , mem_(
                boost::interprocess::open_or_create
                , name.c_str()
                , sharedMemorySize
            )
            , head_(nullptr)
        {
            head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByName>>(boost::interprocess::unique_instance)();
            if constexpr (!std::is_trivially_copyable_v<T>) {
                head_->data = 0;
            }
        }
        ItemType head(void *) {
            return fromPtr(head_);
        }
        ItemType loadUntil(void *env, std::string const &id) {
            if (id == "" || id == "head") {
                return head(env);
            }
            return fromPtr(
                mem_.find<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByName>>(id.c_str()).first
            );
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            if (!current.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("FetchNext on nullptr");
            }
            auto next = current.ptr->next.load(std::memory_order_acquire);
            if (next != 0) {
                return fromPtr(
                    reinterpret_cast<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByName> *>(reinterpret_cast<char *>(current.ptr)+next)
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
            return std::atomic_compare_exchange_strong<std::ptrdiff_t>(
                &(current.ptr->next)
                , &x
                , (reinterpret_cast<char const *>(toBeWritten.ptr)-reinterpret_cast<char const *>(current.ptr))
            );
        }
        template <class ExtraData>
        void saveExtraData(std::string const &key, ExtraData const &data) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports storing extra data only if enabled in template signature");
            if constexpr (std::is_trivially_copyable_v<ExtraData>) {
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
            } else {
                auto encoded = basic::bytedata_utils::RunSerializer<ExtraData>::apply(data);
                saveExtraBytes(key, basic::ByteData {std::move(encoded)});
            }
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports loading extra data only if enabled in template signature");
            if constexpr (std::is_trivially_copyable_v<ExtraData>) {
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
            } else {
                auto b = loadExtraBytes(key);
                if (b) {
                    return basic::bytedata_utils::RunDeserializer<ExtraData>::apply(b->content);
                } else {
                    return std::nullopt;
                }
            }
        }
        void saveExtraBytes(std::string const &key, basic::ByteData const &data) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports storing extra data only if enabled in template signature");
            if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                std::size_t sz = data.content.length();
                auto *dataPtr = mem_.construct<char>(boost::interprocess::anonymous_instance)[sz+sizeof(std::size_t)]();
                std::memcpy(dataPtr+sizeof(std::size_t), data.content.data(), sz);
                std::memcpy(dataPtr, reinterpret_cast<char const *>(&sz), sizeof(std::size_t));
                auto *loc = mem_.find_or_construct<std::atomic<std::ptrdiff_t>>(key.c_str())(0);
                std::ptrdiff_t newVal = dataPtr-reinterpret_cast<char *>(loc);
                loc->store(newVal, std::memory_order_release);
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected) {
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
        std::optional<basic::ByteDataView> loadExtraBytes(std::string const &key) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports loading extra data only if enabled in template signature");
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
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected) {
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
        ItemType createItemFromData(std::string const &id, T &&data) {
            if constexpr (std::is_trivially_copyable_v<T>) {
                return fromPtr(
                    mem_.construct<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByName>>(id.c_str())(id, std::move(data))
                );
            } else {
                auto enc = basic::bytedata_utils::RunSerializer<T>::apply(data);
                std::size_t sz = enc.length();
                auto *ptr = mem_.construct<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByName>>(id.c_str())(id, 0);
                auto *dataPtr = mem_.construct<char>(boost::interprocess::anonymous_instance)[sz+sizeof(std::size_t)]();
                std::memcpy(dataPtr, reinterpret_cast<char const *>(&sz), sizeof(std::size_t));
                std::memcpy(dataPtr+sizeof(std::size_t), enc.data(), sz);
                ptr->data = (dataPtr-reinterpret_cast<char *>(ptr));
                return {
                    ptr
                    , std::move(data)
                };
            }
        }
        void destroyItem(ItemType &p) {
            if (p.ptr) {
                if constexpr (!std::is_trivially_copyable_v<T>) {
                    if (p.ptr->data != 0) {
                        mem_.destroy_ptr(reinterpret_cast<char *>(p.ptr)+p.ptr->data);
                    }
                }
                mem_.destroy_ptr(p.ptr);
            }
        }
    };

    template <class T, BoostSharedMemoryChainExtraDataProtectionStrategy EDPS>
    class LockFreeInBoostSharedMemoryChain<
        T
        , BoostSharedMemoryChainFastRecoverSupport::ByOffset
        , EDPS 
    > : public LockFreeInBoostSharedMemoryChainBase<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset> {
    private:
        IPCMutexWrapper<EDPS> mutex_;
#ifdef _MSC_VER
        boost::interprocess::managed_windows_shared_memory mem_;
#else
        boost::interprocess::managed_shared_memory mem_;
#endif
        BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset> *head_;
    public:
        static constexpr BoostSharedMemoryChainFastRecoverSupport FastRecoverySupport = BoostSharedMemoryChainFastRecoverSupport::ByOffset;
        static constexpr BoostSharedMemoryChainExtraDataProtectionStrategy ExtraDataProtectionStrategy = EDPS;
        using ItemType = BoostSharedMemoryChainItem<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset>;
    private:
        inline ItemType fromPtr(BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset> *ptr) const {
            if constexpr (std::is_trivially_copyable_v<T>) {
                return ItemType {
                    ptr
                    , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                    , ParsedNodeData<T> {}
                };
            } else {
                if (ptr) {
                    if (ptr->data == 0) {
                        return ItemType {
                            ptr
                            , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                            , ParsedNodeData<T> {}
                        };
                    }
                    char const *dataPtr = reinterpret_cast<char const *>(ptr)+ptr->data;
                    std::size_t sz;
                    std::memcpy(&sz, dataPtr, sizeof(std::size_t));
                    return ItemType {
                        ptr
                        , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                        , basic::bytedata_utils::RunDeserializer<T>::apply(std::string_view {dataPtr+sizeof(std::size_t), sz})
                    };
                } else {
                    return ItemType {
                        ptr
                        , reinterpret_cast<char const *>(ptr)-reinterpret_cast<char const *>(head_)
                        , ParsedNodeData<T> {}
                    };
                }
            }
        }
    public:
        LockFreeInBoostSharedMemoryChain(std::string const &name, std::size_t sharedMemorySize) :
            mutex_(name) 
            , mem_(
                boost::interprocess::open_or_create
                , name.c_str()
                , sharedMemorySize
            )
            , head_(nullptr)
        {
            head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset>>(boost::interprocess::unique_instance)();
            if constexpr (!std::is_trivially_copyable_v<T>) {
                head_->data = 0;
            }
        }
        ItemType head(void *) {
            return fromPtr(
                head_
            );
        }
        ItemType loadUntil(void *env, std::string const &id) {
            if (id.length() < sizeof(std::ptrdiff_t)) {
                return head(env);
            }
            auto offset = *reinterpret_cast<std::ptrdiff_t const *>(id.data());
            return fromPtr(
                reinterpret_cast<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset> *>(reinterpret_cast<char *>(head_)+offset)
            );
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            if (!current.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("FetchNext on nullptr");
            }
            auto next = current.ptr->next.load(std::memory_order_acquire);
            if (next != 0) {
                return fromPtr(
                    reinterpret_cast<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset> *>(reinterpret_cast<char *>(current.ptr)+next)
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
            return std::atomic_compare_exchange_strong<std::ptrdiff_t>(
                &(current.ptr->next)
                , &x
                , (reinterpret_cast<char const *>(toBeWritten.ptr)-reinterpret_cast<char const *>(current.ptr))
            );
        }
        template <class ExtraData>
        void saveExtraData(std::string const &key, ExtraData const &data) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports storing extra data only if enabled in template signature");
            if constexpr (std::is_trivially_copyable_v<ExtraData>) {
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
            } else {
                auto encoded = basic::bytedata_utils::RunSerializer<ExtraData>::apply(data);
                saveExtraBytes(key, basic::ByteData {std::move(encoded)});
            }
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports loading extra data only if enabled in template signature");
            if constexpr (std::is_trivially_copyable_v<ExtraData>) {
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
            } else {
                auto b = loadExtraBytes(key);
                if (b) {
                    return basic::bytedata_utils::RunDeserializer<ExtraData>::apply(b->content);
                } else {
                    return std::nullopt;
                }
            }
        }
        void saveExtraBytes(std::string const &key, basic::ByteData const &data) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports storing extra data only if enabled in template signature");
            if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory) {
                std::size_t sz = data.content.length();
                auto *dataPtr = mem_.construct<char>(boost::interprocess::anonymous_instance)[sz+sizeof(std::size_t)]();
                std::memcpy(dataPtr+sizeof(std::size_t), data.content.data(), sz);
                std::memcpy(dataPtr, reinterpret_cast<char const *>(&sz), sizeof(std::size_t));
                auto *loc = mem_.find_or_construct<std::atomic<std::ptrdiff_t>>(key.c_str())(0);
                std::ptrdiff_t newVal = dataPtr-reinterpret_cast<char *>(loc);
                loc->store(newVal, std::memory_order_release);
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected) {
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
        std::optional<basic::ByteDataView> loadExtraBytes(std::string const &key) {
            static_assert(EDPS != BoostSharedMemoryChainExtraDataProtectionStrategy::DontSupportExtraData, "LockFreeInBoostSharedMemoryChain supports loading extra data only if enabled in template signature");
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
            } else if constexpr (EDPS == BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected) {
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
        ItemType createItemFromData(std::string const &id, T &&data) {
            if constexpr (std::is_trivially_copyable_v<T>) {
                return fromPtr(
                    mem_.construct<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset>>(boost::interprocess::anonymous_instance)(std::move(data))
                );
            } else {
                auto enc = basic::bytedata_utils::RunSerializer<T>::apply(data);
                std::size_t sz = enc.length();
                auto *ptr = mem_.construct<BoostSharedMemoryStorageItem<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset>>(boost::interprocess::anonymous_instance)(0);
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
        void destroyItem(ItemType &p) {
            if (p.ptr) {
                if constexpr (!std::is_trivially_copyable_v<T>) {
                    if (p.ptr->data != 0) {
                        mem_.destroy_ptr(reinterpret_cast<char const *>(p.ptr)+p.ptr->data);
                    }
                }
                mem_.destroy_ptr(p.ptr);
            }
        }
    };

}}}}}

#endif