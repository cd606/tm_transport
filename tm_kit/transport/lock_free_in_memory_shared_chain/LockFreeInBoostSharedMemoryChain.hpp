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
    struct SharedMemoryChainComponent {};

    class LockFreeInBoostSharedMemoryChainException : public std::runtime_error {
    public:
        LockFreeInBoostSharedMemoryChainException(std::string const &s) : std::runtime_error(s) {}
    };

    enum class BoostSharedMemoryChainFastRecoverSupport {
        ByName, ByOffset
    };

    template <class T>
    using ActualNodeData = std::conditional_t<std::is_trivially_copyable_v<T>,T,std::ptrdiff_t>;
   
    template <class T>
    struct BoostSharedMemoryStorageItem {
        ActualNodeData<T> data;
        std::atomic<std::ptrdiff_t> next;
        BoostSharedMemoryStorageItem() : data(), next(0) {
        }
        BoostSharedMemoryStorageItem(ActualNodeData<T> &&d) : data(std::move(d)), next(0) {
        }
    };
    
    template <class T>
    using ParsedNodeData = std::conditional_t<std::is_trivially_copyable_v<T>,basic::VoidStruct,std::optional<T>>;
    template <class T, BoostSharedMemoryChainFastRecoverSupport FRS>
    struct BoostSharedMemoryChainItem {};
    template <class T>
    struct BoostSharedMemoryChainItem<T, BoostSharedMemoryChainFastRecoverSupport::ByName> {
        BoostSharedMemoryStorageItem<T> *ptr;
        std::string id;
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
        BoostSharedMemoryStorageItem<T> *ptr;
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
        BoostSharedMemoryStorageItem<T> *head_;
    public:
        using StorageIDType = std::string;
        using DataType = T;
        static constexpr BoostSharedMemoryChainFastRecoverSupport FastRecoverySupport = BoostSharedMemoryChainFastRecoverSupport::ByName;
        static constexpr BoostSharedMemoryChainExtraDataProtectionStrategy ExtraDataProtectionStrategy = EDPS;
        using ItemType = BoostSharedMemoryChainItem<T,BoostSharedMemoryChainFastRecoverSupport::ByName>;
        static constexpr bool SupportsExtraData = true;
    private:
        static inline ItemType fromIDAndPtr(std::string const &id, BoostSharedMemoryStorageItem<T> *ptr) {
            if constexpr (std::is_trivially_copyable_v<T>) {
                return ItemType {
                    ptr
                    , id
                    , ParsedNodeData<T> {}
                };
            } else {
                if (ptr) {
                    if (ptr->data == 0) {
                        return ItemType {
                            ptr
                            , id
                            , ParsedNodeData<T> {}
                        };
                    }
                    char const *dataPtr = reinterpret_cast<char const *>(ptr)+ptr->data;
                    std::size_t sz;
                    std::memcpy(&sz, dataPtr, sizeof(std::size_t));
                    return ItemType {
                        ptr
                        , id
                        , basic::bytedata_utils::RunDeserializer<T>::apply(std::string_view {dataPtr+sizeof(std::size_t), sz})
                    };
                } else {
                    return ItemType {
                        ptr
                        , id
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
            head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T>>(boost::interprocess::unique_instance)();
            if constexpr (!std::is_trivially_copyable_v<T>) {
                head_->data = 0;
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
                id, mem_.find<BoostSharedMemoryStorageItem<T>>(id.c_str()).first
            );
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            if (!current.ptr) {
                throw LockFreeInBoostSharedMemoryChainException("FetchNext on nullptr");
            }
            auto next = current.ptr->next.load(std::memory_order_acquire);
            if (next != 0) {
                auto *p = reinterpret_cast<BoostSharedMemoryStorageItem<T> *>(reinterpret_cast<char *>(current.ptr)+next);
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
            return ret;
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
        template <class Env>
        static StorageIDType newStorageID() {
            return Env::id_to_string(Env::new_id());
        }
        template <class Env>
        static std::string newStorageIDAsString() {
            return newStorageID<Env>();
        }
        ItemType formChainItem(StorageIDType const &id, T &&data) {
            if constexpr (std::is_trivially_copyable_v<T>) {
                return fromIDAndPtr(
                    id
                    , mem_.construct<BoostSharedMemoryStorageItem<T>>(id.c_str())(std::move(data))
                );
            } else {
                auto enc = basic::bytedata_utils::RunSerializer<T>::apply(data);
                std::size_t sz = enc.length();
                auto *ptr = mem_.construct<BoostSharedMemoryStorageItem<T>>(id.c_str())(0);
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
        void destroyItem(ItemType &&p) {
            if (p.ptr) {
                if constexpr (!std::is_trivially_copyable_v<T>) {
                    if (p.ptr->data != 0) {
                        mem_.destroy_ptr(reinterpret_cast<char *>(p.ptr)+p.ptr->data);
                    }
                }
                mem_.destroy_ptr(p.ptr);
            }
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
        BoostSharedMemoryStorageItem<T> *head_;
    public:
        using StorageIDType = std::ptrdiff_t;
        using DataType = T;
        static constexpr BoostSharedMemoryChainFastRecoverSupport FastRecoverySupport = BoostSharedMemoryChainFastRecoverSupport::ByOffset;
        static constexpr BoostSharedMemoryChainExtraDataProtectionStrategy ExtraDataProtectionStrategy = EDPS;
        using ItemType = BoostSharedMemoryChainItem<T,BoostSharedMemoryChainFastRecoverSupport::ByOffset>;
        static constexpr bool SupportsExtraData = true;
    private:
        inline ItemType fromPtr(BoostSharedMemoryStorageItem<T> *ptr) const {
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
            head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T>>(boost::interprocess::unique_instance)();
            if constexpr (!std::is_trivially_copyable_v<T>) {
                head_->data = 0;
            }
        }
        ItemType head(void *) {
            return fromPtr(
                head_
            );
        }
        ItemType loadUntil(void *env, StorageIDType const &id) {
            return fromPtr(
                reinterpret_cast<BoostSharedMemoryStorageItem<T> *>(reinterpret_cast<char *>(head_)+id)
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
                    reinterpret_cast<BoostSharedMemoryStorageItem<T> *>(reinterpret_cast<char *>(current.ptr)+next)
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
            return ret;
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
        template <class Env>
        static StorageIDType newStorageID() {
            return 0;
        }
        template <class Env>
        static std::string newStorageIDAsString() {
            return "";
        }
        ItemType formChainItem(StorageIDType const &notUsed, T &&data) {
            if constexpr (std::is_trivially_copyable_v<T>) {
                return fromPtr(
                    mem_.construct<BoostSharedMemoryStorageItem<T>>(boost::interprocess::anonymous_instance)(std::move(data))
                );
            } else {
                auto enc = basic::bytedata_utils::RunSerializer<T>::apply(data);
                std::size_t sz = enc.length();
                auto *ptr = mem_.construct<BoostSharedMemoryStorageItem<T>>(boost::interprocess::anonymous_instance)(0);
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
        void destroyItem(ItemType &&p) {
            if (p.ptr) {
                if constexpr (!std::is_trivially_copyable_v<T>) {
                    if (p.ptr->data != 0) {
                        mem_.destroy_ptr(reinterpret_cast<char const *>(p.ptr)+p.ptr->data);
                    }
                }
                mem_.destroy_ptr(p.ptr);
            }
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
    };

}}}}}

#endif