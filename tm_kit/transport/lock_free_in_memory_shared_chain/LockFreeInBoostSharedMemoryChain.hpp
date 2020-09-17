#ifndef TM_KIT_TRANSPORT_LOCK_FREE_IN_BOOST_SHARED_MEMORY_SHARED_CHAIN_HPP_
#define TM_KIT_TRANSPORT_LOCK_FREE_IN_BOOST_SHARED_MEMORY_SHARED_CHAIN_HPP_

#include <tm_kit/basic/simple_shared_chain/ChainReader.hpp>
#include <tm_kit/basic/simple_shared_chain/ChainWriter.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <atomic>

#include <boost/interprocess/managed_shared_memory.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace lock_free_in_memory_shared_chain {
    class LockFreeInBoostSharedMemoryChainException : public std::runtime_error {
    public:
        LockFreeInBoostSharedMemoryChainException(std::string const &s) : std::runtime_error(s) {}
    };

    template <class T>
    struct BoostSharedMemoryStorageItem {
        char id[36]; //for uuid, please notice that the end '\0' is not included
        T data;
        std::atomic<std::ptrdiff_t> next;
        BoostSharedMemoryStorageItem() : data(), next(0) {
            std::memset(id, 0, 36);
        }
    };
    
    template <class T>
    using BoostSharedMemoryChainItem = BoostSharedMemoryStorageItem<T> *;
    
    template <class T, class Enable=std::enable_if_t<std::is_standard_layout_v<T>, void>>
    class LockFreeInBoostSharedMemoryChain {
    private:
        boost::interprocess::managed_shared_memory mem_;
        BoostSharedMemoryStorageItem<T> *head_;
    public:
        using ItemType = BoostSharedMemoryChainItem<T>;
        LockFreeInBoostSharedMemoryChain(std::string const &name, std::size_t sharedMemorySize) : 
            mem_(
                boost::interprocess::open_or_create
                , name.c_str()
                , sharedMemorySize
            )
            , head_(nullptr)
        {
            head_ = mem_.find_or_construct<BoostSharedMemoryStorageItem<T>>("head")();
        }
        ItemType head(void *) {
            return head_;
        }
        ItemType loadUntil(void *, std::string const &id) {
            if (id == "" || id == "head") {
                return head_;
            }
            return mem_.find<BoostSharedMemoryStorageItem<T>>(id.c_str()).first;
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
            *(mem_.find_or_construct<ExtraData>(key.c_str())()) = data;
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            static_assert(std::is_standard_layout_v<ExtraData>, "LockFreeInBoostSharedMemoryChain only supports loading standard layout extraData objects");
            auto *p = mem_.find<ExtraData>(key.c_str()).first;
            if (p) {
                return *p;
            } else {
                return std::nullopt;
            }
        }
        ItemType createItemFromData(std::string const &id, T &&data) {
            auto *p = mem_.construct<BoostSharedMemoryStorageItem<T>>(id.c_str())();
            std::memcpy(p->id, id.c_str(), std::min<std::size_t>(36, id.length()));
            p->data = std::move(data);
            p->next = 0;
            return p;
        }
        void destroyItem(ItemType &p) {
            mem_.destroy_ptr(p);
        }
    };

}}}}}

#endif