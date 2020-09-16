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
    struct StorageItem {
        char id[36]; //for uuid, please notice that the end '\0' is not included
        T data;
        std::atomic<StorageItem<T> *> next;
    };
    
    template <class T>
    using ChainItem = StorageItem<T> *;
    
    template <class T, class Enable=std::enable_if_t<std::is_pod_v<T>, void>>
    class LockFreeInBoostSharedMemoryChain {
    private:
        boost::interprocess::managed_shared_memory mem_;
        StorageItem<T> *head_;
    public:
        using ItemType = ChainItem<T>;
        LockFreeInBoostSharedMemoryChain(std::string const &name, std::size_t sharedMemorySize) : 
            mem_(
                boost::interprocess::open_or_create
                , name.c_str()
                , sharedMemorySize
            )
            , head_(nullptr)
        {
            head_ = mem_.find_or_construct<StorageItem<T>>("head")();
            head_->id[0] = '\0';
            head_->data = T {};
            head_->next = nullptr;
        }
        ItemType head(void *) {
            return head_;
        }
        ItemType loadUntil(void *, std::string const &id) {
            if (id == "" || id == "head") {
                return head_;
            }
            return mem_.find<StorageItem<T>>(id.c_str()).first;
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            if (!current) {
                throw LockFreeInBoostSharedMemoryChainException("FetchNext on nullptr");
            }
            StorageItem<T> *p = current->next.load(std::memory_order_acquire);
            if (p) {
                return p;
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
            StorageItem<T> *p = nullptr;
            return std::atomic_compare_exchange_strong<StorageItem<T> *>(
                &(current->next)
                , &p
                , toBeWritten
            );
        }
        template <class ExtraData>
        void saveExtraData(std::string const &key, ExtraData const &data) {
            if constexpr (std::is_pod_v<ExtraData>) {
                *(mem_.find_or_construct<ExtraData>(key.c_str())()) = data;
            }
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            if constexpr (std::is_pod_v<ExtraData>) {
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
            auto *p = mem_.construct<StorageItem<T>>(id.c_str())();
            std::memcpy(p->id, id.c_str(), std::min(36, id.length()));
            p->data = std::move(data);
            p->next = nullptr;
            return p;
        }
        void destroyItem(ItemType &p) {
            mem_.destroy_ptr(p);
        }
    };

}}}}}

#endif