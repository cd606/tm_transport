#ifndef TM_KIT_TRANSPORT_LOCK_FREE_IN_MEMORY_SHARED_CHAIN_HPP_
#define TM_KIT_TRANSPORT_LOCK_FREE_IN_MEMORY_SHARED_CHAIN_HPP_

#include <tm_kit/basic/simple_shared_chain/ChainReader.hpp>
#include <tm_kit/basic/simple_shared_chain/ChainWriter.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <atomic>

//The main purpose of this implementation is to test the template flexibility
//of chain implementations. It can also be used in single-process communications
//, though

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace lock_free_in_memory_shared_chain {
    class LockFreeInMemoryChainException : public std::runtime_error {
    public:
        LockFreeInMemoryChainException(std::string const &s) : std::runtime_error(s) {}
    };

    template <class T>
    struct StorageItem {
        std::string id;
        T data;
        std::atomic<StorageItem<T> *> next;

        StorageItem() : id(""), data(), next(nullptr) {}
        StorageItem(std::string const &s, T const &d, StorageItem<T> *p) : id(s), data(d), next(p) {}
        StorageItem(std::string const &s, T &&d, StorageItem<T> *p) : id(s), data(std::move(d)), next(p) {}
    };
    
    template <class T>
    using ChainItem = StorageItem<T> *;
    
    template <class T>
    class LockFreeInMemoryChain {
    private:
        StorageItem<T> head_;
    public:
        using ItemType = ChainItem<T>;
        LockFreeInMemoryChain() : head_() {
        }
        ItemType head(void *) {
            return &head_;
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            if (!current) {
                throw LockFreeInMemoryChainException("FetchNext on nullptr");
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
                throw LockFreeInMemoryChainException("AppendAfter on nullptr");
            }
            if (!toBeWritten) {
                throw LockFreeInMemoryChainException("AppendAfter trying to append nullptr");
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
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            return std::nullopt;
        }
    };

}}}}}

#endif