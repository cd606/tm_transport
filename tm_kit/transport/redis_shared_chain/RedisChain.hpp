#ifndef TM_KIT_TRANSPORT_REDIS_SHARED_CHAIN_HPP_
#define TM_KIT_TRANSPORT_REDIS_SHARED_CHAIN_HPP_

#include <tm_kit/basic/simple_shared_chain/ChainReader.hpp>
#include <tm_kit/basic/simple_shared_chain/ChainWriter.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/SerializationHelperMacros.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>

#ifdef _MSC_VER
#include <winsock2.h>
#undef min
#undef max
#endif
#include <hiredis/hiredis.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace redis_shared_chain {
    #define RedisChainItemFields \
        ((std::string, id)) \
        ((std::optional<T>, data)) \
        ((std::string, nextID)) 

    TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT(((typename, T)), ChainItem, RedisChainItemFields);
}}}}} 

TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT_SERIALIZE_NO_FIELD_NAMES(((typename, T)), dev::cd606::tm::transport::redis_shared_chain::ChainItem, RedisChainItemFields);

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace redis_shared_chain {
    struct RedisChainComponent {};
    
    struct RedisChainConfiguration {
        std::string redisServerAddr="127.0.0.1:6379";
        std::string headKey="";
        std::string chainPrefix="shared_chain_test";
        std::string dataPrefix="shared_chain_test_data";
        std::string extraDataPrefix="shared_chain_test_extra_data";
        
        RedisChainConfiguration() = default;
        RedisChainConfiguration(RedisChainConfiguration const &) = default;
        RedisChainConfiguration &operator=(RedisChainConfiguration const &) = default;
        RedisChainConfiguration(RedisChainConfiguration &&) = default;
        RedisChainConfiguration &operator=(RedisChainConfiguration &&) = default;
        
        RedisChainConfiguration &RedisServerAddr(std::string const &addr) {
            redisServerAddr = addr;
            return *this;
        }
        RedisChainConfiguration &HeadKey(std::string const &key) {
            headKey = key;
            return *this;
        }
        RedisChainConfiguration &ChainPrefix(std::string const &p) {
            chainPrefix = p;
            return *this;
        }
        RedisChainConfiguration &DataPrefix(std::string const &p) {
            dataPrefix = p;
            return *this;
        }
        RedisChainConfiguration &ExtraDataPrefix(std::string const &p) {
            extraDataPrefix = p;
            return *this;
        }
    };

    class RedisChainException : public std::runtime_error {
    public:
        RedisChainException(std::string const &s) : std::runtime_error(s) {}
    };

    template <class T>
    class RedisChain {
    private:
        const RedisChainConfiguration configuration_;
        redisContext *redisCtx_;
        std::mutex redisMutex_;
        std::optional<ByteDataHookPair> hookPair_;

        template <class X>
        std::optional<X> parseRedisData(redisReply *r) {
            std::optional<X> x {X {}};
            if (hookPair_ && hookPair_->wireToUser) {
                auto parsed = hookPair_->wireToUser->hook(basic::ByteDataView {std::string_view(r->str, r->len)});
                if (!parsed) {
                    return std::nullopt;
                }
                if (basic::bytedata_utils::RunDeserializer<X>::applyInPlace(
                    *x, std::string_view(parsed->content)
                )) {
                    return std::move(x);
                } else {
                    return std::nullopt;
                }
            } else {
                if (basic::bytedata_utils::RunDeserializer<X>::applyInPlace(
                    *x, std::string_view(r->str, r->len)
                )) {
                    return std::move(x);
                } else {
                    return std::nullopt;
                }
            }
        }
        template <class X>
        std::string serialize(X const &x) {
            if (hookPair_ && hookPair_->userToWire) {
                return hookPair_->userToWire->hook(
                    basic::ByteData {basic::bytedata_utils::RunSerializer<X>::apply(x)}
                ).content;
            } else {
                return basic::bytedata_utils::RunSerializer<X>::apply(x);
            }
        }
    public:
        using StorageIDType = std::string;
        using DataType = T;
        using ItemType = ChainItem<T>;
        static constexpr bool SupportsExtraData = true;
        RedisChain(RedisChainConfiguration const &configuration, std::optional<ByteDataHookPair> hookPair=std::nullopt) :
            configuration_(configuration)
            , redisCtx_(nullptr), redisMutex_()
            , hookPair_(hookPair)
        {
            auto idx = configuration_.redisServerAddr.find(':');
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                redisCtx_ = redisConnect(
                    configuration_.redisServerAddr.substr(
                        0, idx
                    ).c_str()
                    , (
                        idx == std::string::npos
                        ?
                        6379
                        :
                        std::stoi(
                            configuration_.redisServerAddr.substr(idx+1)
                        )
                    )
                );
            }
        }
        ~RedisChain() {
            std::lock_guard<std::mutex> _(redisMutex_);
            if (redisCtx_) {
                redisFree(redisCtx_);
            }
        }
        ItemType head(void *) {
            static const std::string headKeyStr = configuration_.chainPrefix+":"+configuration_.headKey;
            static const std::string luaStr = "local x = redis.call('GET',KEYS[1]); if x then return x else redis.call('SET',KEYS[1],''); return '' end";
            redisReply *r = nullptr;
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                r = (redisReply *) redisCommand(
                    redisCtx_, "EVAL %s 1 %s", luaStr.c_str(), headKeyStr.c_str()
                );
            }
            if (r == nullptr || r->type != REDIS_REPLY_STRING) {
                if (r != nullptr) {
                    freeReplyObject((void *) r);
                }
                throw RedisChainException("head: Redis chain fetch head error for "+headKeyStr);
            }
            std::string nextID {r->str, r->len};
            freeReplyObject((void *) r);
            return ItemType {configuration_.headKey, T{}, std::move(nextID)};
        }
        ItemType loadUntil(void *env, StorageIDType const &id) {
            if (id == "") {
                return head(env);
            }
            std::string chainKey = configuration_.chainPrefix+":"+id;
            std::string dataKey = configuration_.dataPrefix+":"+id;
            redisReply *r = nullptr;
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                r = (redisReply *) redisCommand(
                    redisCtx_, "GET %s", dataKey.c_str()
                );
            }
            if (r == nullptr || r->type != REDIS_REPLY_STRING) {
                if (r != nullptr) {
                    freeReplyObject((void *) r);
                }
                throw RedisChainException("loadUntil: Redis chain fetch error for "+dataKey);
            }
            std::size_t l = r->len;
            auto data = parseRedisData<T>(r);
            freeReplyObject((void *) r);
            r = nullptr;
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                r = (redisReply *) redisCommand(
                    redisCtx_, "GET %s", chainKey.c_str()
                );
            }
            if (r == nullptr || r->type != REDIS_REPLY_STRING) {
                if (r != nullptr) {
                    freeReplyObject((void *) r);
                }
                throw RedisChainException("loadUntil: Redis chain fetch error for "+chainKey);
            }
            std::string nextID {r->str, r->len};
            freeReplyObject((void *) r);
            return ItemType {id, std::move(data), std::move(nextID)};
        }
        std::optional<ItemType> fetchNext(ItemType const &current) {
            std::string nextID = current.nextID;
            if (nextID == "") {
                std::string chainKey = configuration_.chainPrefix+":"+current.id;
                redisReply *r = nullptr;
                {
                    std::lock_guard<std::mutex> _(redisMutex_);
                    r = (redisReply *) redisCommand(
                        redisCtx_, "GET %s", chainKey.c_str()
                    );
                }
                if (r == nullptr || r->type != REDIS_REPLY_STRING) {
                    if (r != nullptr) {
                        freeReplyObject((void *) r);
                    }
                    throw RedisChainException("fetchNext: Redis chain fetch error for "+chainKey);
                }
                nextID = std::string {r->str, r->len};
                freeReplyObject((void *) r);
            }
            if (nextID == "") {
                return std::nullopt;
            }
            std::string chainKey = configuration_.chainPrefix+":"+nextID;
            std::string dataKey = configuration_.dataPrefix+":"+nextID;
            redisReply *r = nullptr;
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                r = (redisReply *) redisCommand(
                    redisCtx_, "GET %s", dataKey.c_str()
                );
            }
            if (r == nullptr || r->type != REDIS_REPLY_STRING) {
                if (r != nullptr) {
                    freeReplyObject((void *) r);
                }
                throw RedisChainException("fetchNext: Redis chain fetch error for "+dataKey);
            }
            std::size_t l = r->len;
            auto data = parseRedisData<T>(r);
            freeReplyObject((void *) r);
            r = nullptr;
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                r = (redisReply *) redisCommand(
                    redisCtx_, "GET %s", chainKey.c_str()
                );
            }
            if (r == nullptr || r->type != REDIS_REPLY_STRING) {
                if (r != nullptr) {
                    freeReplyObject((void *) r);
                }
                throw RedisChainException("fetchNext: Redis chain fetch error for "+chainKey);
            }
            std::string nextNextID {r->str, r->len};
            freeReplyObject((void *) r);
            return ItemType {nextID, std::move(data), std::move(nextNextID)};
        }
        bool idIsAlreadyOnChain(std::string const &id) {
            std::string chainKey = configuration_.chainPrefix+":"+id;
            redisReply *r = nullptr;
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                r = (redisReply *) redisCommand(
                    redisCtx_, "GET %s", chainKey.c_str()
                );
            }
            bool ret = (r != nullptr && r->type == REDIS_REPLY_STRING);
            freeReplyObject((void *) r);
            return ret;
        }
        bool appendAfter(ItemType const &current, ItemType &&toBeWritten) {
            static const std::string luaStr = "local x = redis.call('GET',KEYS[1]); local y = redis.call('GET',KEYS[2]); local z = redis.call('GET',KEYS[3]); if x == '' and not y and not z then redis.call('SET',KEYS[1],ARGV[1]); redis.call('SET',KEYS[2],ARGV[2]); redis.call('SET',KEYS[3],''); return 1 else return 0 end";
            if (current.nextID != "") {
                return false;
            }
            if (!toBeWritten.data) {
                throw RedisChainException("appendAfter: Cannot append a new item whose data is empty");
            }
            if (toBeWritten.nextID != "") {
                throw RedisChainException("appendAfter: Cannot append a new item whose nextID is already non-empty");
            }
            std::string currentChainKey = configuration_.chainPrefix+":"+current.id;
            std::string newDataKey = configuration_.dataPrefix+":"+toBeWritten.id;
            std::string newChainKey = configuration_.chainPrefix+":"+toBeWritten.id;
            std::string newData = serialize<T>(std::move(*(toBeWritten.data)));
            redisReply *r = nullptr;
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                r = (redisReply *) redisCommand(
                    redisCtx_, "EVAL %s 3 %s %s %s %s %b", luaStr.c_str()
                        , currentChainKey.c_str(), newDataKey.c_str(), newChainKey.c_str()
                        , toBeWritten.id.c_str(), newData.c_str(), newData.length()
                );
            }
            if (r == nullptr || r->type != REDIS_REPLY_INTEGER) {
                if (r != nullptr) {
                    freeReplyObject((void *) r);
                }
                throw RedisChainException("appendAfter: Redis chain compare set error for "+currentChainKey+","+newDataKey+","+newChainKey);
            }
            bool ret = (r->integer != 0);
            freeReplyObject((void *) r);
            return ret;
        }
        bool appendAfter(ItemType const &current, std::vector<ItemType> &&toBeWritten) {
            if (toBeWritten.empty()) {
                return true;
            }
            if (toBeWritten.size() == 1) {
                return appendAfter(current, std::move(toBeWritten[0]));
            }
            if (current.nextID != "") {
                return false;
            }
            if (!toBeWritten[0].data) {
                throw RedisChainException("appendAfter: Cannot append a new first item whose data is empty");
            }
            if (toBeWritten.back().nextID != "") {
                throw RedisChainException("appendAfter: Cannot append a new last item whose nextID is already non-empty");
            }

            std::size_t newItemCount = toBeWritten.size();
            std::ostringstream luaStrOss;
            luaStrOss << "local x = redis.call('GET',KEYS[1])";
            for (auto ii=0; ii<newItemCount; ++ii) {
                luaStrOss << "; local y" << ii << " = redis.call('GET',KEYS[" << (ii+1)*2 << "])";
                luaStrOss << "; local z" << ii << " = redis.call('GET',KEYS[" << (ii+1)*2+1 << "])";
            }
            luaStrOss << "; if x == ''";
            for (auto ii=0; ii<newItemCount; ++ii) {
                luaStrOss << " and not y" << ii << " and not z" << ii;
            }
            luaStrOss << " then redis.call('SET',KEYS[1],ARGV[1])";
            for (auto ii=0; ii<newItemCount; ++ii) {
                luaStrOss << "; redis.call('SET',KEYS[" << (ii+1)*2 << "],ARGV[" << (ii+1)*2 << "])";
                luaStrOss << "; redis.call('SET',KEYS[" << (ii+1)*2+1 << "],ARGV[" << (ii+1)*2+1 << "])";
            }
            luaStrOss << "; return 1 else return 0 end";

            std::string luaStr = luaStrOss.str();

            std::size_t argvCount = 2*(2*newItemCount+1)+3; //the three others are "EVAL", luaString, and key count 
            using pChar = const char *;
            const char **argv = new pChar[argvCount];
            std::size_t *argvLen = new std::size_t[argvCount];
            std::string cmd = "EVAL";
            argv[0] = cmd.c_str();
            argvLen[0] = cmd.length();
            argv[1] = luaStr.c_str();
            argvLen[1] = luaStr.length();
            std::string keyCount = std::to_string(2*newItemCount+1);
            argv[2] = keyCount.c_str();
            argvLen[2] = keyCount.length();

            std::string currentChainKey = configuration_.chainPrefix+":"+current.id;
            argv[3] = currentChainKey.c_str();
            argvLen[3] = currentChainKey.length();

            std::vector<std::string> newDataKeys;
            std::vector<std::string> newChainKeys;
            std::vector<std::string> newDatas;
            for (auto const &x : toBeWritten) {
                newDataKeys.push_back(configuration_.dataPrefix+":"+x.id);
                newChainKeys.push_back(configuration_.chainPrefix+":"+x.id);
                newDatas.push_back(serialize<T>(*(x.data)));
            }
            for (auto ii=0; ii<newItemCount; ++ii) {
                argv[2*ii+4] = newDataKeys[ii].c_str();
                argvLen[2*ii+4] = newDataKeys[ii].length();
                argv[2*ii+5] = newChainKeys[ii].c_str();
                argvLen[2*ii+5] = newChainKeys[ii].length();
            }
            argv[2*newItemCount+4] = toBeWritten[0].id.c_str();
            argvLen[2*newItemCount+4] = toBeWritten[0].id.length();
            for (auto ii=0; ii<newItemCount; ++ii) {
                argv[2*ii+2*newItemCount+5] = newDatas[ii].data();
                argvLen[2*ii+2*newItemCount+5] = newDatas[ii].length();
                argv[2*ii+2*newItemCount+6] = toBeWritten[ii].nextID.c_str();
                argvLen[2*ii+2*newItemCount+6] = toBeWritten[ii].nextID.length();
            }

            redisReply *r = nullptr;
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                r = (redisReply *) redisCommandArgv(
                    redisCtx_, argvCount, argv, argvLen
                );
            }
            delete[] argvLen;
            delete[] argv;

            if (r == nullptr || r->type != REDIS_REPLY_INTEGER) {
                if (r != nullptr) {
                    freeReplyObject((void *) r);
                }
                throw RedisChainException("appendAfter: Redis chain compare set error for "+currentChainKey+","+newDataKeys[0]+","+newChainKeys[0]);
            }
            bool ret = (r->integer != 0);
            freeReplyObject((void *) r);
            return ret;
        }
        //These two are helper functions to store and load data under extraDataPrefix
        template <class ExtraData>
        void saveExtraData(std::string const &key, ExtraData const &data) {
            std::string dataKey = configuration_.extraDataPrefix+":"+key;
            std::string dataStr = serialize<ExtraData>(data);
            redisReply *r = nullptr;
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                r = (redisReply *) redisCommand(
                    redisCtx_, "SET %s %b", dataKey.c_str(), dataStr.c_str(), dataStr.length()
                );
            }
            freeReplyObject((void *) r);
        }
        template <class ExtraData>
        std::optional<ExtraData> loadExtraData(std::string const &key) {
            std::string dataKey = configuration_.extraDataPrefix+":"+key;
            redisReply *r = nullptr;
            {
                std::lock_guard<std::mutex> _(redisMutex_);
                r = (redisReply *) redisCommand(
                    redisCtx_, "GET %s", dataKey.c_str()
                );
            }
            if (r == nullptr) {
                return std::nullopt;
            }
            if (r->type != REDIS_REPLY_STRING) {
                freeReplyObject((void *) r);
                return std::nullopt;
            }
            auto data = parseRedisData<ExtraData>(r);
            freeReplyObject((void *) r);
            return data;
        }
        template <class Env>
        static StorageIDType newStorageID() {
            return Env::id_to_string(Env::new_id());
        }
        template <class Env>
        static std::string newStorageIDAsString() {
            return newStorageID<Env>();
        }
        static ItemType formChainItem(StorageIDType const &itemID, T &&itemData) {
            return ItemType {
                itemID, {std::move(itemData)}, ""
            };
        }
        static std::vector<ItemType> formChainItems(std::vector<std::tuple<StorageIDType, T>> &&itemDatas) {
            std::vector<ItemType> ret;
            bool first = true;
            for (auto &&x: itemDatas) {
                auto *p = (first?nullptr:&(ret.back()));
                if (p) {
                    p->nextID = std::get<0>(x);
                }
                ret.push_back(formChainItem(std::get<0>(x), std::move(std::get<1>(x))));
                first = false;
            }
            return ret;
        }
        static StorageIDType extractStorageID(ItemType const &p) {
            return p.id;
        }
        static T const *extractData(ItemType const &p) {
            if (p.data) {
                return &(*(p.data));
            } else {
                return nullptr;
            }
        }
        static std::string_view extractStorageIDStringView(ItemType const &p) {
            return std::string_view {p.id};
        }
    };
}}}}}

#endif