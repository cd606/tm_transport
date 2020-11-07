using System;
using Here;
using StackExchange.Redis;
using PeterO.Cbor;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    public class RedisSharedChainConfiguration 
    {
        public string redisServerAddr {get; set;}
        public string headKey {get; set;}
        public string chainPrefix {get; set;}
        public string dataPrefix {get; set;}
        public string extraDataPrefix {get; set;}
        public static RedisSharedChainConfiguration Default(string commonPrefix = "", bool useDate = true)
        {
            var today = DateTime.Now.ToString("yyyy_mm_dd");
            return new RedisSharedChainConfiguration {
                redisServerAddr = "127.0.0.1:6379"
                , headKey = ""
                , chainPrefix = commonPrefix+"_"+(useDate?today:"")+"_chain"
                , dataPrefix = commonPrefix+"_"+(useDate?today:"")+"_data"
                , extraDataPrefix = commonPrefix+"_"+(useDate?today:"")+"_extra_data"
            };
        }
    }

    public class RedisSharedChainItem<T> 
    {
        public string id {get; set;}
        public T data {get; set;}
        public string nextID {get; set;}
        public override string ToString()
        {
            return $"RedisSharedChainItem{{id='{id}',data={data},nextID='{nextID}'}}";
        }
    }
    public class RedisSharedChain<T>
    {
        private RedisSharedChainConfiguration config;
        private IDatabase client;
        public RedisSharedChain(RedisSharedChainConfiguration config)
        {
            this.config = config;
            this.client = ConnectionMultiplexer.Connect(config.redisServerAddr).GetDatabase();
        }
        public RedisSharedChainItem<T> Head(T defaultData=default(T))
        {
            var headKeyStr = config.chainPrefix+":"+config.headKey;
            var luaStr = "local x = redis.call('GET',KEYS[1]); if x then return x else redis.call('SET',KEYS[1],''); return '' end";
            var redisReply = client.ScriptEvaluate(
                luaStr
                , new RedisKey[] {headKeyStr}
            );
            if (!redisReply.IsNull)
            {
                return new RedisSharedChainItem<T>() {
                    id = config.headKey
                    , data = defaultData
                    , nextID = (string) redisReply
                };
            }
            return null;
        }
        public RedisSharedChainItem<T> LoadUntil(string id, T defaultData=default(T))
        {
            if (id == null || id.Equals(""))
            {
                return Head(defaultData);
            }
            var chainKey = config.chainPrefix+":"+id;
            var dataKey = config.dataPrefix+":"+id;
            var dataReply = client.StringGet(dataKey);
            if (dataReply.IsNull)
            {
                throw new Exception("LoadUntil: Redis chain fetch error for "+dataKey);
            }
            var data = CborDecoder<T>.Decode(CBORObject.DecodeFromBytes(dataReply));
            if (!data.HasValue)
            {
                throw new Exception("LoadUntil: Redis chain parse error for "+dataKey);
            }
            var chainReply = client.StringGet(chainKey);
            if (chainReply.IsNull)
            {
                throw new Exception("LoadUntil: Redis chain fetch error for "+chainKey);
            }
            return new RedisSharedChainItem<T>() {
                id = id
                , data = data.Value
                , nextID = chainReply
            };
        }
        public Option<RedisSharedChainItem<T>> FetchNext(RedisSharedChainItem<T> current)
        {
            var nextID = current.nextID;
            string chainKey;
            RedisValue r;
            if (nextID.Equals("")) 
            {
                chainKey = config.chainPrefix+":"+current.id;
                r = client.StringGet(chainKey);
                if (r.IsNull)
                {
                    throw new Exception("FetchNext: Redis chain fetch error for "+chainKey);
                }
                nextID = r;
            }
            if (nextID.Equals("")) 
            {
                return Option.None;
            }
            chainKey = config.chainPrefix+":"+nextID;
            var dataKey = config.dataPrefix+":"+nextID;
            r = client.StringGet(dataKey);
            if (r.IsNull)
            {
                throw new Exception("FetchNext: Redis chain fetch error for "+dataKey);
            }
            var data = CborDecoder<T>.Decode(CBORObject.DecodeFromBytes(r));
            if (!data.HasValue)
            {
                throw new Exception("FetchNext: Redis chain parse error for "+dataKey);
            }
            r = client.StringGet(chainKey);
            if (r.IsNull)
            {
                throw new Exception("FetchNext: Redis chain fetch error for "+chainKey);
            }
            return new RedisSharedChainItem<T>() {
                id = nextID
                , data = data.Value
                , nextID = r
            };
        }
        public bool IDIsAlreadyOnChain(string id)
        {
            var chainKey = config.chainPrefix+":"+id;
            return !(client.StringGet(chainKey).IsNull);
        }
        public bool AppendAfter(RedisSharedChainItem<T> current, RedisSharedChainItem<T> toBeWritten)
        {
            const string luaStr = "local x = redis.call('GET',KEYS[1]); local y = redis.call('GET',KEYS[2]); local z = redis.call('GET',KEYS[3]); if x == '' and not y and not z then redis.call('SET',KEYS[1],ARGV[1]); redis.call('SET',KEYS[2],ARGV[2]); redis.call('SET',KEYS[3],''); return 1 else return 0 end";
            if (!current.nextID.Equals("")) 
            {
                return false;
            }
            if (!toBeWritten.nextID.Equals("")) 
            {
                throw new Exception("AppendAfter: Cannot append a new item whose nextID is already non-empty");
            }
            var currentChainKey = config.chainPrefix+":"+current.id;
            var newDataKey = config.dataPrefix+":"+toBeWritten.id;
            var newChainKey = config.chainPrefix+":"+toBeWritten.id;
            var newData = CborEncoder<T>.Encode(toBeWritten.data).EncodeToBytes();
            var r = client.ScriptEvaluate(
                luaStr
                , new RedisKey[] {currentChainKey, newDataKey, newChainKey}
                , new RedisValue[] {toBeWritten.id, newData}
            );
            if (r.IsNull)
            {
                throw new Exception("AppendAfter: Redis chain compare set error for "+currentChainKey+","+newDataKey+","+newChainKey);
            }
            return (((int) r) != 0);
        }
        public void SaveExtraData<ExtraData>(string key, ExtraData data) 
        {
            var dataKey = config.extraDataPrefix+":"+key;
            var dataStr = CborEncoder<ExtraData>.Encode(data).EncodeToBytes();
            client.StringSet(dataKey, dataStr);
        }
        public Option<ExtraData> LoadExtraData<ExtraData>(string key) 
        {
            var dataKey = config.extraDataPrefix+":"+key;
            var r = client.StringGet(dataKey);
            if (r.IsNull)
            {
                return Option.None;
            }
            return CborDecoder<ExtraData>.Decode(CBORObject.DecodeFromBytes(r));
        }
    }
}