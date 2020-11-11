using System;
using Here;
using dotnet_etcd;
using Google.Protobuf;
using StackExchange.Redis;
using PeterO.Cbor;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    public class EtcdSharedChainConfiguration 
    {
        public string etcd3URL {get; set;}
        public string headKey {get; set;}
        public bool saveDataOnSeparateStorage {get; set;}
        public string chainPrefix {get; set;}
        public string dataPrefix {get; set;}
        public string extraDataPrefix {get; set;}
        public string redisServerAddr {get; set;}
        public bool duplicateFromRedis {get; set;}
        public ushort redisTTLSeconds {get; set;}
        public bool automaticallyDuplicateToRedis {get; set;}
        public static EtcdSharedChainConfiguration Default(string commonPrefix = "", bool useDate = true)
        {
            var today = DateTime.Now.Date.ToString("yyyy_mm_dd");
            return new EtcdSharedChainConfiguration() {
                etcd3URL = "http://127.0.0.1:2379"
                , headKey = ""
                , saveDataOnSeparateStorage = true
                , chainPrefix = $"{commonPrefix}_{(useDate?today:"")}_chain"
                , dataPrefix = $"{commonPrefix}_{(useDate?today:"")}_data"
                , extraDataPrefix = $"{commonPrefix}_{(useDate?today:"")}_extra_data"
                , redisServerAddr = "127.0.0.1:6379"
                , duplicateFromRedis = false
                , redisTTLSeconds = 0
                , automaticallyDuplicateToRedis = false
            };
        }
    }
    public class EtcdSharedChainItem<T> 
    {
        public long revision {get; set;}
        public string id {get; set;}
        public T data {get; set;}
        public string nextID {get; set;}
        public override string ToString()
        {
            return $"EtcdSharedChainItem{{revision={revision},id='{id}',data={data.ToString()},nextID='{nextID}'}}";
        }
    }
    public class EtcdSharedChain<T>
    {
        private EtcdSharedChainConfiguration config;
        private EtcdClient client;
        private IDatabase redisClient;
        public EtcdSharedChain(EtcdSharedChainConfiguration config)
        {
            this.config = config;
            this.client = new EtcdClient(config.etcd3URL);
            if (config.duplicateFromRedis || config.automaticallyDuplicateToRedis)
            {
                this.redisClient = ConnectionMultiplexer.Connect(config.redisServerAddr).GetDatabase();
            }
        }
        public EtcdSharedChainItem<T> Head(T defaultData=default(T))
        {
            EtcdSharedChainItem<T> current;
            var h = config.chainPrefix+":"+config.headKey;
            if (config.duplicateFromRedis)
            {
                var redisReply = redisClient.StringGet(h);
                if (!redisReply.IsNull)
                {
                    var parsed = CborDecoder<(long,T,string)>.Decode(CBORObject.DecodeFromBytes(redisReply));
                    if (parsed.HasValue)
                    {
                        current = new EtcdSharedChainItem<T>() {
                            revision = parsed.Value.Item1
                            , id = config.headKey
                            , data = defaultData
                            , nextID = parsed.Value.Item3
                        };
                        return current;
                    }
                }
            }
            if (config.saveDataOnSeparateStorage)
            {
                var txnReq = new Etcdserverpb.TxnRequest();
                var cmp = new Etcdserverpb.Compare();
                cmp.Result = Etcdserverpb.Compare.Types.CompareResult.Greater;
                cmp.Target = Etcdserverpb.Compare.Types.CompareTarget.Version;
                cmp.Key = ByteString.CopyFromUtf8(h);
                cmp.Version = 0;
                txnReq.Compare.Add(cmp);
                var get = new Etcdserverpb.RangeRequest();
                get.Key = ByteString.CopyFromUtf8(h);
                var op = new Etcdserverpb.RequestOp();
                op.RequestRange = get;
                txnReq.Success.Add(op);
                var put = new Etcdserverpb.PutRequest();
                put.Key = ByteString.CopyFromUtf8(h);
                put.Value = ByteString.CopyFromUtf8("");
                op = new Etcdserverpb.RequestOp();
                op.RequestPut = put;
                txnReq.Failure.Add(op);
                get = new Etcdserverpb.RangeRequest();
                get.Key = ByteString.CopyFromUtf8(h);
                op = new Etcdserverpb.RequestOp();
                op.RequestRange = get;
                txnReq.Failure.Add(op);

                var etcdReply = client.Transaction(txnReq);
                if (etcdReply.Succeeded)
                {
                    current = new EtcdSharedChainItem<T>() {
                        revision = etcdReply.Responses[0].ResponseRange.Kvs[0].ModRevision
                        , id = config.headKey
                        , data = defaultData 
                        , nextID = etcdReply.Responses[0].ResponseRange.Kvs[0].Value.ToStringUtf8()
                    };
                }
                else
                {
                    current = new EtcdSharedChainItem<T>() {
                        revision = etcdReply.Responses[1].ResponseRange.Kvs[0].ModRevision
                        , id = config.headKey
                        , data = defaultData 
                        , nextID = ""
                    };
                }
            }
            else
            {
                if (defaultData == null)
                {
                    throw new Exception("Head error: must provide a non-null default value when accessing head of a chain that does not have separate data storage");
                }
                var txnReq = new Etcdserverpb.TxnRequest();
                var cmp = new Etcdserverpb.Compare();
                cmp.Result = Etcdserverpb.Compare.Types.CompareResult.Greater;
                cmp.Target = Etcdserverpb.Compare.Types.CompareTarget.Version;
                cmp.Key = ByteString.CopyFromUtf8(h);
                cmp.Version = 0;
                txnReq.Compare.Add(cmp);
                var get = new Etcdserverpb.RangeRequest();
                get.Key = ByteString.CopyFromUtf8(h);
                var op = new Etcdserverpb.RequestOp();
                op.RequestRange = get;
                txnReq.Success.Add(op);
                var put = new Etcdserverpb.PutRequest();
                put.Key = ByteString.CopyFromUtf8(h);
                put.Value = ByteString.CopyFrom(CborEncoder<(T,string)>.Encode((defaultData, "")).EncodeToBytes());
                op = new Etcdserverpb.RequestOp();
                op.RequestPut = put;
                txnReq.Failure.Add(op);
                get = new Etcdserverpb.RangeRequest();
                get.Key = ByteString.CopyFromUtf8(h);
                op = new Etcdserverpb.RequestOp();
                op.RequestRange = get;
                txnReq.Failure.Add(op);

                var etcdReply = client.Transaction(txnReq);
                if (etcdReply.Succeeded)
                {
                    var decoded = CborDecoder<(T,string)>.Decode(CBORObject.DecodeFromBytes(etcdReply.Responses[0].ResponseRange.Kvs[0].Value.ToByteArray()));
                    current = new EtcdSharedChainItem<T>() {
                        revision = etcdReply.Responses[0].ResponseRange.Kvs[0].ModRevision
                        , id = config.headKey
                        , data = defaultData 
                        , nextID = decoded.HasValue?decoded.Value.Item2:""
                    };
                }
                else
                {
                    current = new EtcdSharedChainItem<T>() {
                        revision = etcdReply.Responses[1].ResponseRange.Kvs[0].ModRevision
                        , id = config.headKey
                        , data = defaultData 
                        , nextID = ""
                    };
                }
            }
            return current;
        }
        public EtcdSharedChainItem<T> LoadUntil(string id, T defaultData=default(T))
        {
            if (id == null || id.Equals(""))
            {
                return Head(defaultData);
            }
            if (config.duplicateFromRedis)
            {
                var key = config.chainPrefix+":"+id;
                var r = redisClient.StringGet(key);
                if (!r.IsNull)
                {
                    var data = CborDecoder<(long,T,string)>.Decode(CBORObject.DecodeFromBytes(r));
                    if (data.HasValue)
                    {
                        return new EtcdSharedChainItem<T>()
                        {
                            revision = data.Value.Item1
                            , id = id
                            , data = data.Value.Item2
                            , nextID = data.Value.Item3
                        };
                    }
                }
            }
            if (config.saveDataOnSeparateStorage)
            {
                var txn = new Etcdserverpb.TxnRequest();
                var action = new Etcdserverpb.RequestOp();
                var get = new Etcdserverpb.RangeRequest();
                get.Key = ByteString.CopyFromUtf8(config.chainPrefix+":"+id);
                action.RequestRange = get;
                txn.Success.Add(action);
                action = new Etcdserverpb.RequestOp();
                get = new Etcdserverpb.RangeRequest();
                get.Key = ByteString.CopyFromUtf8(config.dataPrefix+":"+id);
                action.RequestRange = get;
                txn.Success.Add(action);

                var txnResp = client.Transaction(txn);
                if (txnResp.Succeeded)
                {
                    if (txnResp.Responses.Count < 2)
                    {
                        throw new Exception("LoadUntil Error! No record for "+config.chainPrefix+":"+id+" or "+config.dataPrefix+":"+id);
                    }
                    if (txnResp.Responses[0].ResponseRange.Kvs.Count == 0)
                    {
                        throw new Exception("LoadUntil Error! No record for "+config.chainPrefix+":"+id);
                    }
                    if (txnResp.Responses[1].ResponseRange.Kvs.Count == 0)
                    {
                        throw new Exception("LoadUntil Error! No record for "+config.dataPrefix+":"+id);
                    }
                    var dataKV = txnResp.Responses[1].ResponseRange.Kvs[0];
                    var data = CborDecoder<T>.Decode(CBORObject.DecodeFromBytes(dataKV.Value.ToByteArray()));
                    if (data.HasValue)
                    {
                        return new EtcdSharedChainItem<T>() {
                            revision = dataKV.ModRevision
                            , id = id
                            , data = data.Value
                            , nextID = txnResp.Responses[0].ResponseRange.Kvs[0].Value.ToStringUtf8()
                        };
                    }
                    else
                    {
                        throw new Exception("LoadUntil Error! Bad record for "+config.dataPrefix+":"+id);
                    }
                }
                else
                {
                    throw new Exception("LoadUntil Error! No record for "+config.chainPrefix+":"+id+" or "+config.dataPrefix+":"+id);
                }
            }
            else
            {               
                var rangeResp = client.GetRange(config.chainPrefix+":"+id);
                if (rangeResp.Kvs.Count == 0)
                {
                    throw new Exception("LoadUntil Error! No record for "+config.chainPrefix+":"+id);
                }
                var kv = rangeResp.Kvs[0];
                var mapData = CborDecoder<(T,string)>.Decode(CBORObject.DecodeFromBytes(kv.Value.ToByteArray()));
                if (mapData.HasValue)
                {
                    return new EtcdSharedChainItem<T>() {
                        revision = kv.ModRevision
                        , id = id
                        , data = mapData.Value.Item1
                        , nextID = mapData.Value.Item2
                    };
                }
                else
                {
                    throw new Exception("LoadUntil Error! Bad record for "+config.chainPrefix+":"+id);
                }
            }
        }
        public Option<EtcdSharedChainItem<T>> FetchNext(EtcdSharedChainItem<T> current)
        {
            if (config.duplicateFromRedis)
            {
                if (!current.nextID.Equals(""))
                {
                    var key = config.chainPrefix+":"+current.nextID;
                    var r = redisClient.StringGet(key);
                    if (!r.IsNull)
                    {
                        var nextData = CborDecoder<(long,T,string)>.Decode(CBORObject.DecodeFromBytes(r));
                        if (nextData.HasValue)
                        {
                            return new EtcdSharedChainItem<T>() {
                                revision = nextData.Value.Item1
                                , id = current.nextID
                                , data = nextData.Value.Item2
                                , nextID = nextData.Value.Item3
                            };
                        }
                    }
                }
                else
                {
                    var key = config.chainPrefix+":"+current.id;
                    var r = redisClient.StringGet(key);
                    if (!r.IsNull)
                    {
                        var data = CborDecoder<(long,T,string)>.Decode(CBORObject.DecodeFromBytes(r));
                        if (data.HasValue)
                        {
                            if (!data.Value.Item3.Equals(""))
                            {
                                key = config.chainPrefix+":"+data.Value.Item3;
                                r = redisClient.StringGet(key);
                                if (!r.IsNull)
                                {
                                    var nextData = CborDecoder<(long,T,string)>.Decode(CBORObject.DecodeFromBytes(r));
                                    if (nextData.HasValue)
                                    {
                                        return new EtcdSharedChainItem<T>() {
                                            revision = nextData.Value.Item1
                                            , id = data.Value.Item3
                                            , data = nextData.Value.Item2
                                            , nextID = nextData.Value.Item3
                                        };
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (config.saveDataOnSeparateStorage)
            {
                var nextID = current.nextID;
                if (nextID.Equals(""))
                {
                    var rangeResp = client.GetRange(config.chainPrefix+":"+current.id);
                    if (rangeResp.Kvs.Count == 0)
                    {
                        throw new Exception("FetchNext Error! No record for "+config.chainPrefix+":"+current.id);
                    }
                    nextID = rangeResp.Kvs[0].Value.ToStringUtf8();
                }
                if (!nextID.Equals(""))
                {
                    var txn = new Etcdserverpb.TxnRequest();
                    var action = new Etcdserverpb.RequestOp();
                    var get = new Etcdserverpb.RangeRequest();
                    get.Key = ByteString.CopyFromUtf8(config.chainPrefix+":"+nextID);
                    action.RequestRange = get;
                    txn.Success.Add(action);
                    action = new Etcdserverpb.RequestOp();
                    get = new Etcdserverpb.RangeRequest();
                    get.Key = ByteString.CopyFromUtf8(config.dataPrefix+":"+nextID);
                    action.RequestRange = get;
                    txn.Success.Add(action);

                    var txnResp = client.Transaction(txn);
                    if (txnResp.Succeeded)
                    {
                        if (txnResp.Responses.Count < 2)
                        {
                            throw new Exception("FetchNext Error! No record for "+config.chainPrefix+":"+nextID+" or "+config.dataPrefix+":"+nextID);
                        }
                        if (txnResp.Responses[0].ResponseRange.Kvs.Count == 0)
                        {
                            throw new Exception("FetchNext Error! No record for "+config.chainPrefix+":"+nextID);
                        }
                        if (txnResp.Responses[1].ResponseRange.Kvs.Count == 0)
                        {
                            throw new Exception("FetchNext Error! No record for "+config.dataPrefix+":"+nextID);
                        }
                        var dataKV = txnResp.Responses[1].ResponseRange.Kvs[0];
                        var data = CborDecoder<T>.Decode(CBORObject.DecodeFromBytes(dataKV.Value.ToByteArray()));
                        if (data.HasValue)
                        {
                            return new EtcdSharedChainItem<T>() {
                                revision = dataKV.ModRevision
                                , id = nextID
                                , data = data.Value
                                , nextID = txnResp.Responses[0].ResponseRange.Kvs[0].Value.ToStringUtf8()
                            };
                        }
                        else
                        {
                            throw new Exception("FetchNext Error! Bad record for "+config.dataPrefix+":"+nextID);
                        }
                    }
                    else
                    {
                        throw new Exception("Fetch Error! No record for "+config.chainPrefix+":"+nextID);
                    }
                }
                else
                {
                    return Option.None;
                }
            }
            else
            {
                var nextID = current.nextID;
                if (nextID.Equals(""))
                {
                    var rangeResp = client.GetRange(config.chainPrefix+":"+current.id);
                    if (rangeResp.Kvs.Count == 0)
                    {
                        throw new Exception("FetchNext Error! No record for "+config.chainPrefix+":"+current.id);
                    }
                    var data = CborDecoder<(T,string)>.Decode(CBORObject.DecodeFromBytes(rangeResp.Kvs[0].Value.ToByteArray()));
                    if (data.HasValue)
                    {
                        nextID = data.Value.Item2;
                    }
                    else
                    {
                        throw new Exception("FetchNext Error! Bad record for "+config.chainPrefix+":"+current.id);
                    }
                }
                if (!nextID.Equals(""))
                {
                    var rangeResp = client.GetRange(config.chainPrefix+":"+nextID);
                    if (rangeResp.Kvs.Count == 0)
                    {
                        throw new Exception("FetchNext Error! No record for "+config.chainPrefix+":"+nextID);
                    }
                    var data = CborDecoder<(T,string)>.Decode(CBORObject.DecodeFromBytes(rangeResp.Kvs[0].Value.ToByteArray()));
                    if (data.HasValue)
                    {
                        return new EtcdSharedChainItem<T>() {
                            revision = rangeResp.Kvs[0].ModRevision
                            , id = nextID
                            , data = data.Value.Item1
                            , nextID = data.Value.Item2
                        };
                    }
                    else
                    {
                        throw new Exception("FetchNext Error! Bad record for "+config.chainPrefix+":"+nextID);
                    }
                }
                else
                {
                    return Option.None;
                }
            }
        }
        public bool IDIsAlreadyOnChain(string id)
        {
            var txn = new Etcdserverpb.TxnRequest();
            var cmp = new Etcdserverpb.Compare();
            cmp.Result = Etcdserverpb.Compare.Types.CompareResult.Greater;
            cmp.Target = Etcdserverpb.Compare.Types.CompareTarget.Version;
            cmp.Key = ByteString.CopyFromUtf8(config.chainPrefix+":"+id);
            cmp.Version = 0;
            txn.Compare.Add(cmp);
            var txnResp = client.Transaction(txn);
            return txnResp.Succeeded;
        }
        public bool AppendAfter(EtcdSharedChainItem<T> current, EtcdSharedChainItem<T> toBeWritten)
        {
            if (!current.nextID.Equals(""))
            {
                return false;
            }
            if (!toBeWritten.nextID.Equals(""))
            {
                throw new Exception("Cannot append a new item whose nextID is already non-empty");
            }
            var currentChainKey = config.chainPrefix+":"+current.id;
            if (config.saveDataOnSeparateStorage)
            {
                var newDataKey = config.dataPrefix+":"+toBeWritten.id;
                var newChainKey = config.chainPrefix+":"+toBeWritten.id;
                var txn = new Etcdserverpb.TxnRequest();
                var cmp = new Etcdserverpb.Compare();
                cmp.Result = Etcdserverpb.Compare.Types.CompareResult.Equal;
                cmp.Target = Etcdserverpb.Compare.Types.CompareTarget.Value;
                cmp.Key = ByteString.CopyFromUtf8(currentChainKey);
                cmp.Value = ByteString.CopyFromUtf8("");
                txn.Compare.Add(cmp);
                cmp = new Etcdserverpb.Compare();
                cmp.Result = Etcdserverpb.Compare.Types.CompareResult.Equal;
                cmp.Target = Etcdserverpb.Compare.Types.CompareTarget.Version;
                cmp.Key = ByteString.CopyFromUtf8(newDataKey);
                cmp.Version = 0;
                txn.Compare.Add(cmp);
                cmp = new Etcdserverpb.Compare();
                cmp.Result = Etcdserverpb.Compare.Types.CompareResult.Equal;
                cmp.Target = Etcdserverpb.Compare.Types.CompareTarget.Version;
                cmp.Key = ByteString.CopyFromUtf8(newChainKey);
                cmp.Version = 0;
                txn.Compare.Add(cmp);
                var action = new Etcdserverpb.RequestOp();
                var put = new Etcdserverpb.PutRequest();
                put.Key = ByteString.CopyFromUtf8(currentChainKey);
                put.Value = ByteString.CopyFromUtf8(toBeWritten.id);
                action.RequestPut = put;
                txn.Success.Add(action);
                action = new Etcdserverpb.RequestOp();
                put = new Etcdserverpb.PutRequest();
                put.Key = ByteString.CopyFromUtf8(newDataKey);
                put.Value = ByteString.CopyFrom(CborEncoder<T>.Encode(toBeWritten.data).EncodeToBytes());
                action.RequestPut = put;
                txn.Success.Add(action);
                action = new Etcdserverpb.RequestOp();
                put = new Etcdserverpb.PutRequest();
                put.Key = ByteString.CopyFromUtf8(newChainKey);
                put.Value = ByteString.CopyFromUtf8("");
                action.RequestPut = put;
                txn.Success.Add(action);

                var txnResp = client.Transaction(txn);
                if (txnResp.Succeeded)
                {
                    if (config.automaticallyDuplicateToRedis)
                    {
                        DuplicateToRedis(redisClient, txnResp.Responses[2].ResponsePut.Header.Revision, current.id, current.data, toBeWritten.id, config.redisTTLSeconds);
                    }
                }
                return txnResp.Succeeded;
            }
            else
            {
                var newChainKey = config.chainPrefix+":"+toBeWritten.id;
                var txn = new Etcdserverpb.TxnRequest();
                var cmp = new Etcdserverpb.Compare();
                cmp.Result = Etcdserverpb.Compare.Types.CompareResult.Equal;
                cmp.Target = Etcdserverpb.Compare.Types.CompareTarget.Mod;
                cmp.Key = ByteString.CopyFromUtf8(currentChainKey);
                cmp.ModRevision = current.revision;
                txn.Compare.Add(cmp);
                cmp = new Etcdserverpb.Compare();
                cmp.Result = Etcdserverpb.Compare.Types.CompareResult.Equal;
                cmp.Target = Etcdserverpb.Compare.Types.CompareTarget.Version;
                cmp.Key = ByteString.CopyFromUtf8(newChainKey);
                cmp.Version = 0;
                txn.Compare.Add(cmp);
                var action = new Etcdserverpb.RequestOp();
                var put = new Etcdserverpb.PutRequest();
                put.Key = ByteString.CopyFromUtf8(currentChainKey);
                put.Value = ByteString.CopyFrom(CborEncoder<(T,string)>.Encode((current.data, toBeWritten.id)).EncodeToBytes());
                action.RequestPut = put;
                txn.Success.Add(action);
                action = new Etcdserverpb.RequestOp();
                put = new Etcdserverpb.PutRequest();
                put.Key = ByteString.CopyFromUtf8(newChainKey);
                put.Value = ByteString.CopyFrom(CborEncoder<(T,string)>.Encode((toBeWritten.data, "")).EncodeToBytes());
                action.RequestPut = put;
                txn.Success.Add(action);

                var txnResp = client.Transaction(txn);
                if (txnResp.Succeeded)
                {
                    if (config.automaticallyDuplicateToRedis)
                    {
                        DuplicateToRedis(redisClient, txnResp.Responses[1].ResponsePut.Header.Revision, current.id, current.data, toBeWritten.id, config.redisTTLSeconds);
                    }
                }
                return txnResp.Succeeded;
            }
        }
        public void DuplicateToRedis(IDatabase db, long rev, string id, T data, string nextID, ushort ttlSeconds)
        {
            var redisData = CborEncoder<(long,T,string)>.Encode((rev,data,nextID)).EncodeToBytes();
            var currentChainKey = config.chainPrefix+":"+id;
            if (ttlSeconds > 0)
            {
                db.StringSet(currentChainKey, redisData, expiry: TimeSpan.FromSeconds(ttlSeconds));
            }
            else
            {
                db.StringSet(currentChainKey, redisData);
            }
        }
        public void SaveExtraData<ExtraData>(string key, ExtraData data)
        {
            var put = new Etcdserverpb.PutRequest();
            put.Key = ByteString.CopyFromUtf8(config.extraDataPrefix+":"+key);
            put.Value = ByteString.CopyFrom(CborEncoder<ExtraData>.Encode(data).EncodeToBytes());
            client.Put(put);
        }
        public Option<ExtraData> LoadExtraData<ExtraData>(string key)
        {
            var queryRes = client.Get(config.extraDataPrefix+":"+key);
            if (queryRes.Kvs.Count == 0)
            {
                return Option.None;
            }
            return CborDecoder<ExtraData>.Decode(CBORObject.DecodeFromBytes(queryRes.Kvs[0].Value.ToByteArray()));
        }
    }
}