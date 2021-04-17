import * as cbor from 'cbor'

import {Etcd3, IOptions as IEtcd3Options} from 'etcd3'
//NOTE: according to https://www.gitmemory.com/SunshowerC, etcd3 1.0.1 is not compatible with 
//cockatiel 1.1.0, and cockatiel@1.0.0 must be installed instead, otherwise tsc will reject
//the importing of etcd3
import * as asyncRedis from 'async-redis'
import * as dateFormat from 'dateformat'
import {TMTransportUtils} from './TMTransport'

export interface EtcdSharedChainConfiguration {
    etcd3Options : IEtcd3Options
    , headKey : string
    , saveDataOnSeparateStorage : boolean
    , chainPrefix : string
    , dataPrefix : string
    , extraDataPrefix : string
    , redisServerAddr : string
    , duplicateFromRedis : boolean
    , redisTTLSeconds : number
    , automaticallyDuplicateToRedis : boolean
}

interface EtcdSharedChainItem {
    revision : bigint
    , id : string
    , data : any
    , nextID : string
}

export class EtcdSharedChain {
    config : EtcdSharedChainConfiguration;
    client : Etcd3;
    redisClient : any;
    current : EtcdSharedChainItem;

    static defaultEtcdSharedChainConfiguration(commonPrefix = "", useDate = true) : EtcdSharedChainConfiguration {
        var today = dateFormat(new Date(), "yyyy_mm_dd");
        return {
            etcd3Options : {hosts: '127.0.0.1:2379'}
            , headKey : ""
            , saveDataOnSeparateStorage : true
            , chainPrefix : commonPrefix+"_"+(useDate?today:"")+"_chain"
            , dataPrefix : commonPrefix+"_"+(useDate?today:"")+"_data"
            , extraDataPrefix : commonPrefix+"_"+(useDate?today:"")+"_extra_data"
            , redisServerAddr : "127.0.0.1:6379"
            , duplicateFromRedis : false
            , redisTTLSeconds : 0
            , automaticallyDuplicateToRedis : false
        };
    }

    constructor(config : EtcdSharedChainConfiguration) {
        this.config = config;
        this.client = new Etcd3(config.etcd3Options);
        if (config.duplicateFromRedis || config.automaticallyDuplicateToRedis) {
            this.redisClient = asyncRedis.createClient(`redis://${this.config.redisServerAddr}`, {'return_buffers': true});
        }
        this.current = null;
    }

    async start(defaultData : any) {
        if (this.config.duplicateFromRedis) {
            try {
                let redisReply = await this.redisClient.get(this.config.chainPrefix+":"+this.config.headKey);
                if (redisReply !== null) {
                    let parsed = cbor.decode(redisReply);
                    if (parsed.length == 3) {
                        this.current = {
                            revision : BigInt(parsed[0])
                            , id : this.config.headKey
                            , data : parsed[1]
                            , nextID : parsed[2]
                        };
                        return;
                    }
                }
            } catch (err) {
            }
        }
        if (this.config.saveDataOnSeparateStorage) {
            let etcdReply = await this.client
                .if(this.config.chainPrefix+":"+this.config.headKey, "Version", ">", 0)
                .then(this.client.get(this.config.chainPrefix+":"+this.config.headKey))
                .else(
                    this.client.put(this.config.chainPrefix+":"+this.config.headKey).value("")
                    , this.client.get(this.config.chainPrefix+":"+this.config.headKey)
                )
                .commit();
            if (etcdReply.succeeded) {
                this.current = {
                    revision : BigInt(etcdReply.responses[0].response_range.kvs[0].mod_revision)
                    , id : this.config.headKey
                    , data : defaultData
                    , nextID : etcdReply.responses[0].response_range.kvs[0].value.toString()
                }
            } else {
                this.current = {
                    revision : BigInt(etcdReply.responses[1].response_range.kvs[0].mod_revision)
                    , id : this.config.headKey
                    , data : defaultData
                    , nextID : ""
                }
            }
        } else {
            let etcdReply = await this.client
                .if(this.config.chainPrefix+":"+this.config.headKey, "Version", ">", 0)
                .then(this.client.get(this.config.chainPrefix+":"+this.config.headKey))
                .else(
                    this.client.put(this.config.chainPrefix+":"+this.config.headKey).value("")
                    , this.client.get(this.config.chainPrefix+":"+this.config.headKey)
                )
                .commit();
            if (etcdReply.succeeded) {
                let x = etcdReply.responses[0].response_range.kvs[0].value;
                if (x.byteLength > 0) {
                    let v = cbor.decode(x);
                    //please notice that defaultData is always returned as the value of the
                    //head, and we don't care what decoded v[0] is
                    this.current = {
                        revision : BigInt(etcdReply.responses[0].response_range.kvs[0].mod_revision)
                        , id : this.config.headKey
                        , data : defaultData
                        , nextID : v[1]
                    }
                } else {
                    this.current = {
                        revision : BigInt(etcdReply.responses[0].response_range.kvs[0].mod_revision)
                        , id : this.config.headKey
                        , data : defaultData
                        , nextID : ""
                    }
                }
            } else {
                this.current = {
                    revision : BigInt(etcdReply.responses[1].response_range.kvs[0].mod_revision)
                    , id : this.config.headKey
                    , data : defaultData
                    , nextID : ""
                }
            }
        }
    }

    async tryLoadUntil(id : string) : Promise<boolean> {
        //the behavior is a little different from the C++ version
        //in that it will not throw an exception if the id is not
        //there, therefore the method name and siganture are also different
        if (this.config.duplicateFromRedis) {
            try {
                let redisReply = await this.redisClient.get(this.config.chainPrefix+":"+id);
                if (redisReply !== null) {
                    let parsed = cbor.decode(redisReply);
                    if (parsed.length == 3) {
                        this.current = {
                            revision : BigInt(parsed[0])
                            , id : id
                            , data : parsed[1]
                            , nextID : parsed[2]
                        };
                        return true;
                    }
                }
            } catch (err) {
            }
        }
        if (this.config.saveDataOnSeparateStorage) {
            let etcdReply = await this.client
                .if(this.config.chainPrefix+":"+id, "Version", ">", 0)
                .then(
                    this.client.get(this.config.chainPrefix+":"+id)
                    , this.client.get(this.config.dataPrefix+":"+id)
                )
                .commit();
            if (etcdReply.succeeded) {
                this.current = {
                    revision : BigInt(etcdReply.responses[0].response_range.kvs[0].mod_revision)
                    , id : id
                    , data : cbor.decode(etcdReply.responses[1].response_range.kvs[0].value)
                    , nextID : etcdReply.responses[0].response_range.kvs[0].value.toString()
                };
                return true;
            } else {
                return false;
            }
        } else {
            let etcdReply = await this.client
                .if(this.config.chainPrefix+":"+id, "Version", ">", 0)
                .then(
                    this.client.get(this.config.chainPrefix+":"+id)
                )
                .commit();
            if (etcdReply.succeeded) {
                let parsed = cbor.decode(etcdReply.responses[0].response_range.kvs[0].value);
                this.current = {
                    revision : BigInt(etcdReply.responses[0].response_range.kvs[0].mod_revision)
                    , id : id
                    , data : parsed[0]
                    , nextID : parsed[1]
                };
                return true;
            } else {
                return false;
            }
        }
    }

    async next() : Promise<boolean> {
        if (this.config.duplicateFromRedis) {
            if (this.current.nextID == '') {
                let thisID = this.current.id;
                try {
                    let redisReply = await this.redisClient.get(this.config.chainPrefix+":"+thisID);
                    if (redisReply !== null) {
                        let parsed = cbor.decode(redisReply);
                        if (parsed.length == 3) {
                            let nextID = parsed[2];
                            if (nextID != '') {
                                redisReply = await this.redisClient.get(this.config.chainPrefix+":"+nextID);
                                if (redisReply !== null) {
                                    parsed = cbor.decode(redisReply);
                                    if (parsed.length == 3) {
                                        this.current = {
                                            revision : BigInt(parsed[0])
                                            , id : nextID
                                            , data : parsed[1]
                                            , nextID : parsed[2]
                                        };
                                        return true;
                                    }  
                                }
                            }
                        }
                    }
                } catch (err) {
                }
            } else {
                let nextID = this.current.nextID;
                try {
                    let redisReply = await this.redisClient.get(this.config.chainPrefix+":"+nextID);
                    if (redisReply !== null) {
                        let parsed = cbor.decode(redisReply);
                        if (parsed.length == 3) {
                            this.current = {
                                revision : BigInt(parsed[0])
                                , id : nextID
                                , data : parsed[1]
                                , nextID : parsed[2]
                            };
                            return true;
                        }
                    }
                } catch (err) {
                }
            }
        }
        if (this.config.saveDataOnSeparateStorage) {
            let nextID = this.current.nextID;
            if (nextID == '') {
                let etcdReply = await this.client
                    .get(this.config.chainPrefix+":"+this.current.id)
                    .exec();
                nextID = etcdReply.kvs[0].value.toString();
                if (nextID == '') {
                    return false;
                }
            }
            let nextEtcdReply = await this.client
                .if(this.config.chainPrefix+":"+nextID, "Version", ">", 0)
                .then(
                    this.client.get(this.config.chainPrefix+":"+nextID)
                    , this.client.get(this.config.dataPrefix+":"+nextID)
                )
                .commit();
            if (nextEtcdReply.succeeded) {
                this.current = {
                    revision : BigInt(nextEtcdReply.responses[0].response_range.kvs[0].mod_revision)
                    , id : nextID
                    , data : cbor.decode(nextEtcdReply.responses[1].response_range.kvs[0].value)
                    , nextID : nextEtcdReply.responses[0].response_range.kvs[0].value.toString()
                };
                return true;
            } else {
                return false;
            }
        } else {
            let nextID = this.current.nextID;
            if (nextID == '') {
                let etcdReply = await this.client
                    .get(this.config.chainPrefix+":"+this.current.id)
                    .exec();
                let x = etcdReply.kvs[0].value;
                if (x.byteLength == 0) {
                    return false;
                }
                let parsed = cbor.decode(x);
                nextID = parsed[1];
                if (nextID == '') {
                    return false;
                }
            }
            let nextEtcdReply = await this.client
                .get(this.config.chainPrefix+":"+nextID)
                .exec();
            let parsed = cbor.decode(nextEtcdReply.kvs[0].value);
            this.current = {
                revision : BigInt(nextEtcdReply.kvs[0].mod_revision)
                , id : nextID
                , data : parsed[0]
                , nextID : parsed[1]
            };
            return true;
        }
    }

    async idIsAlreadyOnChain(id : string) : Promise<boolean> {
        let etcdReply = await this.client
            .if(this.config.chainPrefix+":"+id, "Version", ">", 0)
            .commit();
        return etcdReply.succeeded;
    }

    async tryAppend(newID : string, newData : any) : Promise<boolean> {
        while (true) {
            let x = await this.next();
            if (!x) {
                break;
            }
        }
        let thisID = this.current.id;
        let succeeded = false;
        let revision = BigInt(0);
        if (this.config.saveDataOnSeparateStorage) {
            let etcdReply = await this.client
                .if(this.config.chainPrefix+":"+thisID, "Mod", "==", Number(this.current.revision))
                .and(this.config.dataPrefix+":"+newID, "Version", "==", 0)
                .and(this.config.chainPrefix+":"+newID, "Version", "==", 0)
                .then(
                    this.client.put(this.config.chainPrefix+":"+thisID).value(newID)
                    , this.client.put(this.config.dataPrefix+":"+newID).value(cbor.encode(newData))
                    , this.client.put(this.config.chainPrefix+":"+newID).value("")
                )
                .commit();
            succeeded = etcdReply.succeeded;
            if (succeeded) {
                revision = BigInt(etcdReply.responses[2].response_put.header.revision);
            }
        } else {
            let etcdReply = await this.client
                .if(this.config.chainPrefix+":"+thisID, "Mod", "==", Number(this.current.revision))
                .and(this.config.chainPrefix+":"+newID, "Version", "==", 0)
                .then(
                    this.client.put(this.config.chainPrefix+":"+thisID).value(
                        cbor.encode([this.current.data, newID])
                    )
                    , this.client.put(this.config.chainPrefix+":"+newID).value(
                        cbor.encode([newData, ""])
                    )
                )
                .commit();
            succeeded = etcdReply.succeeded;
            if (succeeded) {
                revision = BigInt(etcdReply.responses[1].response_put.header.revision);
            }
        }
        if (succeeded) {
            if (this.config.automaticallyDuplicateToRedis) {
                if (this.config.redisTTLSeconds > 0) {
                    await this.redisClient.set(
                        this.config.chainPrefix+":"+thisID
                        , cbor.encode([this.current.revision, this.current.data, newID])
                        , 'EX'
                        , this.config.redisTTLSeconds
                    );
                } else {
                    await this.redisClient.set(
                        this.config.chainPrefix+":"+thisID
                        , cbor.encode([this.current.revision, this.current.data, newID])
                    );
                }
            }
            this.current = {
                revision : revision
                , id : newID
                , data : newData
                , nextID : ""
            };
        }
        return succeeded;
    }

    async append(newID : string, newData : any) {
        while (true) {
            let res = await this.tryAppend(newID, newData);
            if (res) {
                break;
            }
        }
    }

    async saveExtraData(key : string, data : any) {
        await this.client
            .put(this.config.extraDataPrefix+":"+key)
            .value(cbor.encode(data))
            .exec();
    }

    async loadExtraData(key : string) : Promise<any> {
        let etcdReply = await this.client
            .get(this.config.extraDataPrefix+":"+key)
            .exec();
        if (etcdReply.kvs.length == 0) {
            return null;
        }
        return cbor.decode(etcdReply.kvs[0].value);
    }

    async close() {
        if (this.config.duplicateFromRedis || this.config.automaticallyDuplicateToRedis) {
            await this.redisClient.quit();
        }
    }

    currentValue() {
        return this.current;
    }
}

export interface RedisSharedChainConfiguration {
    redisServerAddr : string
    , headKey : string
    , chainPrefix : string
    , dataPrefix : string
    , extraDataPrefix : string
}

interface RedisSharedChainItem {
    id : string
    , data : any
    , nextID : string
}

export class RedisSharedChain {
    config : RedisSharedChainConfiguration;
    client : any;
    current : RedisSharedChainItem;

    static defaultRedisSharedChainConfiguration(commonPrefix = "", useDate = true) : RedisSharedChainConfiguration {
        var today = dateFormat(new Date(), "yyyy_mm_dd");
        return {
            redisServerAddr : "127.0.0.1:6379"
            , headKey : ""
            , chainPrefix : commonPrefix+"_"+(useDate?today:"")+"_chain"
            , dataPrefix : commonPrefix+"_"+(useDate?today:"")+"_data"
            , extraDataPrefix : commonPrefix+"_"+(useDate?today:"")+"_extra_data"
        };
    }

    constructor(config : RedisSharedChainConfiguration) {
        this.config = config;
        this.client = asyncRedis.createClient(`redis://${this.config.redisServerAddr}`, {'return_buffers': true});
        this.current = null;
    }

    async start(defaultData : any) {
        const headKeyStr = this.config.chainPrefix+":"+this.config.headKey;
        const luaStr = "local x = redis.call('GET',KEYS[1]); if x then return x else redis.call('SET',KEYS[1],''); return '' end";
        let redisReply = await this.client.eval(luaStr, 1, headKeyStr);
        if (redisReply != null) {
            this.current = {
                id : this.config.headKey
                , data : defaultData
                , nextID : redisReply.toString('utf-8')
            };
        }
    }

    async tryLoadUntil(id : string) : Promise<boolean> {
        let chainKey = this.config.chainPrefix+":"+id;
        let dataKey = this.config.dataPrefix+":"+id;
        let dataReply = await this.client.get(dataKey);
        if (dataReply == null) {
            return false;
        }
        let chainReply = await this.client.get(chainKey);
        if (chainReply == null) {
            return false;
        }
        this.current = {
            id : id
            , data : cbor.decode(dataReply)
            , nextID : chainReply.toString('utf-8')
        };
        return true;
    }

    async next() : Promise<boolean> {
        let nextID = this.current.nextID;
        if (nextID == '') {
            let chainKey = this.config.chainPrefix+":"+this.current.id;
            let redisReply = await this.client.get(chainKey);
            if (redisReply != null) {
                nextID = redisReply.toString('utf-8');
            }
            if (nextID == '') {
                return false;
            }
        }
        let chainKey = this.config.chainPrefix+":"+nextID;
        let dataKey = this.config.dataPrefix+":"+nextID;
        let dataReply = await this.client.get(dataKey);
        if (dataReply == null) {
            return false;
        }        
        let chainReply = await this.client.get(chainKey);
        if (chainReply == null) {
            return false;
        }
        this.current = {
            id : nextID
            , data : cbor.decode(dataReply)
            , nextID : chainReply.toString('utf-8')
        };
        return true;
    }

    async idIsAlreadyOnChain(id : string) : Promise<boolean> {
        let chainKey = this.config.chainPrefix+":"+id;
        let chainReply = await this.client.get(chainKey);
        return (chainReply != null);
    }

    async tryAppend(newID : string, newData : any) : Promise<boolean> {
        while (true) {
            let x = await this.next();
            if (!x) {
                break;
            }
        }

        let currentChainKey = this.config.chainPrefix+":"+this.current.id;
        let newDataKey = this.config.dataPrefix+":"+newID;
        let newChainKey = this.config.chainPrefix+":"+newID;
        const luaStr = "local x = redis.call('GET',KEYS[1]); local y = redis.call('GET',KEYS[2]); local z = redis.call('GET',KEYS[3]); if x == '' and not y and not z then redis.call('SET',KEYS[1],ARGV[1]); redis.call('SET',KEYS[2],ARGV[2]); redis.call('SET',KEYS[3],''); return 1 else return 0 end";
        let redisReply = await this.client.eval(luaStr, 3, currentChainKey, newDataKey, newChainKey, newID, cbor.encode(newData));
        let succeeded = (redisReply != null && redisReply != 0);
        if (succeeded) {
            this.current = {
                id : newID
                , data : newData
                , nextID : ""
            };
        }
        return succeeded;
    }

    async append(newID : string, newData : any) {
        while (true) {
            let res = await this.tryAppend(newID, newData);
            if (res) {
                break;
            }
        }
    }

    async saveExtraData(key : string, data : any) {
        let dataKey = this.config.extraDataPrefix+":"+key;
        await this.client.set(dataKey, cbor.encode(data));
    }

    async loadExtraData(key : string) : Promise<any> {
        let dataKey = this.config.extraDataPrefix+":"+key;
        let dataReply = await this.client.get(dataKey);
        if (dataReply == null) {
            return null;
        }
        return cbor.decode(dataReply);
    }

    async close() {
        await this.client.quit();
    }

    currentValue() {
        return this.current;
    }
}

export type SharedChainSpec = ["etcd", EtcdSharedChainConfiguration] | ["redis", RedisSharedChainConfiguration];

export class SharedChainUtils {
    static parseChainLocator(locatorStr : string) : SharedChainSpec {
        if (locatorStr.startsWith('etcd://')) {
            let parsedParts = TMTransportUtils.parseConnectionLocator(locatorStr.substr('etcd://'.length));
            let conf : EtcdSharedChainConfiguration = {
                etcd3Options : {hosts: `${parsedParts.host}:${parsedParts.port}`}
                , headKey : TMTransportUtils.getConnectionLocatorProperty(parsedParts, "headKey", "head")
                , saveDataOnSeparateStorage : TMTransportUtils.getConnectionLocatorProperty(parsedParts, "saveDataOnSeparateStorage","true") == "true"
                , chainPrefix : parsedParts.identifier
                , dataPrefix : TMTransportUtils.getConnectionLocatorProperty(parsedParts, "dataPrefix", parsedParts.identifier+"_data")
                , extraDataPrefix : TMTransportUtils.getConnectionLocatorProperty(parsedParts, "extraDataPrefix", parsedParts.identifier+"_extra_data")
                , redisServerAddr : TMTransportUtils.getConnectionLocatorProperty(parsedParts, "redisServerAddr", "")
                , duplicateFromRedis : TMTransportUtils.getConnectionLocatorProperty(parsedParts, "redisServerAddr", "") != ""
                , redisTTLSeconds : parseInt(TMTransportUtils.getConnectionLocatorProperty(parsedParts, "redisTTLSeconds", "0"))
                , automaticallyDuplicateToRedis : TMTransportUtils.getConnectionLocatorProperty(parsedParts, "automaticallyDuplicateToRedis", "false") == "true"
            };
            return ["etcd", conf];
        } else if (locatorStr.startsWith('redis://')) {
            let parsedParts = TMTransportUtils.parseConnectionLocator(locatorStr.substr('redis://'.length));
            let conf : RedisSharedChainConfiguration = {
                redisServerAddr : `${parsedParts.host}:${parsedParts.port}`
                , headKey : TMTransportUtils.getConnectionLocatorProperty(parsedParts, "headKey", "head")
                , chainPrefix : parsedParts.identifier
                , dataPrefix : TMTransportUtils.getConnectionLocatorProperty(parsedParts, "dataPrefix", parsedParts.identifier+"_data")
                , extraDataPrefix : TMTransportUtils.getConnectionLocatorProperty(parsedParts, "extraDataPrefix", parsedParts.identifier+"_extra_data")
            };
            return ["redis", conf];
        } else {
            return null;
        }
    }
}