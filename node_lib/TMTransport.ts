import * as dgram from 'dgram'
import * as amqp from 'amqplib'
import * as redis from 'redis'
import * as zmq from 'zeromq'
import * as cbor from 'cbor'
import * as Stream from 'stream'
import * as fs from 'fs'
import {v4 as uuidv4} from "uuid"

import {Etcd3, IOptions as IEtcd3Options, STMConflictError} from 'etcd3'
//NOTE: according to https://www.gitmemory.com/SunshowerC, etcd3 1.0.1 is not compatible with 
//cockatiel 1.1.0, and cockatiel@1.0.0 must be installed instead, otherwise tsc will reject
//the importing of etcd3
import * as asyncRedis from 'async-redis'
import * as dateFormat from 'dateformat'

export enum Transport {
    Multicast
    , RabbitMQ
    , Redis
    , ZeroMQ
}

export enum TopicMatchType {
    MatchAll
    , MatchExact
    , MatchRE
}

export interface TopicSpec {
    matchType : TopicMatchType
    , exactString : string
    , regex : RegExp
}

export interface ConnectionLocator {
    host : string
    , port : number
    , username : string
    , password : string
    , identifier : string
    , properties : Record<string, string>
}

class TMTransportUtils {
    static parseConnectionLocator(locatorStr : string) : ConnectionLocator {
        let idx = locatorStr.indexOf('[');
        let mainPortion : string;
        let propertyPortion : string;
        if (idx < 0) {
            mainPortion = locatorStr;
            propertyPortion = "";
        } else {
            mainPortion = locatorStr.substr(0, idx);
            propertyPortion = locatorStr.substr(idx);
        }
        let mainParts = mainPortion.split(':');
        let properties : Record<string, string> = {};
        if (propertyPortion.length > 2 && propertyPortion[propertyPortion.length-1] == ']') {
            let realPropertyPortion = propertyPortion.substr(1, propertyPortion.length-2);
            let propertyParts = realPropertyPortion.split(',');
            for (let p of propertyParts) {
                let nameAndValue = p.split('=');
                if (nameAndValue.length == 2) {
                    let name = nameAndValue[0];
                    let value = nameAndValue[1];
                    properties[name] = value;
                }
            }
        }
        let ret : ConnectionLocator = {
            host : ''
            , port : 0
            , username : ''
            , password : ''
            , identifier : ''
            , properties : properties
        };
        if (mainParts.length >= 1) {
            ret.host = mainParts[0];
        } else {
            ret.host = "";
        }
        if (mainParts.length >= 2 && mainParts[1] != "") {
            ret.port = parseInt(mainParts[1]);
        } else {
            ret.port = 0;
        }
        if (mainParts.length >= 3) {
            ret.username = mainParts[2];
        } else {
            ret.username = "";
        }
        if (mainParts.length >= 4) {
            ret.password = mainParts[3];
        } else {
            ret.password = "";
        }
        if (mainParts.length >= 5) {
            ret.identifier = mainParts[4];
        } else {
            ret.identifier = "";
        }
        return ret;
    }
    static parseAddress(address : string) : [Transport, ConnectionLocator] {
        if (address.startsWith("multicast://")) {
            return [Transport.Multicast, this.parseConnectionLocator(address.substr("multicast://".length))];
        } else if (address.startsWith("rabbitmq://")) {
            return [Transport.RabbitMQ, this.parseConnectionLocator(address.substr("rabbitmq://".length))];
        } else if (address.startsWith("redis://")) {
            return [Transport.Redis, this.parseConnectionLocator(address.substr("redis://".length))];
        } else if (address.startsWith("zeromq://")) {
            return [Transport.ZeroMQ, this.parseConnectionLocator(address.substr("zeromq://".length))];
        } else {
            return null;
        }
    }
    static parseComplexTopic(topic : string) : TopicSpec {
        if (topic === "") {
            return {matchType : TopicMatchType.MatchAll, exactString : "", regex : null};
        } else if (topic.length > 3 && topic.startsWith("r/") && topic.endsWith("/")) {
            return {matchType : TopicMatchType.MatchRE, exactString : "", regex : new RegExp(topic.substr(2, topic.length-3))};
        } else {
            return {matchType : TopicMatchType.MatchExact, exactString : topic, regex : null};
        }
    }
    static parseTopic(transport : Transport, topic : string) : TopicSpec {
        switch (transport) {
            case Transport.Multicast:
                return this.parseComplexTopic(topic);
            case Transport.RabbitMQ:
                return {matchType : TopicMatchType.MatchExact, exactString : topic, regex : null};
            case Transport.Redis:
                return {matchType : TopicMatchType.MatchExact, exactString : topic, regex : null};
            case Transport.ZeroMQ:
                return this.parseComplexTopic(topic);
            default:
                return null;
        }
    }
    static async createAMQPConnection(locator : ConnectionLocator) : Promise<amqp.Connection> {
        if ("ssl" in locator.properties && locator.properties.ssl === "true") {
            if (!("ca_cert" in locator.properties)) {
                return null;
            }
            
            let url = `amqps://${locator.username}:${locator.password}@${locator.host}${locator.port>0?`:${locator.port}`:''}`;
            if ("vhost" in locator.properties) {
                url += `/${locator.properties.vhost}`;
            }
            
            let opt : Record<string, any> = {
                ca : [fs.readFileSync(locator.properties.ca_cert)]
            };

            if ("client_key" in locator.properties && "client_cert" in locator.properties) {
                opt["cert"] = fs.readFileSync(locator.properties.client_cert);
                opt["key"] = fs.readFileSync(locator.properties.client_key);
            }
            return amqp.connect(url, opt);
        } else {
            let url = `amqp://${locator.username}:${locator.password}@${locator.host}${locator.port>0?`:${locator.port}`:''}`;
            if ("vhost" in locator.properties) {
                url += `/${locator.properties.vhost}`;
            }
            return amqp.connect(url);
        }
    }
    static bufferTransformer(f : (data : Buffer) => Buffer) : Stream.Transform {
        let t = new Stream.Transform({
            transform : function(chunk : [string, Buffer], encoding : BufferEncoding, callback) {
                let processed = f(chunk[1]);
                if (processed) {
                    this.push([chunk[0], processed], encoding);
                }
                callback();
            }
            , objectMode : true
        });
        return t;
    }
}

export class MultiTransportListener {
    private static multicastInputToStream(locator : ConnectionLocator, topic : TopicSpec, stream : Stream.Readable) {
        let filter = function(s : string) {return true;}    
        switch (topic.matchType) {
            case TopicMatchType.MatchAll:
                break;
            case TopicMatchType.MatchExact:
                let matchS = topic.exactString;
                filter = function(s : string) {
                    return s === matchS;
                }
                break;
            case TopicMatchType.MatchRE:
                let matchRE = topic.regex;
                filter = function(s : string) {
                    return matchRE.test(s);
                }
                break;
        } 
        
        let sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
        sock.on('message', function(msg : Buffer, _rinfo) {
            try {
                let decoded = cbor.decode(msg);
                let t = decoded[0] as string;
                if (filter(t)) {
                    stream.push([t, decoded[1] as Buffer]);
                }
            } catch (e) {
            }
        });
        sock.bind(locator.port, function() {
            sock.setBroadcast(true);
            if ("ttl" in locator.properties) {
                sock.setMulticastTTL(parseInt(locator.properties.ttl));
            }
            sock.addMembership(locator.host);
        });
    }

    private static rabbitmqInputToStream(locator : ConnectionLocator, topic : TopicSpec, stream : Stream.Readable) {
        (async () => {
            let connection = await TMTransportUtils.createAMQPConnection(locator);
            let channel = await connection.createChannel();
            channel.assertExchange(
                locator.identifier
                , "topic"
                , {
                    "durable" :  ("durable" in locator.properties && locator.properties.durable === "true")
                    , "autoDelete" : ("auto_delete" in locator.properties && locator.properties.auto_delete === "true")
                }
            );
            let queue = await channel.assertQueue("");
            channel.bindQueue(
                queue.queue
                , locator.identifier
                , topic.exactString
            );
            channel.consume(queue.queue, function(msg) {
                if (msg != null) {
                    stream.push(
                        [msg.fields.routingKey, msg.content]
                    );
                } else {
                    stream.push(null);
                }
            });
        })();
    }

    private static redisInputToStream(locator : ConnectionLocator, topic : TopicSpec, stream : Stream.Readable) {
        (async () => {
            let subscriber = redis.createClient({
                host : locator.host
                , port : ((locator.port>0)?locator.port:6379)
                , return_buffers : true
            });
            subscriber.on('pmessage_buffer', function(_pattern : string, channel : string, message : Buffer) {
                stream.push([channel, message]);
            });
            subscriber.on('pmessage', function(_pattern : string, channel : string, message : Buffer) {
                stream.push([channel, message]);
            });
            await new Promise<string>((resolve, reject) => {
                subscriber.psubscribe(topic.exactString, function(err, reply) {
                    if (err) {
                        reject(err);
                    }
                    resolve(reply);
                });
            });
        })();
    }

    private static zeromqInputToStream(locator : ConnectionLocator, topic : TopicSpec, stream : Stream.Readable) {
        let filter = function(s : string) {return true;}    
        switch (topic.matchType) {
            case TopicMatchType.MatchAll:
                break;
            case TopicMatchType.MatchExact:
                let matchS = topic.exactString;
                filter = function(s : string) {
                    return s === matchS;
                }
                break;
            case TopicMatchType.MatchRE:
                let matchRE = topic.regex;
                filter = function(s : string) {
                    return matchRE.test(s);
                }
                break;
        } 

        let sock = new zmq.Subscriber();
        sock.connect(`tcp://${locator.host}:${locator.port}`);
        sock.subscribe('');  
        (async () => {
            for await (const [topic, _msg] of sock) {   
                try {
                    let decoded = cbor.decode(topic);
                    let t = decoded[0] as string;
                    if (filter(t)) {
                        stream.push([t, decoded[1] as Buffer]);
                    }
                } catch (e) {
                }  
            }
        })();
    }

    private static defaultTopic(transport : Transport) : string {
        switch (transport) {
            case Transport.Multicast:
                return "";
            case Transport.RabbitMQ:
                return "#";
            case Transport.Redis:
                return "*";
            case Transport.ZeroMQ:
                return "";
            default:
                return "";
        }
    }

    static inputStream(address : string, topic? : string, wireToUserHook? : ((data: Buffer) => Buffer)) : Stream.Readable {
        let parsedAddr = TMTransportUtils.parseAddress(address);
        if (parsedAddr == null) {
            return null;
        }
        let parsedTopic : TopicSpec = null;
        if (topic) {
            parsedTopic = TMTransportUtils.parseTopic(parsedAddr[0], topic);
        } else {
            parsedTopic = TMTransportUtils.parseTopic(parsedAddr[0], this.defaultTopic(parsedAddr[0]));
        }
        if (parsedTopic == null) {
            return null;
        }
        let s = new Stream.Readable({
            read : function() {}
            , objectMode : true
        });
        switch (parsedAddr[0]) {
            case Transport.Multicast:
                this.multicastInputToStream(parsedAddr[1], parsedTopic, s);
                break;
            case Transport.RabbitMQ:
                this.rabbitmqInputToStream(parsedAddr[1], parsedTopic, s);
                break;
            case Transport.Redis:
                this.redisInputToStream(parsedAddr[1], parsedTopic, s);
                break;
            case Transport.ZeroMQ:
                this.zeromqInputToStream(parsedAddr[1], parsedTopic, s);
                break;
            default:
                return;
        }
        if (wireToUserHook) {
            let decode = TMTransportUtils.bufferTransformer(wireToUserHook);
            s.pipe(decode);
            return decode;
        } else {
            return s;
        }
    }
}

export class MultiTransportPublisher {
    private static multicastWrite(locator : ConnectionLocator) : (chunk : [string, Buffer], encoding : BufferEncoding) => void {
        let sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
        sock.bind(locator.port, function() {
            sock.setBroadcast(true);
            if ("ttl" in locator.properties) {
                sock.setMulticastTTL(parseInt(locator.properties.ttl));
            }
            sock.addMembership(locator.host);
        });
        return function(chunk : [string, Buffer], _encoding : BufferEncoding) {
            sock.send(cbor.encode(chunk), locator.port, locator.host);
        }
    }
    private static async rabbitmqWrite(locator : ConnectionLocator) : Promise<(chunk : [string, Buffer], encoding : BufferEncoding) => void> {
        let connection = await TMTransportUtils.createAMQPConnection(locator);
        let channel = await connection.createChannel();
        let exchange = locator.identifier;
        return function(chunk : [string, Buffer], encoding : BufferEncoding) {
            channel.publish(
                exchange
                , chunk[0]
                , chunk[1]
                , {
                    contentEncoding : encoding
                    , deliveryMode : 1
                    , expiration: "1000"
                }
            );
        }
    }
    private static redisWrite(locator : ConnectionLocator) : (chunk : [string, Buffer], encoding : BufferEncoding) => void {
        let publisher = redis.createClient({
            host : locator.host
            , port : ((locator.port>0)?locator.port:6379)
        });
        return function(chunk : [string, Buffer], _encoding : BufferEncoding) {
            publisher.send_command("PUBLISH", [chunk[0], chunk[1]]);
        }
    }
    private static zeromqWrite(locator : ConnectionLocator) : (chunk : [string, Buffer], encoding : BufferEncoding) => void {
        let sock = new zmq.Publisher();
        sock.bind(`tcp://${locator.host}:${locator.port}`);
        return function(chunk : [string, Buffer], _encoding : BufferEncoding) {
            sock.send(cbor.encode(chunk));
        };
    }
    static async outputStream(address : string, userToWireHook? : ((data : Buffer) => Buffer)) : Promise<Stream.Writable> {
        let parsedAddr = TMTransportUtils.parseAddress(address);
        if (parsedAddr == null) {
            return null;
        }
        let writeFunc = function(_chunk : [string, Buffer], _encoding : BufferEncoding) {
        }
        switch (parsedAddr[0]) {
            case Transport.Multicast:
                writeFunc = this.multicastWrite(parsedAddr[1]);
                break;
            case Transport.RabbitMQ:
                writeFunc = await this.rabbitmqWrite(parsedAddr[1]);
                break;
            case Transport.Redis:
                writeFunc = this.redisWrite(parsedAddr[1]);
                break;
            case Transport.ZeroMQ:
                writeFunc = this.zeromqWrite(parsedAddr[1]);
                break;
            default:
                return null;
        }
        let s = new Stream.Writable({
            write : function(chunk : [string, Buffer], encoding : BufferEncoding, callback) {
                writeFunc(chunk, encoding);
                callback();
            }
            , objectMode : true
        });
        if (userToWireHook) {
            let encode = TMTransportUtils.bufferTransformer(userToWireHook);
            encode.pipe(s);
            return encode;
        } else {
            return s;
        }
    }
}

export interface FacilityOutput {
    id : string
    , originalInput : Buffer
    , output : Buffer
    , isFinal : boolean
}

export interface ClientFacilityStreamParameters {
    address : string
    , userToWireHook? : (data : Buffer) => Buffer
    , wireToUserHook? : (data : Buffer) => Buffer
    , identityAttacher? : (data : Buffer) => Buffer
}

export interface ServerFacilityStreamParameters {
    address : string
    , userToWireHook? : (data : Buffer) => Buffer
    , wireToUserHook? : (data : Buffer) => Buffer
    , identityResolver? : (data : Buffer) => [boolean, string, Buffer]
}

export class MultiTransportFacilityClient {
    private static async rabbitmqFacilityStream(locator : ConnectionLocator) : Promise<Stream.Duplex> {
        let connection = await TMTransportUtils.createAMQPConnection(locator);
        let channel = await connection.createChannel();
        let replyQueue = await channel.assertQueue('', {exclusive: true, autoDelete: true});

        let inputMap = new Map<string, Buffer>();

        let stream = new Stream.Duplex({
            write : function(chunk : [string, Buffer], _encoding, callback) {
                inputMap.set(chunk[0], chunk[1]);
                channel.sendToQueue(
                    locator.identifier
                    , chunk[1]
                    , {
                        correlationId : chunk[0]
                        , contentEncoding : 'with_final'
                        , replyTo: replyQueue.queue
                        , deliveryMode: (("persistent" in locator.properties && locator.properties.persistent === "true")?2:1)
                        , expiration: '5000'
                    }
                );
                callback();
            }
            , read : function() {}
            , objectMode : true
        });
        channel.consume(replyQueue.queue, function(msg) : void {
            let isFinal = false;
            let res : Buffer = null;
            let id = msg.properties.correlationId;
            let input : Buffer = null;

            if (!inputMap.has(id)) {
                return;
            }
            input = inputMap.get(id);

            if (msg.properties.contentEncoding == 'with_final') {
                if (msg.content.byteLength > 0) {
                    res = msg.content.slice(0, msg.content.byteLength-1);
                    isFinal = (msg.content[msg.content.byteLength-1] != 0);
                } else {
                    res = null;
                    isFinal = false;
                }
            } else {
                res = msg.content;
                isFinal = false;
            }
            if (res != null) {
                if (isFinal) {
                    inputMap.delete(id);
                }
                let output : FacilityOutput = {
                    id: id
                    , originalInput : input
                    , output : res
                    , isFinal : isFinal
                };
                stream.push(output);
            }
        }, {noAck : true});

        return stream;
    }
    private static async redisFacilityStream(locator : ConnectionLocator) : Promise<Stream.Duplex> {
        let publisher = redis.createClient({
            host : locator.host
            , port : ((locator.port>0)?locator.port:6379)
        });
        let subscriber = redis.createClient({
            host : locator.host
            , port : ((locator.port>0)?locator.port:6379)
            , return_buffers : true
        });
        
        let streamID = uuidv4();
        let inputMap = new Map<string, Buffer>();
        let stream = new Stream.Duplex({
            write : function(chunk : [string, Buffer], _encoding, callback) {
                inputMap.set(chunk[0], chunk[1]);
                publisher.send_command("PUBLISH", [locator.identifier, cbor.encode([streamID, cbor.encode(chunk)])]);
                callback();
            }
            , read : function() {}   
            , objectMode: true
        })

        let handleMessage = function(message : Buffer) : void {
            let parsed = cbor.decode(message);
            if (parsed.length != 2) {
                return;
            }

            let isFinal = false;
            let res : Buffer = null;
            let id = parsed[0];
            let input : Buffer = null;

            if (!inputMap.has(id)) {
                return;
            }
            input = inputMap.get(id);

            if (parsed[1].byteLength > 0) {
                res = parsed[1].slice(0, parsed[1].byteLength-1);
                isFinal = ((parsed[1])[parsed[1].byteLength-1] != 0);
            } else {
                res = null;
                isFinal = false;
            }

            if (res != null) {
                if (isFinal) {
                    inputMap.delete(id);
                }
                let output : FacilityOutput = {
                    id: id
                    , originalInput : input
                    , output : res
                    , isFinal : isFinal
                };
                stream.push(output);
            }
        }

        subscriber.on('message_buffer', function(_channel : string, message : Buffer) {
            handleMessage(message);
        });
        subscriber.on('message', function(_channel : string, message : Buffer) {
            handleMessage(message);
        });
        return new Promise<Stream.Duplex>((resolve, reject) => {
            subscriber.subscribe(streamID, function(err, _reply) {
                if (err) {
                    return reject(err);
                }
                return resolve(stream);
            });
        });
    }
    
    static async facilityStream(param : ClientFacilityStreamParameters) : Promise<[Stream.Writable, Stream.Readable]> {
        let parsedAddr = TMTransportUtils.parseAddress(param.address);
        if (parsedAddr == null) {
            return null;
        }
        let s : Stream.Duplex = null;
        switch (parsedAddr[0]) {
            case Transport.RabbitMQ:
                s = await this.rabbitmqFacilityStream(parsedAddr[1]);
                break;
            case Transport.Redis:
                s = await this.redisFacilityStream(parsedAddr[1]);
                break;
            default:
                return null;
        }
        let input : Stream.Writable = s;
        let output : Stream.Readable = s;
        if (param.userToWireHook) {
            let encode = TMTransportUtils.bufferTransformer(param.userToWireHook);
            encode.pipe(input);
            input = encode;
        }
        if (param.identityAttacher) {
            let attach = TMTransportUtils.bufferTransformer(param.identityAttacher);
            attach.pipe(input);
            input = attach;
        }
        if (param.wireToUserHook) {
            let decode = new Stream.Transform({
                transform : function(chunk : FacilityOutput, encoding : BufferEncoding, callback) {
                    let newChunk = chunk;
                    let processed = param.wireToUserHook(chunk.output);
                    if (processed) {
                        newChunk.output = processed;
                        this.push(newChunk, encoding);
                    }
                    callback();
                }
                , objectMode : true
            });
            output.pipe(decode);
            output = decode;
        }
        return [input, output];
    }
    static keyify() : Stream.Transform {
        return new Stream.Transform({
            transform : function(chunk : Buffer, _encoding, callback) {
                this.push([uuidv4(), chunk]);
                callback();
            }
            , readableObjectMode : true
        })
    }
}

export class MultiTransportFacilityServer {
    private static async rabbitmqFacilityStream(locator : ConnectionLocator) : Promise<Stream.Duplex> {
        let connection = await TMTransportUtils.createAMQPConnection(locator);
        let channel = await connection.createChannel();
        let serviceQueue = await channel.assertQueue(
            locator.identifier
            , {
                durable : ("durable" in locator.properties && locator.properties.durable === "true")
                , autoDelete : false 
                , exclusive : false
            }
        );
        let replyMap = new Map<string, [string, string]>();
        let ret = new Stream.Duplex({
            write : function(chunk : [string, Buffer, boolean], _encoding, callback) {
                if (!replyMap.has(chunk[0])) {
                    callback();
                    return;
                }
                let replyInfo = replyMap.get(chunk[0]);
                let outBuffer : Buffer = chunk[1];
                if (replyInfo[1] == 'with_final') {
                    outBuffer = Buffer.concat([chunk[1], Buffer.from([chunk[2]?1:0])]);
                }
                channel.sendToQueue(
                    replyInfo[0]
                    , outBuffer
                    , {
                        correlationId : chunk[0]
                        , contentEncoding : replyInfo[1]
                        , replyTo : serviceQueue.queue
                    }
                );
                if (chunk[2]) {
                    replyMap.delete(chunk[0]);
                }
                callback();
            }
            , read : function(_size : number) {}
            , objectMode : true
        });
        channel.consume(serviceQueue.queue, function(msg) {
            if (msg != null) {
                let id = msg.properties.correlationId as string;
                if (replyMap.has(id)) {
                    return;
                }
                replyMap.set(id, [msg.properties.replyTo as string, msg.properties.contentEncoding as string]);
                ret.push(
                    [id, msg.content]
                );
            }
        });

        return ret;
    }
    private static async redisFacilityStream(locator : ConnectionLocator) : Promise<Stream.Duplex> {
        let publisher = redis.createClient({
            host : locator.host
            , port : ((locator.port>0)?locator.port:6379)
        });
        let subscriber = redis.createClient({
            host : locator.host
            , port : ((locator.port>0)?locator.port:6379)
            , return_buffers : true
        });

        let replyMap = new Map<string, string>();

        let ret = new Stream.Duplex({
            write : function(chunk : [string, Buffer, boolean], _encoding, callback) {
                let [id, data, final] = chunk;
                if (!replyMap.has(id)) {
                    callback();
                    return;
                }
                let replyInfo = replyMap.get(id);
                let outBuffer = Buffer.concat([data, Buffer.from([final?1:0])]);
                publisher.send_command("PUBLISH", [replyInfo, cbor.encode([id, outBuffer])]);
                if (final) {
                    replyMap.delete(id);
                }
                callback();
            }
            , read : function(_size : number) {}
            , objectMode : true
        });
        let handleMessage = function(message : Buffer) : void {
            let parsed = cbor.decode(message);
            if (parsed.length != 2) {
                return;
            }
            let [replyQueue, idAndData] = parsed;

            parsed = cbor.decode(idAndData);
            if (parsed.length != 2) {
                return;
            }
            let [id, data] = parsed;

            if (replyMap.has(id)) {
                return;
            }

            replyMap.set(id, replyQueue);
            ret.push([id, data]);
        }

        subscriber.on('message_buffer', function(_channel : string, message : Buffer) {
            handleMessage(message);
        });
        subscriber.on('message', function(_channel : string, message : Buffer) {
            handleMessage(message);
        });
        return new Promise<Stream.Duplex>((resolve, reject) => {
            subscriber.subscribe(locator.identifier, function(err, _reply) {
                if (err) {
                    return reject(err);
                }
                return resolve(ret);
            });
        });
    }
    
    static async facilityStream(param : ServerFacilityStreamParameters) : Promise<[Stream.Writable, Stream.Readable]> {
        let parsedAddr = TMTransportUtils.parseAddress(param.address);
        if (parsedAddr == null) {
            return null;
        }
        let s : Stream.Duplex = null;
        switch (parsedAddr[0]) {
            case Transport.RabbitMQ:
                s = await this.rabbitmqFacilityStream(parsedAddr[1]);
                break;
            case Transport.Redis:
                s = await this.redisFacilityStream(parsedAddr[1]);
                break;
            default:
                return null;
        }
        let input : Stream.Writable = s;
        let output : Stream.Readable = s;
        if (param.userToWireHook) {
            let encode = new Stream.Transform({
                transform : function(chunk : [string, Buffer, boolean], encoding : BufferEncoding, callback) {
                    let processed = param.userToWireHook(chunk[1]);
                    if (processed) {
                        this.push([chunk[0], processed, chunk[2]], encoding);
                    }
                    callback();
                }
                , objectMode : true
            });
            encode.pipe(input);
            input = encode;
        }
        if (param.wireToUserHook) {
            let decode = TMTransportUtils.bufferTransformer(param.wireToUserHook);
            output.pipe(decode);
            output = decode;
        }
        if (param.identityResolver) {
            let resolve = new Stream.Transform({
                transform : function(chunk : [string, Buffer], encoding : BufferEncoding, callback) {
                    let processed = param.identityResolver(chunk[1]);
                    if (processed && processed[0]) {
                        this.push([chunk[0], [processed[1], processed[2]]], encoding);
                    }
                    callback();
                }
                , objectMode : true
            });
            output.pipe(resolve);
            output = resolve;
        }
        return [input, output];
    }
}

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
            , saveDataOnSeparateStorage : false
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
            this.redisClient = asyncRedis.createClient();
        }
        this.current = null;
    }

    async start() {
        if (this.config.duplicateFromRedis) {
            try {
                let redisReply = await this.redisClient.get(this.config.chainPrefix+":"+this.config.headKey);
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
                let x = etcdReply.responses[0].response_range;
                this.current = {
                    revision : BigInt(etcdReply.responses[0].response_range.kvs[0].mod_revision)
                    , id : this.config.headKey
                    , data : null
                    , nextID : etcdReply.responses[0].response_range.kvs[0].value.toString()
                }
            } else {
                this.current = {
                    revision : BigInt(etcdReply.responses[1].response_range.kvs[0].mod_revision)
                    , id : this.config.headKey
                    , data : null
                    , nextID : etcdReply.responses[1].response_range.kvs[0].value.toString()
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
                let v = cbor.decode(etcdReply.responses[0].response_range.kvs[0].value);
                this.current = {
                    revision : BigInt(etcdReply.responses[0].response_range.kvs[0].mod_revision)
                    , id : this.config.headKey
                    , data : null
                    , nextID : v[1]
                }
            } else {
                let v = cbor.decode(etcdReply.responses[1].response_range.kvs[0].value);
                this.current = {
                    revision : BigInt(etcdReply.responses[1].response_range.kvs[0].mod_revision)
                    , id : this.config.headKey
                    , data : null
                    , nextID : v[1]
                }
            }
        }
    }

    async next() : Promise<boolean> {
        if (this.config.duplicateFromRedis) {
            if (this.current.nextID == '') {
                let thisID = this.current.id;
                try {
                    let redisReply = await this.redisClient.get(this.config.chainPrefix+":"+thisID);
                    let parsed = cbor.decode(redisReply);
                    if (parsed.length == 3) {
                        let nextID = parsed[2];
                        if (nextID != '') {
                            redisReply = await this.redisClient.get(this.config.chainPrefix+":"+nextID);
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
                } catch (err) {
                }
            } else {
                let nextID = this.current.nextID;
                try {
                    let redisReply = await this.redisClient.get(this.config.chainPrefix+":"+nextID);
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
                let parsed = cbor.decode(etcdReply.kvs[0].value);
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

    async append(newID : string, newData : any) {
        while (true) {
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
                    .then(
                        this.client.put(this.config.chainPrefix+":"+thisID).value(
                            cbor.encode([this.current.data, newID])
                        )
                        , this.client.put(this.config.chainPrefix+":"+newID).value("")
                    )
                    .commit();
                succeeded = etcdReply.succeeded;
                if (succeeded) {
                    revision = BigInt(etcdReply.responses[1].response_put.header.revision);
                }
            }
            if (succeeded) {
                if (this.config.automaticallyDuplicateToRedis) {
                    await this.redisClient.set(
                        this.config.chainPrefix+":"+thisID
                        , cbor.encode([this.current.revision, this.current.data, newID])
                    );
                }
                this.current = {
                    revision : revision
                    , id : newID
                    , data : newData
                    , nextID : ""
                };
                break;
            }
        }
    }

    currentValue() {
        return this.current;
    }
}