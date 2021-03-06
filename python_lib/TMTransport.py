import re
from typing import TypedDict,Callable,Optional,Tuple,List,Any,Dict
from enum import Enum
from datetime import date

import asyncio
import aio_pika
import cbor
import socket
import struct
import aioredis
import zmq
import zmq.asyncio
import uuid
import etcd3
import redis

class ConnectionLocatorProperties(TypedDict):
    name : str
    value : str

class ConnectionLocator:
    host : str = ""
    port : int = 0
    username : str = ""
    password : str = ""
    identifier : str = ""
    properties : ConnectionLocatorProperties = {}
    def __init__(self, description : str):
        idx = description.find('[')
        if idx < 0:
            mainPortion = description
            propertyPortion = ""
        else:
            mainPortion = description[0:idx]
            propertyPortion = description[idx:]
        mainParts = mainPortion.split(':')
        if len(propertyPortion) > 2 and propertyPortion[-1] == ']':
            realPropertyPortion = propertyPortion[1:len(propertyPortion)-1]
            propertyParts = realPropertyPortion.split(',')
            for p in propertyParts:
                nameAndValue = p.split('=')
                if len(nameAndValue) == 2:
                    [name, value] = nameAndValue
                    self.properties[name] = value
        if len(mainParts) >= 1:
            self.host = mainParts[0]
        if len(mainParts) >= 2:
            if mainParts[1] == "":
                self.port = 0
            else:
                self.port = int(mainParts[1])
        if len(mainParts) >= 3:
            self.username = mainParts[2]
        if len(mainParts) >= 4:
            self.password = mainParts[3]
        if len(mainParts) >= 5:
            self.identifier = mainParts[4]
    def checkBooleanProperty(self, key : str) -> bool :
        if key not in self.properties:
            return False
        return self.properties[key] == "true"


class Transport(Enum):
    Multicast = 0
    RabbitMQ = 1
    Redis = 2
    ZeroMQ = 3

class TopicMatchType(Enum):
    MatchAll = 0
    MatchExact = 1
    MatchRE = 2

class TopicSpec:
    matchType : TopicMatchType = TopicMatchType.MatchAll
    exactString : str = ""
    regex : re.Pattern = re.compile("")
    def __init__(self, topic : str = None, complexTopic : str = None):
        if complexTopic:
            if complexTopic == "":
                self.matchType = TopicMatchType.MatchAll
                self.exactString = ""
                self.regex = re.compile("")
            elif len(complexTopic) > 3 and complexTopic[0:2] == "r/" and complexTopic[-1] == "/":
                self.matchType = TopicMatchType.MatchRE
                self.exactString = ""
                self.regex = re.compile(complexTopic[2:len(complexTopic)-1])
            else:
                self.matchType = TopicMatchType.MatchExact
                self.exactString = complexTopic
                self.regex = re.compile("")
        elif topic:
            self.matchType = TopicMatchType.MatchExact
            self.exactString = topic
            self.regex = re.compile("")

class TMTransportUtils:
    @staticmethod
    def parseConnectionLocator(locatorStr : str) -> ConnectionLocator :
        return ConnectionLocator(locatorStr)
    @staticmethod
    def parseAddress(address : str) -> Tuple[Transport, ConnectionLocator]:
        if address.startswith("multicast://"):
            return (Transport.Multicast, TMTransportUtils.parseConnectionLocator(address[len("multicast://"):]))
        elif address.startswith("rabbitmq://"):
            return (Transport.RabbitMQ, TMTransportUtils.parseConnectionLocator(address[len("rabbitmq://"):]))
        elif address.startswith("redis://"):
            return (Transport.Redis, TMTransportUtils.parseConnectionLocator(address[len("redis://"):]))
        elif address.startswith("zeromq://"):
            return (Transport.ZeroMQ, TMTransportUtils.parseConnectionLocator(address[len("zeromq://"):]))
        else:
            return None
    @staticmethod
    def parseTopic(transport : Transport, topic : str) -> TopicSpec :
        if transport == Transport.Multicast:
            return TopicSpec(complexTopic=topic)
        elif transport == Transport.RabbitMQ:
            return TopicSpec(topic=topic)
        elif transport == Transport.Redis:
            return TopicSpec(topic=topic)
        elif transport == Transport.ZeroMQ:
            return TopicSpec(complexTopic=topic)
        else:
            return None
    @staticmethod
    async def createAMQPConnection(locator : ConnectionLocator) -> aio_pika.RobustConnection :
        if locator.checkBooleanProperty("ssl"):
            if "ca_cert" not in locator.properties:
                return None
            url = f"amqps://{locator.username}:{locator.password}@{locator.host}"
            if locator.port > 0:
                url += f":{locator.port}"
            if "vhost" in locator.properties:
                url += f"/{locator.properties['vhost']}"
            else:
                url += '/'
            url += f"?ca_certs={locator.properties['ca_cert']}"
            if "client_key" in locator.properties and "client_cert" in locator.properties:
                url += f"&certfile={locator.properties['client_cert']}&keyfile={locator.properties['client_key']}"
        else:
            url = f"amqp://{locator.username}:{locator.password}@{locator.host}"
            if locator.port > 0:
                url += f":{locator.port}"
            if "vhost" in locator.properties:
                url += f"/{locator.properties['vhost']}"
            else:
                url += '/'
        connection = await aio_pika.connect_robust(url)
        return connection
    @staticmethod
    def bufferTransformer(f : Callable[[bytes],Optional[bytes]], qin : asyncio.Queue, qouts : List[asyncio.Queue]) -> asyncio.Task :
        async def taskcr() :
            while True:
                item : Tuple[str,bytes] = await qin.get()
                strInfo, data = item
                processed = f(data)
                if processed:
                    for q in qouts:
                        q.put_nowait([strInfo, processed])
        return asyncio.create_task(taskcr())

class MultiTransportListener:
    @staticmethod
    def defaultTopic(transport : Transport) -> str:
        if transport == Transport.Multicast:
            return ""
        elif transport == Transport.RabbitMQ:
            return "#"
        elif transport == Transport.Redis:
            return "*"
        elif transport == Transport.ZeroMQ:
            return ""
        else:
            return ""

    @staticmethod
    def multicastInput(locator: ConnectionLocator, topic : TopicSpec, qouts : List[asyncio.Queue]) -> asyncio.Task:
        filter : Callable[[bytes], bool] = lambda x : True
        if topic.matchType == TopicMatchType.MatchAll:
            filter = lambda x : True
        elif topic.matchType == TopicMatchType.MatchExact:
            filter = lambda x : x == topic.exactString
        elif topic.matchType == TopicMatchType.MatchRE:
            filter = lambda x : bool(topic.regex.fullmatch(x))

        class MulticastListener:
            def connection_made(self, transport):
                pass
            def datagram_received(self, data, addr):
                decoded = cbor.loads(data)
                if len(decoded) == 2:
                    if filter(decoded[0]):
                        for q in qouts:
                            q.put_nowait((decoded[0], decoded[1]))

        async def taskcr():
            loop = asyncio.get_running_loop()
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('', locator.port))
            group = socket.inet_aton(locator.host)
            mreq = struct.pack('4sL', group, socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            await loop.create_datagram_endpoint(
                lambda: MulticastListener()
                , sock=sock
            )
        return asyncio.create_task(taskcr())
    
    @staticmethod
    def rabbitmqInput(locator : ConnectionLocator, topic : TopicSpec, qouts : List[asyncio.Queue]) -> asyncio.Task :
        async def taskcr():
            connection = await TMTransportUtils.createAMQPConnection(locator)
            channel = await connection.channel()
            exchange = await channel.declare_exchange(
                name = locator.identifier
                , type = aio_pika.ExchangeType.TOPIC
                , passive = locator.checkBooleanProperty("passive")
                , durable = locator.checkBooleanProperty("durable")
                , auto_delete = locator.checkBooleanProperty("autoDelete")
            )
            queue = await channel.declare_queue(auto_delete=True)
            await queue.bind(exchange, topic.exactString)
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        for q in qouts:
                            q.put_nowait((message.routing_key, message.body))
        return asyncio.create_task(taskcr())

    @staticmethod
    def redisInput(locator : ConnectionLocator, topic : TopicSpec, qouts : List[asyncio.Queue]) -> asyncio.Task :
        async def taskcr():
            redisURL = f"redis://{locator.host}"
            if locator.port > 0:
                redisURL = redisURL+f":{locator.port}"
            connection = await aioredis.create_redis_pool(redisURL)
            channel, = await connection.psubscribe(topic.exactString)
            async for ch, message in channel.iter():
                for q in qouts:
                    q.put_nowait((ch.decode('utf-8'), message))
        return asyncio.create_task(taskcr())

    @staticmethod
    def zeromqInput(locator : ConnectionLocator, topic : TopicSpec, qouts : List[asyncio.Queue]) -> asyncio.Task :
        filter : Callable[[bytes], bool] = lambda x : True
        if topic.matchType == TopicMatchType.MatchAll:
            filter = lambda x : True
        elif topic.matchType == TopicMatchType.MatchExact:
            filter = lambda x : x == topic.exactString
        elif topic.matchType == TopicMatchType.MatchRE:
            filter = lambda x : bool(topic.regex.fullmatch(x))

        async def taskcr():
            ctx = zmq.asyncio.Context()
            sock = ctx.socket(zmq.SUB)
            if locator.host == 'inproc' or locator.host == 'ipc':
                sock.connect(f"{locator.host}://{locator.identifier}")
            else:
                sock.connect(f"tcp://{locator.host}:{locator.port}")
            sock.subscribe("")
            while True:
                data = await sock.recv()
                decoded = cbor.loads(data)
                if len(decoded) == 2:
                    if filter(decoded[0]):
                        for q in qouts:
                            q.put_nowait((decoded[0], decoded[1]))
        return asyncio.create_task(taskcr())

    @staticmethod
    def input(address : str, qouts : List[asyncio.Queue], topic : str = None, wireToUserHook : Callable[[bytes],bytes] = None) -> List[asyncio.Task] :
        ret = []
        parsedAddr = TMTransportUtils.parseAddress(address)
        if not parsedAddr:
            return ret
        transport, locator = parsedAddr
        if topic:
            parsedTopic = TMTransportUtils.parseTopic(transport, topic)
        else:
            parsedTopic = TMTransportUtils.parseTopic(transport, MultiTransportListener.defaultTopic(transport))
        if not parsedTopic:
            return ret
        realQouts = qouts
        if wireToUserHook:
            qin = asyncio.Queue()
            ret.append(TMTransportUtils.bufferTransformer(wireToUserHook, qin, qouts))
            realQouts = [qin]
        if transport == Transport.Multicast:
            ret.append(MultiTransportListener.multicastInput(locator, parsedTopic, realQouts))
        elif transport == Transport.RabbitMQ:
            ret.append(MultiTransportListener.rabbitmqInput(locator, parsedTopic, realQouts))
        elif transport == Transport.Redis:
            ret.append(MultiTransportListener.redisInput(locator, parsedTopic, realQouts))
        elif transport == Transport.ZeroMQ:
            ret.append(MultiTransportListener.zeromqInput(locator, parsedTopic, realQouts))
        return ret

class MultiTransportPublisher:
    @staticmethod
    def multicastOutput(locator : ConnectionLocator, qin : asyncio.Queue) -> asyncio.Task :
        class MulticastListener:
            def connection_made(self, transport):
                pass
            def datagram_received(self, data, addr):
                pass
        async def taskcr():
            loop = asyncio.get_running_loop()
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            if 'ttl' in locator.properties:
                ttl = struct.pack('b', int(locator.properties['ttl']))
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            group = socket.inet_aton(locator.host)
            mreq = struct.pack('4sL', group, socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            transport,protocol = await loop.create_datagram_endpoint(
                lambda: MulticastListener()
                , sock=sock
            )
            while True:
                topic, data = await qin.get()
                transport.sendto(cbor.dumps([topic, data]), (locator.host, locator.port))            
        return asyncio.create_task(taskcr())

    @staticmethod
    def rabbitmqOutput(locator : ConnectionLocator, qin : asyncio.Queue) -> asyncio.Task :
        async def taskcr():
            connection = await TMTransportUtils.createAMQPConnection(locator)
            channel = await connection.channel()
            exchange = await channel.declare_exchange(
                name = locator.identifier
                , type = aio_pika.ExchangeType.TOPIC
                , passive = locator.checkBooleanProperty("passive")
                , durable = locator.checkBooleanProperty("durable")
                , auto_delete = locator.checkBooleanProperty("autoDelete")
            )
            while True:
                topic, data = await qin.get()
                await exchange.publish(
                    aio_pika.Message(
                        data
                        , delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT
                        , expiration=1000
                    )
                    , routing_key = topic
                )
        return asyncio.create_task(taskcr())

    @staticmethod
    def redisOutput(locator : ConnectionLocator, qin : asyncio.Queue) -> asyncio.Task :
        async def taskcr():
            redisURL = f"redis://{locator.host}"
            if locator.port > 0:
                redisURL = redisURL+f":{locator.port}"
            connection = await aioredis.create_redis_pool(redisURL)
            while True:
                topic, data = await qin.get()
                await connection.publish(topic, data)
        return asyncio.create_task(taskcr())

    @staticmethod
    def zeromqOutput(locator : ConnectionLocator, qin : asyncio.Queue) -> asyncio.Task :
        async def taskcr():
            ctx = zmq.asyncio.Context()
            sock = ctx.socket(zmq.PUB)
            if locator.host == 'inproc' or locator.host == 'ipc':
                sock.bind(f"{locator.host}://{locator.identifier}")
            else:
                sock.bind(f"tcp://{locator.host}:{locator.port}")
            while True:
                topic, data = await qin.get()
                await sock.send(cbor.dumps([topic, data]))
        return asyncio.create_task(taskcr())

    @staticmethod
    def output(address : str, qin : asyncio.Queue, userToWireHook : Callable[[bytes],bytes] = None) -> List[asyncio.Task]:
        ret = []
        parsedAddr = TMTransportUtils.parseAddress(address)
        if not parsedAddr:
            return ret
        transport, locator = parsedAddr
        realQin = qin
        if userToWireHook:
            realQin = asyncio.Queue()
            ret.append(TMTransportUtils.bufferTransformer(userToWireHook, qin, [realQin]))
        if transport == Transport.Multicast:
            ret.append(MultiTransportPublisher.multicastOutput(locator, realQin))
        elif transport == Transport.RabbitMQ:
            ret.append(MultiTransportPublisher.rabbitmqOutput(locator, realQin))
        elif transport == Transport.Redis:
            ret.append(MultiTransportPublisher.redisOutput(locator, realQin))
        elif transport == Transport.ZeroMQ:
            ret.append(MultiTransportPublisher.zeromqOutput(locator, realQin))
        return ret

class FacilityOutput:
    id : str = ""
    originalInput : bytes = b''
    output : bytes = b''
    isFinal : bool = False

class InputMap(TypedDict):
    id : str
    input : bytes

class MultiTransportFacilityClient:
    @staticmethod
    def rabbitMQFacility(locator : ConnectionLocator, qin : asyncio.Queue, qout : asyncio.Queue) -> List[asyncio.Task]:
        ret = []
        async def taskcr():
            connection = await TMTransportUtils.createAMQPConnection(locator)
            channel = await connection.channel()
            queue = await channel.declare_queue(exclusive=True, auto_delete=True)

            inputMap : InputMap = {}

            async def readQueue():
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            isFinal = (message.content_type == 'final')
                            res = message.body
                            id = message.correlation_id
                            if id not in inputMap:
                                continue
                            input = inputMap[id]

                            output = FacilityOutput()
                            output.id = id
                            output.originalInput = input
                            output.output = res
                            output.isFinal = isFinal

                            qout.put_nowait(output)

                            if isFinal:
                                del inputMap[id]

            ret.append(asyncio.create_task(readQueue()))

            while True:
                id, data = await qin.get()
                inputMap[id] = data  
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        data
                        , correlation_id=id
                        , reply_to=queue.name
                        , delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT
                        , expiration=1000
                    )
                    , routing_key = locator.identifier
                )

        ret.append(asyncio.create_task(taskcr()))
        return ret
    
    @staticmethod
    def redisFacility(locator : ConnectionLocator, qin : asyncio.Queue, qout : asyncio.Queue) -> List[asyncio.Task]:
        ret = []
        async def taskcr():
            redisURL = f"redis://{locator.host}"
            if locator.port > 0:
                redisURL = redisURL+f":{locator.port}"
            connection = await aioredis.create_redis_pool(redisURL)

            inputMap : InputMap = {}

            streamID = str(uuid.uuid4())

            channel, = await connection.subscribe(streamID)

            async def readQueue():
                async for message in channel.iter():
                    isFinal, idAndRes = cbor.loads(message)
                    id, res = idAndRes
                    if id not in inputMap:
                        continue
                    input = inputMap[id]

                    output = FacilityOutput()
                    output.id = id
                    output.originalInput = input
                    output.output = res
                    output.isFinal = isFinal

                    qout.put_nowait(output)

                    if isFinal:
                        del inputMap[id]

            ret.append(asyncio.create_task(readQueue()))

            while True:
                id, data = await qin.get()
                inputMap[id] = data  
                await connection.publish(locator.identifier, cbor.dumps((streamID, cbor.dumps((id, data)))))

        ret.append(asyncio.create_task(taskcr()))
        return ret

    @staticmethod
    def facility(address : str, qin : asyncio.Queue, qout : asyncio.Queue, userToWireHook : Callable[[bytes],bytes] = None, wireToUserHook : Callable[[bytes],bytes] = None, identityAttacher : Callable[[bytes],bytes] = None) -> List[asyncio.Task]:
        ret = []
        parsedAddr = TMTransportUtils.parseAddress(address)
        if not parsedAddr:
            return []
        transport, locator = parsedAddr
        realQin = qin
        if identityAttacher:
            attacherQin = asyncio.Queue()
            ret.append(TMTransportUtils.bufferTransformer(identityAttacher, realQin, [attacherQin]))
            realQin = attacherQin
        if userToWireHook:
            userToWireHookQin = asyncio.Queue()
            ret.append(TMTransportUtils.bufferTransformer(userToWireHook, realQin, [userToWireHookQin]))
            realQin = userToWireHookQin
        realQout = qout
        if wireToUserHook:
            hookQin = asyncio.Queue()
            hookQout = qout
            async def hookcr() :
                while True:
                    item : FacilityOutput = await resolverQin.get()
                    newData = wireToUserHook(item.output)
                    if newData:
                        item.output = newData
                        hookQout.put_nowait(item)
            ret.append(asyncio.create_task(hookcr()))
            realQout = hookQin
        if transport == Transport.RabbitMQ:
            ret.extend(MultiTransportFacilityClient.rabbitMQFacility(locator, realQin, realQout))
        elif transport == Transport.Redis:
            ret.extend(MultiTransportFacilityClient.redisFacility(locator, realQin, realQout))
        return ret
    
    @staticmethod
    def keyify(x : bytes) -> Tuple[str,bytes]:
        return (str(uuid.uuid4()), x)

class MultiTransportFacilityServer:
    @staticmethod
    def rabbitMQFacility(locator : ConnectionLocator, qin : asyncio.Queue, qout : asyncio.Queue) -> List[asyncio.Task]:
        ret = []
        class ReplyMap(TypedDict):
            id : str
            info : Tuple[str, str]
        async def taskcr():
            connection = await TMTransportUtils.createAMQPConnection(locator)
            channel = await connection.channel()
            queue = await channel.declare_queue(locator.identifier, durable=locator.checkBooleanProperty("durable"))

            replyMap : ReplyMap = {}

            async def readQueue():
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            res = message.body
                            id = message.correlation_id
                            if id in replyMap:
                                continue
                            replyMap[id] = message.reply_to
                            
                            qout.put_nowait((id,res))

            ret.append(asyncio.create_task(readQueue()))

            while True:
                id, data, isFinal = await qin.get()
                if id not in replyMap:
                    continue
                replyTo = replyMap[id]
                if isFinal:
                    del replyMap[id]
                if isFinal:
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            data
                            , correlation_id=id
                            , reply_to=locator.identifier
                            , content_type='final'
                        )
                        , routing_key = replyTo
                    )
                else:
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            data
                            , correlation_id=id
                            , reply_to=locator.identifier
                        )
                        , routing_key = replyTo
                    )

        ret.append(asyncio.create_task(taskcr()))
        return ret
    
    @staticmethod
    def redisFacility(locator : ConnectionLocator, qin : asyncio.Queue, qout : asyncio.Queue) -> List[asyncio.Task]:
        ret = []
        class ReplyMap(TypedDict):
            id : str
            info : str
        async def taskcr():
            redisURL = f"redis://{locator.host}"
            if locator.port > 0:
                redisURL = redisURL+f":{locator.port}"
            connection = await aioredis.create_redis_pool(redisURL)

            replyMap : ReplyMap = {}

            channel, = await connection.subscribe(locator.identifier)

            async def readQueue():
                async for message in channel.iter():
                    try:
                        replyTo, m = cbor.loads(message)
                        id, data = cbor.loads(m)
                    except:
                        continue
                    if id in replyMap:
                        continue
                    replyMap[id] = replyTo
                    qout.put_nowait((id, data))

            ret.append(asyncio.create_task(readQueue()))

            while True:
                id, data, isFinal = await qin.get()
                if id not in replyMap:
                    continue
                replyTo = replyMap[id]
                if isFinal:
                    del replyMap[id]
                await connection.publish(replyTo, cbor.dumps((isFinal,(id, data))))

        ret.append(asyncio.create_task(taskcr()))
        return ret

    @staticmethod
    def facility(address : str, logicReadFrom : asyncio.Queue, logicWriteTo : asyncio.Queue, userToWireHook : Callable[[bytes],bytes] = None, wireToUserHook : Callable[[bytes],bytes] = None, identityResolver : Callable[[bytes],Tuple[bool,str,bytes]] = None) -> List[asyncio.Task]:
        ret = []
        parsedAddr = TMTransportUtils.parseAddress(address)
        if not parsedAddr:
            return ret
        transport, locator = parsedAddr

        fromExternalQ = logicReadFrom
        if wireToUserHook:
            wireToUserHookQin = asyncio.Queue()
            ret.append(TMTransportUtils.bufferTransformer(wireToUserHook, wireToUserHookQin, [fromExternalQ]))
            fromExternalQ = wireToUserHookQin
        if identityResolver:
            resolverQin = asyncio.Queue()
            resolverQout = fromExternalQ
            async def resolvercr() :
                while True:
                    item : Tuple[str,bytes] = await resolverQin.get()
                    strInfo, data = item
                    good, identity, processedData = identityResolver(data)
                    if good:
                        resolverQout.put_nowait((strInfo, (identity, processedData)))
            ret.append(asyncio.create_task(resolvercr()))
            fromExternalQ = resolverQin

        toExternalQ = logicWriteTo
        if userToWireHook:
            hookQin = logicWriteTo
            hookQout = asyncio.Queue()
            async def hookcr() :
                while True:
                    item : Tuple[str,bytes,bool] = await hookQin.get()
                    newData = userToWireHook(item[1])
                    if newData:
                        hookQout.put_nowait((item[0], newData, item[2]))
            ret.append(asyncio.create_task(hookcr()))
            toExternalQ = hookQout
          
        if transport == Transport.RabbitMQ:
            ret.extend(MultiTransportFacilityServer.rabbitMQFacility(locator, toExternalQ, fromExternalQ))
        elif transport == Transport.Redis:
            ret.extend(MultiTransportFacilityServer.redisFacility(locator, toExternalQ, fromExternalQ))
        return ret

class EtcdSharedChainConfiguration(TypedDict):
    etcd3Options : Dict[str,Any]
    headKey : str
    saveDataOnSeparateStorage : bool
    chainPrefix : str
    dataPrefix : str
    extraDataPrefix : str
    redisServerAddr : str
    duplicateFromRedis : bool
    redisTTLSeconds : int
    automaticallyDuplicateToRedis : bool

class EtcdSharedChainItem(TypedDict):
    revision: int
    id : str
    data : Any
    nextID : str

class EtcdSharedChain:
    config : EtcdSharedChainConfiguration
    client : Any
    redisClient : Any
    current : EtcdSharedChainItem

    @staticmethod
    def defaultEtcdSharedChainConfiguration(commonPrefix : str = '', useDate : bool = True) -> EtcdSharedChainConfiguration:
        today = date.today().strftime('%Y_%m_%d')
        ret : EtcdSharedChainConfiguration = {
            'etcd3Options' : {'host':'127.0.0.1', 'port':2379}
            , 'headKey' : ''
            , 'saveDataOnSeparateStorage' : False
            , 'chainPrefix' : commonPrefix+'_'+(today if useDate else '')+'_chain'
            , 'dataPrefix' : commonPrefix+'_'+(today if useDate else '')+'_data'
            , 'extraDataPrefix' : commonPrefix+'_'+(today if useDate else '')+'_extra_data'
            , 'redisServerAddr' : '127.0.0.1:6379'
            , 'duplicateFromRedis' : False
            , 'redisTTLSeconds' : 0
            , 'automaticallyDuplicateToRedis' : False
        }
        return ret

    def __init__(self, config : EtcdSharedChainConfiguration):
        self.config = config
        self.client = etcd3.Etcd3Client(**(config['etcd3Options']))
        if (config['duplicateFromRedis'] or config['automaticallyDuplicateToRedis']):
            [host, portStr] = config['redisServerAddr'].split(':')
            self.redisClient = redis.Redis(host=host, port=int(portStr))
        self.current = {}
    
    def start(self, defaultData : Any):
        if self.config['duplicateFromRedis']:
            redisReply = self.redisClient.get(self.config['chainPrefix']+":"+self.config['headKey'])
            if not (redisReply is None):
                parsed = cbor.loads(redisReply)
                if len(parsed) == 3:
                    self.current = {
                        'revision' : parsed[0]
                        , 'id' : self.config['headKey']
                        , 'data' : parsed[1]
                        , 'nextID' : parsed[2]
                    }
                    return
        if self.config['saveDataOnSeparateStorage']:
            etcdReplySucceeded, etcdReply = self.client.transaction(
                compare=[
                    self.client.transactions.version(self.config['chainPrefix']+":"+self.config['headKey']) > 0
                ]
                , success=[
                    self.client.transactions.get(self.config['chainPrefix']+":"+self.config['headKey'])
                ]
                , failure=[
                    self.client.transactions.put(self.config['chainPrefix']+":"+self.config['headKey'], '')
                    , self.client.transactions.get(self.config['chainPrefix']+":"+self.config['headKey'])
                ]
            )
            if etcdReplySucceeded:
                self.current = {
                    'revision' : etcdReply[0][0][1].mod_revision
                    , 'id' : self.config['headKey']
                    , 'data' : defaultData
                    , 'nextID' : etcdReply[0][0][0].decode('utf-8')
                }
            else:
                self.current = {
                    'revision' : etcdReply[1][0][1].mod_revision
                    , 'id' : self.config['headKey']
                    , 'data' : defaultData
                    , 'nextID' : ''
                }
        else:
            etcdReplySucceeded, etcdReply = self.client.transaction(
                compare=[
                    self.client.transactions.version(self.config['chainPrefix']+":"+self.config['headKey']) > 0
                ]
                , success=[
                    self.client.transactions.get(self.config['chainPrefix']+":"+self.config['headKey'])
                ]
                , failure=[
                    self.client.transactions.put(self.config['chainPrefix']+":"+self.config['headKey'], '')
                    , self.client.transactions.get(self.config['chainPrefix']+":"+self.config['headKey'])
                ]
            )
            if etcdReplySucceeded:
                x = etcdReply[0][0][0]
                if (len(x) > 0):
                    v = cbor.loads(x)
                    self.current = {
                        'revision' : etcdReply[0][0][1].mod_revision
                        , 'id' : self.config['headKey']
                        , 'data' : defaultData
                        , 'nextID' : v[1]
                    }
                else:
                    self.current = {
                        'revision' : etcdReply[0][0][1].mod_revision
                        , 'id' : self.config['headKey']
                        , 'data' : defaultData
                        , 'nextID' : ''
                    }
            else:
                self.current = {
                    'revision' : etcdReply[1][0][1].mod_revision
                    , 'id' : self.config['headKey']
                    , 'data' : defaultData
                    , 'nextID' : ''
                }
    
    def tryLoadUntil(self, id : str) -> bool:
        if self.config['duplicateFromRedis']:
            redisReply = self.redisClient.get(self.config['chainPrefix']+":"+id)
            if not (redisReply is None):
                parsed = cbor.loads(redisReply)
                if len(parsed) == 3:
                    self.current = {
                        'revision' : parsed[0]
                        , 'id' : id
                        , 'data' : parsed[1]
                        , 'nextID' : parsed[2]
                    }
                    return True
        if self.config['saveDataOnSeparateStorage']:
            print(id)
            etcdReplySucceeded, etcdReply = self.client.transaction(
                compare=[
                    self.client.transactions.version(self.config['chainPrefix']+":"+id) > 0
                ]
                , success=[
                    self.client.transactions.get(self.config['chainPrefix']+":"+id)
                    , self.client.transactions.get(self.config['dataPrefix']+":"+id)
                ]
                , failure=[]
            )
            if etcdReplySucceeded:
                self.current = {
                    'revision' : etcdReply[0][0][1].mod_revision
                    , 'id' : id
                    , 'data' : cbor.loads(etcdReply[1][0][0])
                    , 'nextID' : etcdReply[0][0][0].decode('utf-8')
                }
                return True
            else:
                return False
        else:
            etcdReplySucceeded, etcdReply = self.client.transaction(
                compare=[
                    self.client.transactions.version(self.config['chainPrefix']+":"+id) > 0
                ]
                , success=[
                    self.client.transactions.get(self.config['chainPrefix']+":"+id)
                ]
                , failure=[]
            )
            if etcdReplySucceeded:
                parsed = cbor.loads(etcdReply[0][0][0])
                self.current = {
                    'revision' : etcdReply[0][0][1].mod_revision
                    , 'id' : id
                    , 'data' : parsed[0]
                    , 'nextID' : parsed[1]
                }
                return True
            else:
                return False
    
    def next(self) -> bool:
        if self.config['duplicateFromRedis']:
            if self.current['nextID'] == '':
                thisID = self.current['id']
                redisReply = self.redisClient.get(self.config['chainPrefix']+":"+thisID)
                if not (redisReply is None):
                    parsed = cbor.loads(redisReply)
                    if len(parsed) == 3:
                        nextID = parsed[2]
                        if nextID != '':
                            redisReply = self.redisClient.get(self.config['chainPrefix']+":"+nextID)
                            if not (redisReply is None):
                                parsed = cbor.loads(redisReply)
                                if len(parsed) == 3:
                                    self.current = {
                                        'revision' : parsed[0]
                                        , 'id' : nextID
                                        , 'data' : parsed[1]
                                        , 'nextID' : parsed[2]
                                    }
                                    return True
            else:
                nextID = self.current['nextID']
                redisReply = self.redisClient.get(self.config['chainPrefix']+":"+nextID);
                if not (redisReply is None):
                    parsed = cbor.loads(redisReply);
                    if len(parsed) == 3:
                        self.current = {
                            'revision' : parsed[0]
                            , 'id' : nextID
                            , 'data' : parsed[1]
                            , 'nextID' : parsed[2]
                        }
                        return True
        if self.config['saveDataOnSeparateStorage']:
            nextID = self.current['nextID']
            if nextID == '':
                etcdReply = self.client.get(self.config['chainPrefix']+":"+self.current['id'])
                nextID = etcdReply[0].decode('utf-8')
                if nextID == '':
                    return False
            nextEtcdReplySucceeded, nextEtcdReply = self.client.transaction(
                compare=[
                    self.client.transactions.version(self.config['chainPrefix']+":"+nextID) > 0
                ]
                , success=[
                    self.client.transactions.get(self.config['chainPrefix']+":"+nextID)
                    , self.client.transactions.get(self.config['dataPrefix']+":"+nextID)
                ]
                , failure=[]
            )
            if nextEtcdReplySucceeded:
                self.current = {
                    'revision' : nextEtcdReply[0][0][1].mod_revision
                    , 'id' : nextID
                    , 'data' : cbor.loads(nextEtcdReply[1][0][0])
                    , 'nextID' : nextEtcdReply[0][0][0].decode('utf-8')
                }
                return True
            else:
                return False
        else:
            nextID = self.current['nextID']
            if nextID == '':
                etcdReply = self.client.get(self.config['chainPrefix']+":"+self.current['id'])
                x = etcdReply[0]
                if len(x) == 0:
                    return False
                parsed = cbor.loads(x)
                nextID = parsed[1]
                if nextID == '':
                    return False
            nextEtcdReply = self.client.get(self.config['chainPrefix']+":"+nextID)
            parsed = cbor.loads(nextEtcdReply[0]);
            self.current = {
                'revision' : nextEtcdReply[1].mod_revision
                , 'id' : nextID
                , 'data' : parsed[0]
                , 'nextID' : parsed[1]
            }
            return True

    def idIsAlreadyOnChain(self, id : str) -> bool:
        etcdReply = self.client.transaction(
            compare=[
                self.client.transactions.version(self.config['chainPrefix']+":"+id) > 0
            ]
            , success=[]
            , failure=[]
        )
        return etcdReply[0]

    def tryAppend(self, newID : str, newData : Any) -> bool:
        while True:
            x = self.next()
            if not x:
                break
        thisID = self.current['id']
        succeeded = False
        revision = 0
        if self.config['saveDataOnSeparateStorage']:
            etcdReplySucceeded, etcdReply = self.client.transaction(
                compare=[
                    self.client.transactions.mod(self.config['chainPrefix']+":"+thisID) == self.current['revision']
                    , self.client.transactions.version(self.config['dataPrefix']+":"+newID) == 0
                    , self.client.transactions.version(self.config['chainPrefix']+":"+newID) == 0
                ]
                , success=[
                    self.client.transactions.put(self.config['chainPrefix']+":"+thisID, newID)
                    , self.client.transactions.put(self.config['dataPrefix']+":"+newID, cbor.dumps(newData))
                    , self.client.transactions.put(self.config['chainPrefix']+":"+newID, "")
                ]
                , failure=[]
            )
            succeeded = etcdReplySucceeded
            if succeeded:
                revision = etcdReply[2].response_put.header.revision
        else:
            etcdReplySucceeded, etcdReply = self.client.transaction(
                compare=[
                    self.client.transactions.mod(self.config['chainPrefix']+":"+thisID) == self.current['revision']
                    , self.client.transactions.version(self.config['chainPrefix']+":"+newID) == 0
                ]
                , success=[
                    self.client.transactions.put(self.config['chainPrefix']+":"+thisID, cbor.dumps([self.current['data'], newID]))
                    , self.client.transactions.put(self.config['chainPrefix']+":"+newID, cbor.dumps([newData,""]))
                ]
                , failure=[]
            )
            succeeded = etcdReplySucceeded
            if succeeded:
                revision = etcdReply[1].response_put.header.revision
        if succeeded:
            if self.config['automaticallyDuplicateToRedis']:
                ttl = self.config['redisTTLSeconds']
                if ttl > 0:
                    self.redisClient.set(
                        self.config['chainPrefix']+":"+thisID
                        , cbor.dumps([self.current['revision'], self.current['data'], newID])
                        , ex = ttl
                    )
                else:
                    self.redisClient.set(
                        self.config['chainPrefix']+":"+thisID
                        , cbor.dumps([self.current['revision'], self.current['data'], newID])
                    )
            self.current = {
                'revision' : revision
                , 'id' : newID
                , 'data' : newData
                , 'nextID' : ""
            }
        return succeeded

    def append(self, newID : str, newData : Any):
        while True:
            res = self.tryAppend(newID, newData);
            if res:
                break

    def saveExtraData(self, key : str, data : Any):
        self.client.put(self.config['extraDataPrefix']+":"+key, cbor.dumps(data))

    def loadExtraData(self, key : str) -> Any:
        etcdReply = self.client.get(self.config['extraDataPrefix']+":"+key)
        if len(etcdReply) == 0:
            return None
        elif etcdReply[0] is None:
            return None
        else:
            return cbor.loads(etcdReply[0])

    def close(self):
        if (self.config['duplicateFromRedis'] or self.config['automaticallyDuplicateToRedis']):
            self.redisClient.close()
        self.client.close()

    def currentValue(self) -> EtcdSharedChainItem:
        return self.current

class RedisSharedChainConfiguration(TypedDict):
    redisServerAddr : str
    headKey : str
    chainPrefix : str
    dataPrefix : str
    extraDataPrefix : str

class RedisSharedChainItem(TypedDict):
    id : str
    data : Any
    nextID : str

class RedisSharedChain:
    config : RedisSharedChainConfiguration
    client : Any
    current : RedisSharedChainItem

    @staticmethod
    def defaultRedisSharedChainConfiguration(commonPrefix : str = '', useDate : bool = True) -> EtcdSharedChainConfiguration:
        today = date.today().strftime('%Y_%m_%d')
        ret : RedisSharedChainConfiguration = {
            'redisServerAddr' : '127.0.0.1:6379'
            , 'headKey' : ''
            , 'chainPrefix' : commonPrefix+'_'+(today if useDate else '')+'_chain'
            , 'dataPrefix' : commonPrefix+'_'+(today if useDate else '')+'_data'
            , 'extraDataPrefix' : commonPrefix+'_'+(today if useDate else '')+'_extra_data'
        }
        return ret

    def __init__(self, config : RedisSharedChainConfiguration):
        self.config = config
        [host, portStr] = config['redisServerAddr'].split(':')
        self.client = redis.Redis(host=host, port=int(portStr))
        self.current = {}

    def start(self, defaultData : Any):
        headKeyStr : str = self.config['chainPrefix']+":"+self.config['headKey']
        luaStr : str = "local x = redis.call('GET',KEYS[1]); if x then return x else redis.call('SET',KEYS[1],''); return '' end"
        redisReply = self.client.eval(luaStr, 1, headKeyStr)
        if not (redisReply is None):
            self.current = {
                'id' : self.config['headKey']
                , 'data' : defaultData
                , 'nextID' : redisReply.decode('utf-8')
            }

    def tryLoadUntil(self, id : str) -> bool:
        chainKey : str = self.config['chainPrefix']+":"+id
        dataKey : str = self.config['dataPrefix']+":"+id
        dataReply = self.client.get(dataKey)
        if dataReply is None:
            return False
        chainReply = self.client.get(chainKey)
        if chainReply is None:
            return False
        self.current = {
            'id' : id
            , 'data' : cbor.loads(dataReply)
            , 'nextID' : chainReply.decode('utf-8')
        }
        return True

    def next(self) -> bool:
        nextID : str = self.current['nextID']
        if nextID == '':
            chainKey : str = self.config['chainPrefix']+":"+self.current['id']
            redisReply = self.client.get(chainKey)
            if not (redisReply is None):
                nextID = redisReply.decode('utf-8')
            if nextID == '':
                return False
        chainKey = self.config['chainPrefix']+":"+nextID;
        dataKey = self.config['dataPrefix']+":"+nextID;
        dataReply = self.client.get(dataKey);
        if dataReply is None:
            return False      
        chainReply = self.client.get(chainKey)
        if chainReply is None:
            return False
        self.current = {
            'id' : nextID
            , 'data' : cbor.loads(dataReply)
            , 'nextID' : chainReply.decode('utf-8')
        }
        return True

    def idIsAlreadyOnChain(self, id : str) -> bool:
        chainKey : str = self.config['chainPrefix']+":"+id
        chainReply = self.client.get(chainKey)
        return not (chainReply is None)

    def tryAppend(self, newID : str, newData : Any) -> bool:
        while True:
            x = self.next()
            if not x:
                break

        currentChainKey : str = self.config['chainPrefix']+":"+self.current['id']
        newDataKey : str = self.config['dataPrefix']+":"+newID
        newChainKey : str = self.config['chainPrefix']+":"+newID;
        luaStr : str = "local x = redis.call('GET',KEYS[1]); local y = redis.call('GET',KEYS[2]); local z = redis.call('GET',KEYS[3]); if x == '' and not y and not z then redis.call('SET',KEYS[1],ARGV[1]); redis.call('SET',KEYS[2],ARGV[2]); redis.call('SET',KEYS[3],''); return 1 else return 0 end";
        redisReply = self.client.eval(luaStr, 3, currentChainKey, newDataKey, newChainKey, newID, cbor.dumps(newData))
        succeeded : bool = (not (redisReply is None)) and (redisReply != 0)
        if succeeded:
            self.current = {
                'id' : newID
                , 'data' : newData
                , 'nextID' : ""
            }
        return succeeded

    def append(self, newID : str, newData : Any):
        while True:
            res = self.tryAppend(newID, newData);
            if res:
                break

    def saveExtraData(self, key : str, data : Any):
        dataKey : str = self.config['extraDataPrefix']+":"+key
        self.client.set(dataKey, cbor.dumps(data))
    
    def loadExtraData(self, key : str) -> Any:
        dataKey : str = self.config['extraDataPrefix']+":"+key
        dataReply = self.client.get(dataKey);
        if dataReply is None:
            return None
        return cbor.loads(dataReply)

    def close(self):
        self.client.close()

    def currentValue(self) -> RedisSharedChainItem:
        return self.current