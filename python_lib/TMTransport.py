import re
from typing import TypedDict,Callable,Optional,Tuple,List
from enum import Enum

import asyncio
import aio_pika
import cbor
import socket
import struct
import aioredis
import zmq
import zmq.asyncio

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
    def input(address : str, qouts : List[asyncio.Queue], topic : str = None, wireToUserHook : Callable[[bytes],bytes] = None) -> asyncio.Task :
        parsedAddr = TMTransportUtils.parseAddress(address)
        if not parsedAddr:
            return None
        transport, locator = parsedAddr
        if topic:
            parsedTopic = TMTransportUtils.parseTopic(transport, topic)
        else:
            parsedTopic = TMTransportUtils.parseTopic(transport, MultiTransportListener.defaultTopic(transport))
        if not parsedTopic:
            return None
        realQouts = qouts
        if wireToUserHook:
            qin = asyncio.Queue()
            TMTransportUtils.bufferTransformer(wireToUserHook, qin, qouts)
            realQouts = [qin]
        if transport == Transport.Multicast:
            return MultiTransportListener.multicastInput(locator, parsedTopic, realQouts)
        elif transport == Transport.RabbitMQ:
            return MultiTransportListener.rabbitmqInput(locator, parsedTopic, realQouts)
        elif transport == Transport.Redis:
            return MultiTransportListener.redisInput(locator, parsedTopic, realQouts)
        elif transport == Transport.ZeroMQ:
            return MultiTransportListener.zeromqInput(locator, parsedTopic, realQouts)
        else:
            return None

class MultiTransportPublisher:
    @staticmethod
    def multicastOutput(locator : ConnectionLocator, qin : asyncio.Queue) -> asyncio.Task :
        return None
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
        return None
    @staticmethod
    def zeromqOutput(locator : ConnectionLocator, qin : asyncio.Queue) -> asyncio.Task :
        return None
    @staticmethod
    def output(address : str, qin : asyncio.Queue, userToWireHook : Callable[[bytes],bytes] = None) -> asyncio.Task:
        parsedAddr = TMTransportUtils.parseAddress(address)
        if not parsedAddr:
            return None
        transport, locator = parsedAddr
        realQin = qin
        if userToWireHook:
            realQin = asyncio.Queue()
            TMTransportUtils.bufferTransformer(userToWireHook, qin, [realQin])
        if transport == Transport.Multicast:
            return MultiTransportPublisher.multicastOutput(locator, realQin)
        elif transport == Transport.RabbitMQ:
            return MultiTransportPublisher.rabbitmqOutput(locator, realQin)
        elif transport == Transport.Redis:
            return MultiTransportPublisher.redisOutput(locator, realQin)
        elif transport == Transport.ZeroMQ:
            return MultiTransportPublisher.zeromqOutput(locator, realQin)
        else:
            return None