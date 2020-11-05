using System;
using System.Collections.Generic;
using Here;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Dev.CD606.TM.Infra;
using Dev.CD606.TM.Infra.RealTimeApp;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    public static class RabbitMQComponent<Env> where Env : ClockEnv
    {
        //Because the .NET RabbitMQ connection factory requires some different SSL
        //option fields than C++/Node API, the connection locator needs to be 
        //somewhat different
        private static Dictionary<ConnectionLocator,IConnection> connections = new Dictionary<ConnectionLocator, IConnection>();
        private static object connectionsLock = new object(); 
        private static IConnection constructConnection(ConnectionLocator locator)
        {
            var simplifiedDictionary = new Dictionary<string,string>();
            var s = locator.GetProperty("ssl", null);
            if (s != null)
            {
                simplifiedDictionary.Add("ssl", s);
            }
            s = locator.GetProperty("vhost", null);
            if (s != null)
            {
                simplifiedDictionary.Add("vhost", s);
            }
            s = locator.GetProperty("server_name", null);
            if (s != null)
            {
                simplifiedDictionary.Add("server_name", s);
            }
            s = locator.GetProperty("client_cert", null);
            if (s != null)
            {
                simplifiedDictionary.Add("client_cert", s);
            }
            var simplifiedLocator = new ConnectionLocator(
                locator.Host, locator.Port, locator.Username, locator.Password, "", simplifiedDictionary
            );
            lock (connectionsLock)
            {
                if (connections.TryGetValue(simplifiedLocator, out IConnection conn))
                {
                    return conn;
                }
                if (locator.GetProperty("ssl", "false").Equals("true"))
                {
                    conn = new ConnectionFactory() {
                        HostName = locator.Host
                        , Port = (locator.Port==0?5672:locator.Port)
                        , UserName = locator.Username
                        , Password = locator.Password
                        , VirtualHost = locator.GetProperty("vhost", "/")
                        , Ssl = new SslOption(
                            locator.GetProperty("server_name", "")
                            , locator.GetProperty("client_cert", "")
                        )
                    }.CreateConnection();
                }
                else
                {
                    conn = new ConnectionFactory() {
                        HostName = locator.Host
                        , Port = (locator.Port==0?5672:locator.Port)
                        , UserName = locator.Username
                        , Password = locator.Password
                        , VirtualHost = locator.GetProperty("vhost", "/")
                    }.CreateConnection(); 
                }
                connections.Add(simplifiedLocator, conn);
                return conn;
            }
        }
        class BinaryImporter : AbstractImporter<Env, ByteDataWithTopic>
        {
            private WireToUserHook hook;
            private ConnectionLocator locator;
            private TopicSpec topicSpec;
            public BinaryImporter(ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
            {
                this.locator = locator;
                this.topicSpec = topicSpec;
                this.hook = hook;
                if (this.topicSpec.MatchType != TopicMatchType.MatchExact)
                {
                    throw new Exception($"RabbitMQ topic must be exact string");
                }
            }
            public override void start(Env env)
            {
                var connection = constructConnection(locator);
                var channel = connection.CreateModel();
                channel.ExchangeDeclare(
                    exchange : locator.Identifier
                    , type : ExchangeType.Topic
                    , durable : locator.GetProperty("durable", "false").Equals("true")
                    , autoDelete : locator.GetProperty("auto_delete", "false").Equals("true")
                );
                var queueName = channel.QueueDeclare().QueueName;
                channel.QueueBind(
                    queue : queueName
                    , exchange : locator.Identifier
                    , routingKey : topicSpec.ExactString
                );
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => {
                    var b = ea.Body.ToArray();
                    if (hook != null)
                    {
                        var processed = hook.hook(b);
                        if (processed)
                        {
                            b = processed.Value;
                        }
                        else
                        {
                            return;
                        }
                    }
                    publish(InfraUtils.pureTimedDataWithEnvironment<Env, ByteDataWithTopic>(
                        env
                        , new ByteDataWithTopic(ea.RoutingKey, b)
                        , false
                    ));
                };
                channel.BasicConsume(
                    queue : queueName
                    , autoAck : true
                    , consumer : consumer
                );
            }
        }
        public static AbstractImporter<Env, ByteDataWithTopic> CreateImporter(ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
        {
            return new BinaryImporter(locator, topicSpec, hook);
        }
        class TypedImporter<T> : AbstractImporter<Env, TypedDataWithTopic<T>>
        {
            private Func<byte[],Option<T>> decoder;
            private ConnectionLocator locator;
            private TopicSpec topicSpec;
            private WireToUserHook hook;
            public TypedImporter(Func<byte[],Option<T>> decoder, ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
            {
                this.decoder = decoder;
                this.locator = locator;
                this.topicSpec = topicSpec;
                this.hook = hook;
                if (this.topicSpec.MatchType != TopicMatchType.MatchExact)
                {
                    throw new Exception($"RabbitMQ topic must be exact string");
                }
            }
            public override void start(Env env)
            {
                var connection = constructConnection(locator);
                var channel = connection.CreateModel();
                channel.ExchangeDeclare(
                    exchange : locator.Identifier
                    , type : ExchangeType.Topic
                    , durable : locator.GetProperty("durable", "false").Equals("true")
                    , autoDelete : locator.GetProperty("auto_delete", "false").Equals("true")
                );
                var queueName = channel.QueueDeclare().QueueName;
                channel.QueueBind(
                    queue : queueName
                    , exchange : locator.Identifier
                    , routingKey : topicSpec.ExactString
                );
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => {
                    var b = ea.Body.ToArray();
                    if (hook != null)
                    {
                        var processed = hook.hook(b);
                        if (processed)
                        {
                            b = processed.Value;
                        }
                        else
                        {
                            return;
                        }
                    }
                    var t = decoder(b);
                    if (t.HasValue)
                    {
                        publish(InfraUtils.pureTimedDataWithEnvironment<Env, TypedDataWithTopic<T>>(
                            env
                            , new TypedDataWithTopic<T>(ea.RoutingKey, t.Value)
                            , false
                        ));
                    }
                };
                channel.BasicConsume(
                    queue : queueName
                    , autoAck : true
                    , consumer : consumer
                );
            }
        }
        public static AbstractImporter<Env, TypedDataWithTopic<T>> CreateTypedImporter<T>(Func<byte[],Option<T>> decoder, ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
        {
            return new TypedImporter<T>(decoder, locator, topicSpec, hook);
        }
        class BinaryExporter : AbstractExporter<Env, ByteDataWithTopic>
        {
            private ConnectionLocator locator;
            private UserToWireHook hook;
            private IModel channel;
            public BinaryExporter(ConnectionLocator locator, UserToWireHook hook = null)
            {
                this.locator = locator;
                this.hook = hook;
                this.channel = null;
            }
            public void start(Env env)
            {
                var connection = constructConnection(locator);
                channel = connection.CreateModel();
                channel.ExchangeDeclare(
                    exchange : locator.Identifier
                    , type : ExchangeType.Topic
                    , durable : locator.GetProperty("durable", "false").Equals("true")
                    , autoDelete : locator.GetProperty("auto_delete", "false").Equals("true")
                );
            }
            public void handle(TimedDataWithEnvironment<Env,ByteDataWithTopic> data)
            {
                var props = channel.CreateBasicProperties();
                props.Expiration = "1000";
                props.Persistent = false;
                lock (this)
                {
                    channel.BasicPublish(
                        exchange: locator.Identifier
                        , routingKey: data.timedData.value.topic
                        , basicProperties: props
                        , body : (hook == null?data.timedData.value.content:hook.hook(data.timedData.value.content))
                    );
                }
            }
        }
        public static AbstractExporter<Env, ByteDataWithTopic> CreateExporter(ConnectionLocator locator, UserToWireHook hook = null)
        {
            return new BinaryExporter(locator, hook);
        }
        class TypedExporter<T> : AbstractExporter<Env, TypedDataWithTopic<T>>
        {
            private Func<T, byte[]> encoder;
            private ConnectionLocator locator;
            private UserToWireHook hook;
            private IModel channel;
            public TypedExporter(Func<T,byte[]> encoder, ConnectionLocator locator, UserToWireHook hook = null)
            {
                this.encoder = encoder;
                this.locator = locator;
                this.hook = hook;
                this.channel = null;
            }
            public void start(Env env)
            {
                var connection = constructConnection(locator);
                channel = connection.CreateModel();
                channel.ExchangeDeclare(
                    exchange : locator.Identifier
                    , type : ExchangeType.Topic
                    , durable : locator.GetProperty("durable", "false").Equals("true")
                    , autoDelete : locator.GetProperty("auto_delete", "false").Equals("true")
                );
            }
            public void handle(TimedDataWithEnvironment<Env,TypedDataWithTopic<T>> data)
            {
                var props = channel.CreateBasicProperties();
                props.Expiration = "1000";
                props.Persistent = false;
                var b = encoder(data.timedData.value.content);
                if (hook != null)
                {
                    b = hook.hook(b);
                }
                lock (this)
                {
                    channel.BasicPublish(
                        exchange: locator.Identifier
                        , routingKey: data.timedData.value.topic
                        , basicProperties: props
                        , body : b
                    );
                }
            }
        }
        public static AbstractExporter<Env, TypedDataWithTopic<T>> CreateTypedExporter<T>(Func<T,byte[]> encoder, ConnectionLocator locator, UserToWireHook hook = null)
        {
            return new TypedExporter<T>(encoder, locator, hook);
        }
        class ClientFacility<InT,OutT> : AbstractOnOrderFacility<Env,InT,OutT>
        {
            private Func<InT, byte[]> encoder;
            private Func<byte[], Option<OutT>> decoder;
            private ConnectionLocator locator;
            private HookPair hookPair;
            private ClientSideIdentityAttacher identityAttacher;
            private IModel channel;
            private string replyQueue;
            private bool persistent;
            public ClientFacility(Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, ConnectionLocator locator, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
            {
                this.encoder = encoder;
                this.decoder = decoder;
                this.locator = locator;
                this.hookPair = hookPair;
                this.identityAttacher = identityAttacher;
                this.channel = null;
                this.replyQueue = "";
                this.persistent = locator.GetProperty("persistent","false").Equals("true");
            }
            public override void start(Env env)
            {
                var connection = constructConnection(locator);
                channel = connection.CreateModel();
                replyQueue = channel.QueueDeclare().QueueName;
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => {
                    var finalFlag = 
                        (ea.BasicProperties != null && ea.BasicProperties.ContentType != null && ea.BasicProperties.ContentType.Equals("final"));
                    var b = ea.Body.ToArray();
                    if (hookPair != null && hookPair.wireToUserHook != null)
                    {
                        var processed = hookPair.wireToUserHook.hook(b);
                        if (processed)
                        {
                            b = processed.Value;
                        }
                        else
                        {
                            return;
                        }
                    }
                    if (identityAttacher != null && identityAttacher.processIncomingData != null)
                    {
                        var processed = identityAttacher.processIncomingData(b);
                        if (processed)
                        {
                            b = processed.Value;
                        }
                        else
                        {
                            return;
                        }
                    }
                    
                    var t = decoder(b);
                    if (t.HasValue)
                    {
                        publish(InfraUtils.pureTimedDataWithEnvironment<Env, Key<OutT>>(
                            env
                            , new Key<OutT>(
                                ea.BasicProperties.CorrelationId
                                , t.Value
                            )
                            , finalFlag
                        ));
                    }
                };
                channel.BasicConsume(
                    queue : replyQueue
                    , autoAck : true
                    , consumer : consumer
                );
            }
            public override void handle(TimedDataWithEnvironment<Env,Key<InT>> data)
            {
                var props = channel.CreateBasicProperties();
                props.Expiration = "5000";
                props.Persistent = persistent;
                props.ReplyTo = replyQueue;
                props.CorrelationId = data.timedData.value.id;
                var b = encoder(data.timedData.value.key);
                if (identityAttacher != null && identityAttacher.identityAttacher != null)
                {
                    b = identityAttacher.identityAttacher(b);
                }
                if (hookPair != null && hookPair.userToWireHook != null)
                {
                    b = hookPair.userToWireHook.hook(b);
                }
                lock (this)
                {
                    channel.BasicPublish(
                        exchange: ""
                        , routingKey: locator.Identifier
                        , basicProperties: props
                        , body : b
                    );
                }
            }
        }
        public static AbstractOnOrderFacility<Env,InT,OutT> CreateFacility<InT,OutT>(Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, ConnectionLocator locator, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
        {
            return new ClientFacility<InT,OutT>(encoder, decoder, locator, hookPair, identityAttacher);
        }
        class WrapperDecoderImporter<Identity,InT> : AbstractImporter<Env,Key<(Identity,InT)>>
        {
            private Func<(IModel,string)> connectionFunc;
            private Func<byte[],Option<InT>> decoder;
            private HookPair hookPair;
            private ServerSideIdentityChecker<Identity> identityChecker;
            private Dictionary<string, string> replyMap;
            private object mapLock;
            public WrapperDecoderImporter(Func<(IModel,string)> connectionFunc, Func<byte[],Option<InT>> decoder, Dictionary<string,string> replyMap, object mapLock, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
            {
                this.connectionFunc = connectionFunc;
                this.decoder = decoder;
                this.hookPair = hookPair;
                this.identityChecker = identityChecker;
                this.replyMap = replyMap;
                this.mapLock = mapLock;
            }
            public override void start(Env env)
            {
                var channelAndQueue = connectionFunc();
                var channel = channelAndQueue.Item1;
                var queue = channelAndQueue.Item2;
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => {
                    var b = ea.Body.ToArray();
                    if (hookPair != null && hookPair.wireToUserHook != null)
                    {
                        var b1 = hookPair.wireToUserHook.hook(b);
                        if (b1.HasValue)
                        {
                            b = b1.Value;
                        }
                        else
                        {
                            return;
                        }
                    }
                    Identity identity = default(Identity);
                    if (identityChecker != null && identityChecker.identityChecker != null)
                    {
                        var b2 = identityChecker.identityChecker(b);
                        if (b2.HasValue)
                        {
                            identity = b2.Value.Item1;
                            b = b2.Value.Item2;
                        }
                    }
                    var decoded = decoder(b);
                    if (decoded.HasNoValue)
                    {
                        return;
                    }
                    var id = ea.BasicProperties.CorrelationId;
                    lock (mapLock)
                    {
                        if (replyMap.ContainsKey(id))
                        {
                            return;
                        }
                        replyMap.Add(id, ea.BasicProperties.ReplyTo);
                    }
                    publish(new TimedDataWithEnvironment<Env, Key<(Identity, InT)>>(
                        env 
                        , new WithTime<Key<(Identity, InT)>>(
                            env.now()
                            , new Key<(Identity, InT)>(
                                id 
                                , (identity, decoded.Value)
                            )
                            , false
                        )
                    ));
                };
                channel.BasicConsume(
                    queue : queue
                    , autoAck : true
                    , consumer : consumer
                );
            }
        }
        class WrapperDecoderImporterWithoutReply<Identity,InT> : AbstractImporter<Env,Key<(Identity,InT)>>
        {
            private Func<(IModel,string)> connectionFunc;
            private Func<byte[],Option<InT>> decoder;
            private HookPair hookPair;
            private ServerSideIdentityChecker<Identity> identityChecker;
            public WrapperDecoderImporterWithoutReply(Func<(IModel,string)> connectionFunc, Func<byte[],Option<InT>> decoder, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
            {
                this.connectionFunc = connectionFunc;
                this.decoder = decoder;
                this.hookPair = hookPair;
                this.identityChecker = identityChecker;
            }
            public override void start(Env env)
            {
                var channelAndQueue = connectionFunc();
                var channel = channelAndQueue.Item1;
                var queue = channelAndQueue.Item2;
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => {
                    var b = ea.Body.ToArray();
                    if (hookPair != null && hookPair.wireToUserHook != null)
                    {
                        var b1 = hookPair.wireToUserHook.hook(b);
                        if (b1.HasValue)
                        {
                            b = b1.Value;
                        }
                        else
                        {
                            return;
                        }
                    }
                    Identity identity = default(Identity);
                    if (identityChecker != null && identityChecker.identityChecker != null)
                    {
                        var b2 = identityChecker.identityChecker(b);
                        if (b2.HasValue)
                        {
                            identity = b2.Value.Item1;
                            b = b2.Value.Item2;
                        }
                    }
                    var decoded = decoder(b);
                    if (decoded.HasNoValue)
                    {
                        return;
                    }
                    var id = ea.BasicProperties.CorrelationId;
                    publish(new TimedDataWithEnvironment<Env, Key<(Identity, InT)>>(
                        env 
                        , new WithTime<Key<(Identity, InT)>>(
                            env.now()
                            , new Key<(Identity, InT)>(
                                id 
                                , (identity, decoded.Value)
                            )
                            , false
                        )
                    ));
                };
                channel.BasicConsume(
                    queue : queue
                    , autoAck : true
                    , consumer : consumer
                );
            }
        }
        public class WrapperEncoderExporter<Identity, InT, OutT> : AbstractExporter<Env, KeyedData<(Identity,InT),OutT>>
        {
            private Func<IModel> connectionFunc;
            private Func<OutT,byte[]> encoder;
            private HookPair hookPair;
            private ServerSideIdentityChecker<Identity> identityChecker;
            private Dictionary<string, string> replyMap;
            private object mapLock;
            private IModel channel;

            public WrapperEncoderExporter(Func<IModel> connectionFunc, Func<OutT,byte[]> encoder, Dictionary<string, string> replyMap, object mapLock, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
            {
                this.connectionFunc = connectionFunc;
                this.encoder = encoder;
                this.hookPair = hookPair;
                this.identityChecker = identityChecker;
                this.replyMap = replyMap;
                this.mapLock = mapLock;
                this.channel = null;
            }
            public void start(Env env)
            {
                channel = connectionFunc();
            }
            public void handle(TimedDataWithEnvironment<Env, KeyedData<(Identity,InT),OutT>> data)
            {
                string replyQueue = null;
                lock (mapLock)
                {
                    if (!replyMap.TryGetValue(data.timedData.value.key.id, out replyQueue))
                    {
                        return;
                    }
                    if (data.timedData.finalFlag)
                    {
                        replyMap.Remove(data.timedData.value.key.id);
                    }
                }
                var props = channel.CreateBasicProperties();
                props.CorrelationId = data.timedData.value.key.id;
                if (data.timedData.finalFlag)
                {
                    props.ContentType = "final";
                }
                var b = encoder(data.timedData.value.data);
                if (identityChecker != null && identityChecker.processOutgoingData != null)
                {
                    b = identityChecker.processOutgoingData(data.timedData.value.key.key.Item1, b);
                }
                if (hookPair != null && hookPair.userToWireHook != null)
                {
                    b = hookPair.userToWireHook.hook(b);
                }
                lock (this)
                {
                    channel.BasicPublish(
                        exchange : ""
                        , routingKey : replyQueue
                        , basicProperties: props
                        , body : b
                    );
                }
            }
        }
        public static void WrapOnOrderFacility<Identity,InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,(Identity,InT),OutT> facility, Func<byte[],Option<InT>> decoder, Func<OutT,byte[]> encoder, ConnectionLocator locator, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
        {
            var dict = new Dictionary<string,string>();
            var mapLock = new object();
            var connection = constructConnection(locator);
            var channel = connection.CreateModel();
            var importer = new WrapperDecoderImporter<Identity,InT>(
                connectionFunc: () => {
                    var queue = channel.QueueDeclare(
                        queue : locator.Identifier
                        , durable : locator.GetProperty("durable", "false").Equals("true")
                        , exclusive : locator.GetProperty("exclusive", "false").Equals("true")
                        , autoDelete : locator.GetProperty("auto_delete", "false").Equals("true")
                    ).QueueName;
                    return (channel, queue);
                }
                , decoder : decoder
                , replyMap : dict
                , mapLock : mapLock
                , hookPair : hookPair
                , identityChecker : identityChecker
            );
            var exporter = new WrapperEncoderExporter<Identity,InT,OutT>(
                connectionFunc : () => channel
                , encoder : encoder
                , replyMap : dict
                , mapLock : mapLock
                , hookPair : hookPair
                , identityChecker : identityChecker
            );
            r.placeOrderWithFacility(r.importItem(importer), facility, r.exporterAsSink(exporter));
        }
        public static void WrapOnOrderFacilityWithoutReply<Identity,InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,(Identity,InT),OutT> facility, Func<byte[],Option<InT>> decoder, ConnectionLocator locator, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
        {
            var connection = constructConnection(locator);
            var channel = connection.CreateModel();
            var importer = new WrapperDecoderImporterWithoutReply<Identity,InT>(
                connectionFunc: () => {
                    var queue = channel.QueueDeclare(
                        queue : locator.Identifier
                        , durable : locator.GetProperty("durable", "false").Equals("true")
                        , exclusive : locator.GetProperty("exclusive", "false").Equals("true")
                        , autoDelete : locator.GetProperty("auto_delete", "false").Equals("true")
                    ).QueueName;
                    return (channel, queue);
                }
                , decoder : decoder
                , hookPair : hookPair
                , identityChecker : identityChecker
            );
            r.placeOrderWithFacilityAndForget(r.importItem(importer), facility);
        }
        class SimpleWrapperDecoderImporter<InT> : AbstractImporter<Env,Key<InT>>
        {
            private Func<(IModel,string)> connectionFunc;
            private Func<byte[],Option<InT>> decoder;
            private HookPair hookPair;
            private Dictionary<string, string> replyMap;
            private object mapLock;
            public SimpleWrapperDecoderImporter(Func<(IModel,string)> connectionFunc, Func<byte[],Option<InT>> decoder, Dictionary<string,string> replyMap, object mapLock, HookPair hookPair = null)
            {
                this.connectionFunc = connectionFunc;
                this.decoder = decoder;
                this.hookPair = hookPair;
                this.replyMap = replyMap;
                this.mapLock = mapLock;
            }
            public override void start(Env env)
            {
                var channelAndQueue = connectionFunc();
                var channel = channelAndQueue.Item1;
                var queue = channelAndQueue.Item2;
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => {
                    var b = ea.Body.ToArray();
                    if (hookPair != null && hookPair.wireToUserHook != null)
                    {
                        var b1 = hookPair.wireToUserHook.hook(b);
                        if (b1.HasValue)
                        {
                            b = b1.Value;
                        }
                        else
                        {
                            return;
                        }
                    }
                    var decoded = decoder(b);
                    if (decoded.HasNoValue)
                    {
                        return;
                    }
                    var id = ea.BasicProperties.CorrelationId;
                    lock (mapLock)
                    {
                        if (replyMap.ContainsKey(id))
                        {
                            return;
                        }
                        replyMap.Add(id, ea.BasicProperties.ReplyTo);
                    }
                    publish(new TimedDataWithEnvironment<Env, Key<InT>>(
                        env 
                        , new WithTime<Key<InT>>(
                            env.now()
                            , new Key<InT>(
                                id 
                                , decoded.Value
                            )
                            , false
                        )
                    ));
                };
                channel.BasicConsume(
                    queue : queue
                    , autoAck : true
                    , consumer : consumer
                );
            }
        }
        class SimpleWrapperDecoderImporterWithoutReply<InT> : AbstractImporter<Env,Key<InT>>
        {
            private Func<(IModel,string)> connectionFunc;
            private Func<byte[],Option<InT>> decoder;
            private HookPair hookPair;
            public SimpleWrapperDecoderImporterWithoutReply(Func<(IModel,string)> connectionFunc, Func<byte[],Option<InT>> decoder, HookPair hookPair = null)
            {
                this.connectionFunc = connectionFunc;
                this.decoder = decoder;
                this.hookPair = hookPair;
            }
            public override void start(Env env)
            {
                var channelAndQueue = connectionFunc();
                var channel = channelAndQueue.Item1;
                var queue = channelAndQueue.Item2;
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => {
                    var b = ea.Body.ToArray();
                    if (hookPair != null && hookPair.wireToUserHook != null)
                    {
                        var b1 = hookPair.wireToUserHook.hook(b);
                        if (b1.HasValue)
                        {
                            b = b1.Value;
                        }
                        else
                        {
                            return;
                        }
                    }
                    var decoded = decoder(b);
                    if (decoded.HasNoValue)
                    {
                        return;
                    }
                    var id = ea.BasicProperties.CorrelationId;
                    publish(new TimedDataWithEnvironment<Env, Key<InT>>(
                        env 
                        , new WithTime<Key<InT>>(
                            env.now()
                            , new Key<InT>(
                                id 
                                , decoded.Value
                            )
                            , false
                        )
                    ));
                };
                channel.BasicConsume(
                    queue : queue
                    , autoAck : true
                    , consumer : consumer
                );
            }
        }
        public class SimpleWrapperEncoderExporter<InT, OutT> : AbstractExporter<Env, KeyedData<InT,OutT>>
        {
            private Func<IModel> connectionFunc;
            private Func<OutT,byte[]> encoder;
            private HookPair hookPair;
            private Dictionary<string, string> replyMap;
            private object mapLock;
            private IModel channel;

            public SimpleWrapperEncoderExporter(Func<IModel> connectionFunc, Func<OutT,byte[]> encoder, Dictionary<string, string> replyMap, object mapLock, HookPair hookPair = null)
            {
                this.connectionFunc = connectionFunc;
                this.encoder = encoder;
                this.hookPair = hookPair;
                this.replyMap = replyMap;
                this.mapLock = mapLock;
                this.channel = null;
            }
            public void start(Env env)
            {
                channel = connectionFunc();
            }
            public void handle(TimedDataWithEnvironment<Env, KeyedData<InT,OutT>> data)
            {
                string replyQueue = null;
                lock (mapLock)
                {
                    if (!replyMap.TryGetValue(data.timedData.value.key.id, out replyQueue))
                    {
                        return;
                    }
                    if (data.timedData.finalFlag)
                    {
                        replyMap.Remove(data.timedData.value.key.id);
                    }
                }
                var props = channel.CreateBasicProperties();
                props.CorrelationId = data.timedData.value.key.id;
                if (data.timedData.finalFlag)
                {
                    props.ContentType = "final";
                }
                var b = encoder(data.timedData.value.data);
                if (hookPair != null && hookPair.userToWireHook != null)
                {
                    b = hookPair.userToWireHook.hook(b);
                }
                lock (this)
                {
                    channel.BasicPublish(
                        exchange : ""
                        , routingKey : replyQueue
                        , basicProperties: props
                        , body : b
                    );
                }
            }
        }
        public static void WrapOnOrderFacility<InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,InT,OutT> facility, Func<byte[],Option<InT>> decoder, Func<OutT,byte[]> encoder, ConnectionLocator locator, HookPair hookPair = null)
        {
            var dict = new Dictionary<string,string>();
            var mapLock = new object();
            var connection = constructConnection(locator);
            var channel = connection.CreateModel();
            var importer = new SimpleWrapperDecoderImporter<InT>(
                connectionFunc: () => {
                    var queue = channel.QueueDeclare(
                        queue : locator.Identifier
                        , durable : locator.GetProperty("durable", "false").Equals("true")
                        , exclusive : locator.GetProperty("exclusive", "false").Equals("true")
                        , autoDelete : locator.GetProperty("auto_delete", "false").Equals("true")
                    ).QueueName;
                    return (channel, queue);
                }
                , decoder : decoder
                , replyMap : dict
                , mapLock : mapLock
                , hookPair : hookPair
            );
            var exporter = new SimpleWrapperEncoderExporter<InT,OutT>(
                connectionFunc : () => channel
                , encoder : encoder
                , replyMap : dict
                , mapLock : mapLock
                , hookPair : hookPair
            );
            r.placeOrderWithFacility(r.importItem(importer), facility, r.exporterAsSink(exporter));
        }
        public static void WrapOnOrderFacilityWithoutReply<InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,InT,OutT> facility, Func<byte[],Option<InT>> decoder, ConnectionLocator locator, HookPair hookPair = null)
        {
            var connection = constructConnection(locator);
            var channel = connection.CreateModel();
            var importer = new SimpleWrapperDecoderImporterWithoutReply<InT>(
                connectionFunc: () => {
                    var queue = channel.QueueDeclare(
                        queue : locator.Identifier
                        , durable : locator.GetProperty("durable", "false").Equals("true")
                        , exclusive : locator.GetProperty("exclusive", "false").Equals("true")
                        , autoDelete : locator.GetProperty("auto_delete", "false").Equals("true")
                    ).QueueName;
                    return (channel, queue);
                }
                , decoder : decoder
                , hookPair : hookPair
            );
            r.placeOrderWithFacilityAndForget(r.importItem(importer), facility);
        }
    }
}
