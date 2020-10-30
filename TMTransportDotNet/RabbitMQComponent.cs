using System;
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
        private static ConnectionFactory constructConnectionFactory(ConnectionLocator locator)
        {
            if (locator.GetProperty("ssl", "false").Equals("true"))
            {
                return new ConnectionFactory() {
                    HostName = locator.Host
                    , Port = (locator.Port==0?5672:locator.Port)
                    , UserName = locator.Username
                    , Password = locator.Password
                    , VirtualHost = locator.GetProperty("vhost", "/")
                    , Ssl = new SslOption(
                        locator.GetProperty("server_name", "")
                        , locator.GetProperty("client_cert", "")
                    )
                };
            }
            else
            {
                return new ConnectionFactory() {
                    HostName = locator.Host
                    , Port = (locator.Port==0?5672:locator.Port)
                    , UserName = locator.Username
                    , Password = locator.Password
                    , VirtualHost = locator.GetProperty("vhost", "/")
                }; 
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
                var factory = constructConnectionFactory(locator);
                var connection = factory.CreateConnection();
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
        public static AbstractImporter<Env, ByteDataWithTopic> createImporter(ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
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
                var factory = constructConnectionFactory(locator);
                var connection = factory.CreateConnection();
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
        public static AbstractImporter<Env, TypedDataWithTopic<T>> createTypedImporter<T>(Func<byte[],Option<T>> decoder, ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
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
                var factory = constructConnectionFactory(locator);
                var connection = factory.CreateConnection();
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
                channel.BasicPublish(
                    exchange: locator.Identifier
                    , routingKey: data.timedData.value.topic
                    , basicProperties: props
                    , body : (hook == null?data.timedData.value.content:hook.hook(data.timedData.value.content))
                );
            }
        }
        public static AbstractExporter<Env, ByteDataWithTopic> createExporter(ConnectionLocator locator, UserToWireHook hook = null)
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
                var factory = constructConnectionFactory(locator);
                var connection = factory.CreateConnection();
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
                channel.BasicPublish(
                    exchange: locator.Identifier
                    , routingKey: data.timedData.value.topic
                    , basicProperties: props
                    , body : b
                );
            }
        }
        public static AbstractExporter<Env, TypedDataWithTopic<T>> createTypedExporter<T>(Func<T,byte[]> encoder, ConnectionLocator locator, UserToWireHook hook = null)
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
                var factory = constructConnectionFactory(locator);
                var connection = factory.CreateConnection();
                channel = connection.CreateModel();
                replyQueue = channel.QueueDeclare().QueueName;
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) => {
                    var hasFinalFlag = ea.BasicProperties.ContentEncoding.Equals("with_final");
                    var b = ea.Body.ToArray();
                    var finalFlag = false;
                    if (hasFinalFlag && b.Length >= 1)
                    {
                        var b1 = new byte[b.Length-1];
                        Buffer.BlockCopy(b, 0, b1, 0, b.Length-1);
                        finalFlag = (b[b.Length-1] != 0);
                        b = b1;
                    }
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
                            , false
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
                props.ContentEncoding = "with_final";
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
                channel.BasicPublish(
                    exchange: ""
                    , routingKey: locator.Identifier
                    , basicProperties: props
                    , body : b
                );
            }
        }
        public static AbstractOnOrderFacility<Env,InT,OutT> createFacility<InT,OutT>(Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, ConnectionLocator locator, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
        {
            return new ClientFacility<InT,OutT>(encoder, decoder, locator, hookPair, identityAttacher);
        }
    }
}
