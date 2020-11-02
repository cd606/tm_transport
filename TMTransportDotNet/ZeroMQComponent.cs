using System;
using System.Threading;
using Here;
using NetMQ;
using NetMQ.Sockets;
using PeterO.Cbor;
using Dev.CD606.TM.Infra;
using Dev.CD606.TM.Infra.RealTimeApp;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    public static class ZeroMQComponent<Env> where Env : ClockEnv
    {
        private static PublisherSocket getPublisher(ConnectionLocator l)
        {
            if (l.Host.Equals("inproc") || l.Host.Equals("ipc"))
            {
                throw new Exception("inproc/ipc URLs are not supported in NetMQ (pure DotNet ZeroMQ implementation)");
            }
            else
            {
                return new PublisherSocket($"@tcp://{l.Host}:{l.Port}");
            }
        }
        private static SubscriberSocket getSubscriber(ConnectionLocator l)
        {
            if (l.Host.Equals("inproc") || l.Host.Equals("ipc"))
            {
                throw new Exception("inproc/ipc URLs are not supported in NetMQ (pure DotNet ZeroMQ implementation)");
            }
            else
            {
                return new SubscriberSocket($">tcp://{l.Host}:{l.Port}");
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
            }
            public override void start(Env env)
            {
                new Thread(() => {
                    var subscriber = getSubscriber(locator);
                    subscriber.SubscribeToAnyTopic();
                    byte[] msg = null;
                    while (true)
                    {
                        if (subscriber.TryReceiveFrameBytes(
                            TimeSpan.FromSeconds(1)
                            , out msg
                        ))
                        {
                            var cborObj = CBORObject.DecodeFromBytes(msg);
                            if (cborObj.Type != CBORType.Array || cborObj.Count != 2)
                            {
                                continue;
                            }
                            var topic = cborObj[0].AsString();
                            if (topicSpec.Match(topic))
                            {
                                var b = cborObj[1].ToObject<byte[]>();
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
                                    , new ByteDataWithTopic(topic, b)
                                    , false
                                ));
                            }
                        }
                    }
                }).Start();
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
                new Thread(() => {
                    var subscriber = getSubscriber(locator);
                    subscriber.SubscribeToAnyTopic();
                    byte[] msg = null;
                    while (true)
                    {
                        if (subscriber.TryReceiveFrameBytes(
                            TimeSpan.FromSeconds(1)
                            , out msg
                        ))
                        {
                            var cborObj = CBORObject.DecodeFromBytes(msg);
                            if (cborObj.Type != CBORType.Array || cborObj.Count != 2)
                            {
                                continue;
                            }
                            var topic = cborObj[0].AsString();
                            if (topicSpec.Match(topic))
                            {
                                var b = cborObj[1].ToObject<byte[]>();
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
                                        , new TypedDataWithTopic<T>(topic, t.Value)
                                        , false
                                    ));
                                }
                            }
                        }
                    }
                }).Start();
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
            private PublisherSocket socket;
            public BinaryExporter(ConnectionLocator locator, UserToWireHook hook = null)
            {
                this.locator = locator;
                this.hook = hook;
                this.socket = null;
            }
            public void start(Env env)
            {
                socket = getPublisher(locator);
            }
            public void handle(TimedDataWithEnvironment<Env,ByteDataWithTopic> data)
            {
                lock(this)
                {
                    socket.SendFrame(
                        CBORObject.NewArray()
                            .Add(data.timedData.value.topic)
                            .Add(data.timedData.value.content)
                            .EncodeToBytes()
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
            private PublisherSocket socket;
            public TypedExporter(Func<T,byte[]> encoder, ConnectionLocator locator, UserToWireHook hook = null)
            {
                this.encoder = encoder;
                this.locator = locator;
                this.hook = hook;
                this.socket = null;
            }
            public void start(Env env)
            {
                socket = getPublisher(locator);
            }
            public void handle(TimedDataWithEnvironment<Env,TypedDataWithTopic<T>> data)
            {
                var b = encoder(data.timedData.value.content);
                if (hook != null)
                {
                    b = hook.hook(b);
                }
                lock(this)
                {
                    socket.SendFrame(
                        CBORObject.NewArray()
                            .Add(data.timedData.value.topic)
                            .Add(b)
                            .EncodeToBytes()
                    );
                }
            }
        }
        public static AbstractExporter<Env, TypedDataWithTopic<T>> CreateTypedExporter<T>(Func<T,byte[]> encoder, ConnectionLocator locator, UserToWireHook hook = null)
        {
            return new TypedExporter<T>(encoder, locator, hook);
        }
    }
}
