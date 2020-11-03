using System;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using Here;
using PeterO.Cbor;
using Dev.CD606.TM.Infra;
using Dev.CD606.TM.Infra.RealTimeApp;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    public static class MulticastComponent<Env> where Env : ClockEnv
    {
        private static Action<byte[]> getPublisher(ConnectionLocator l)
        {
            UdpClient publisher = new UdpClient(AddressFamily.InterNetwork);
            var addr = IPAddress.Parse(l.Host);
            publisher.JoinMulticastGroup(addr);
            var endPoint = new IPEndPoint(addr, l.Port);
            return (x) => {
                publisher.Send(x, x.Length, endPoint);
            };
        }
        private static Func<byte[]> getReceiver(ConnectionLocator l)
        {
            UdpClient receiver = new UdpClient(AddressFamily.InterNetwork);
            var addr = IPAddress.Parse(l.Host);
            receiver.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            receiver.Client.Bind(new IPEndPoint(IPAddress.Any, l.Port));
            receiver.JoinMulticastGroup(addr);
            return () => {
                var endPoint = new IPEndPoint(addr, l.Port);
                return receiver.Receive(ref endPoint);
            };
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
                    var receiver = getReceiver(locator);
                    byte[] msg = null;
                    while (true)
                    {
                        msg = receiver();
                        if (msg != null && msg.Length > 0)
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
                    var receiver = getReceiver(locator);
                    byte[] msg = null;
                    while (true)
                    {
                        msg = receiver();
                        if (msg != null && msg.Length > 0)
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
            private Action<byte[]> publisher;
            public BinaryExporter(ConnectionLocator locator, UserToWireHook hook = null)
            {
                this.locator = locator;
                this.hook = hook;
                this.publisher = null;
            }
            public void start(Env env)
            {
                publisher = getPublisher(locator);
            }
            public void handle(TimedDataWithEnvironment<Env,ByteDataWithTopic> data)
            {
                lock(this)
                {
                    publisher(
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
            private Action<byte[]> publisher;
            public TypedExporter(Func<T,byte[]> encoder, ConnectionLocator locator, UserToWireHook hook = null)
            {
                this.encoder = encoder;
                this.locator = locator;
                this.hook = hook;
                this.publisher = null;
            }
            public void start(Env env)
            {
                publisher = getPublisher(locator);
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
                    publisher(
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
