using System;
using System.Collections.Generic;
using Here;
using StackExchange.Redis;
using PeterO.Cbor;
using Dev.CD606.TM.Infra;
using Dev.CD606.TM.Infra.RealTimeApp;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    public static class RedisComponent<Env> where Env : ClockEnv
    {
        private class MultiplexerInfo
        {
            public ConnectionMultiplexer multiplexer = null;
            public uint count = 0;
        }
        private static Dictionary<ConnectionLocator, MultiplexerInfo> multiplexers = new Dictionary<ConnectionLocator, MultiplexerInfo>();
        private static object multiplexerLock = new object();
        private static ConnectionMultiplexer getMultiplexer(ConnectionLocator l)
        {
            var simplifiedLocator = new ConnectionLocator(host: l.Host, port : l.Port);
            lock (multiplexerLock)
            {
                if (multiplexers.TryGetValue(simplifiedLocator, out MultiplexerInfo m))
                {
                    ++m.count;
                    return m.multiplexer;
                }
                m = new MultiplexerInfo() {
                    multiplexer = ConnectionMultiplexer.Connect($"{l.Host}:{((l.Port==0)?6379:l.Port)}")
                    , count = 1
                };
                multiplexers.Add(simplifiedLocator, m);
                return m.multiplexer;
            }
        }
        private static void removeMultiplexer(ConnectionLocator locator)
        {
            var simplifiedLocator = new ConnectionLocator(host: locator.Host, port : locator.Port);
            lock (multiplexerLock)
            {
                if (multiplexers.TryGetValue(simplifiedLocator, out MultiplexerInfo m))
                {
                    --m.count;
                    if (m.count <= 0)
                    {
                        m.multiplexer.Close();
                        multiplexers.Remove(simplifiedLocator);
                    }
                }
            }
        }
        class BinaryImporter : AbstractImporter<Env, ByteDataWithTopic>, IDisposable
        {
            private WireToUserHook hook;
            private ConnectionLocator locator;
            private TopicSpec topicSpec;
            private ConnectionMultiplexer multiplexer;
            public BinaryImporter(ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
            {
                this.locator = locator;
                this.topicSpec = topicSpec;
                this.hook = hook;
                this.multiplexer = null;
                if (this.topicSpec.MatchType != TopicMatchType.MatchExact)
                {
                    throw new Exception($"RabbitMQ topic must be exact string");
                }
            }
            public override void start(Env env)
            {
                multiplexer = getMultiplexer(locator);
                multiplexer.GetSubscriber().Subscribe(
                    new RedisChannel(topicSpec.ExactString, RedisChannel.PatternMode.Pattern)
                    , (channel, message) => {
                        var b = (byte[]) message;
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
                            , new ByteDataWithTopic((string) channel, b)
                            , false
                        ));
                    }
                );
            }
            public void Dispose()
            {
                if (multiplexer != null)
                {
                    multiplexer.GetSubscriber().Unsubscribe(
                        new RedisChannel(topicSpec.ExactString, RedisChannel.PatternMode.Pattern)
                    );
                    removeMultiplexer(locator);
                }   
            }
        }
        public static AbstractImporter<Env, ByteDataWithTopic> CreateImporter(ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
        {
            return new BinaryImporter(locator, topicSpec, hook);
        }
        class TypedImporter<T> : AbstractImporter<Env, TypedDataWithTopic<T>>, IDisposable
        {
            private Func<byte[],Option<T>> decoder;
            private ConnectionLocator locator;
            private TopicSpec topicSpec;
            private WireToUserHook hook;
            private ConnectionMultiplexer multiplexer;
            public TypedImporter(Func<byte[],Option<T>> decoder, ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
            {
                this.decoder = decoder;
                this.locator = locator;
                this.topicSpec = topicSpec;
                this.hook = hook;
                this.multiplexer = null;
                if (this.topicSpec.MatchType != TopicMatchType.MatchExact)
                {
                    throw new Exception($"RabbitMQ topic must be exact string");
                }
            }
            public override void start(Env env)
            {
                multiplexer = getMultiplexer(locator);
                multiplexer.GetSubscriber().Subscribe(
                    new RedisChannel(topicSpec.ExactString, RedisChannel.PatternMode.Pattern)
                    , (channel, message) => {
                        var b = (byte[]) message;
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
                                , new TypedDataWithTopic<T>((string) channel, t.Value)
                                , false
                            ));
                        }
                    }
                );
            }
            public void Dispose()
            {
                if (multiplexer != null)
                {
                    multiplexer.GetSubscriber().Unsubscribe(
                        new RedisChannel(topicSpec.ExactString, RedisChannel.PatternMode.Pattern)
                    );
                    removeMultiplexer(locator);
                }   
            }
        }
        public static AbstractImporter<Env, TypedDataWithTopic<T>> CreateTypedImporter<T>(Func<byte[],Option<T>> decoder, ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
        {
            return new TypedImporter<T>(decoder, locator, topicSpec, hook);
        }
        class BinaryExporter : AbstractExporter<Env, ByteDataWithTopic>, IDisposable
        {
            private ConnectionLocator locator;
            private UserToWireHook hook;
            private ISubscriber subscriber;
            public BinaryExporter(ConnectionLocator locator, UserToWireHook hook = null)
            {
                this.locator = locator;
                this.hook = hook;
                this.subscriber = null;
            }
            public void start(Env env)
            {
                subscriber = getMultiplexer(locator).GetSubscriber();
            }
            public void handle(TimedDataWithEnvironment<Env,ByteDataWithTopic> data)
            {
                subscriber.Publish(
                    new RedisChannel(data.timedData.value.topic, RedisChannel.PatternMode.Literal)
                    , (hook == null?data.timedData.value.content:hook.hook(data.timedData.value.content))
                );
            }
            public void Dispose()
            {
                if (subscriber != null)
                {
                    removeMultiplexer(locator);
                }
            }
        }
        public static AbstractExporter<Env, ByteDataWithTopic> CreateExporter(ConnectionLocator locator, UserToWireHook hook = null)
        {
            return new BinaryExporter(locator, hook);
        }
        class TypedExporter<T> : AbstractExporter<Env, TypedDataWithTopic<T>>, IDisposable
        {
            private Func<T, byte[]> encoder;
            private ConnectionLocator locator;
            private UserToWireHook hook;
            private ISubscriber subscriber;
            public TypedExporter(Func<T,byte[]> encoder, ConnectionLocator locator, UserToWireHook hook = null)
            {
                this.encoder = encoder;
                this.locator = locator;
                this.hook = hook;
                this.subscriber = null;
            }
            public void start(Env env)
            {
                subscriber = getMultiplexer(locator).GetSubscriber();
            }
            public void handle(TimedDataWithEnvironment<Env,TypedDataWithTopic<T>> data)
            {
                var b = encoder(data.timedData.value.content);
                if (hook != null)
                {
                    b = hook.hook(b);
                }
                subscriber.Publish(
                    new RedisChannel(data.timedData.value.topic, RedisChannel.PatternMode.Literal)
                    , b
                );
            }
            public void Dispose()
            {
                if (subscriber != null)
                {
                    removeMultiplexer(locator);
                }
            }
        }
        public static AbstractExporter<Env, TypedDataWithTopic<T>> CreateTypedExporter<T>(Func<T,byte[]> encoder, ConnectionLocator locator, UserToWireHook hook = null)
        {
            return new TypedExporter<T>(encoder, locator, hook);
        }
        class ClientFacility<InT,OutT> : AbstractOnOrderFacility<Env,InT,OutT>, IDisposable
        {
            private Func<InT, byte[]> encoder;
            private Func<byte[], Option<OutT>> decoder;
            private ConnectionLocator locator;
            private HookPair hookPair;
            private ClientSideIdentityAttacher identityAttacher;
            private ISubscriber subscriber;
            private string myID;
            public ClientFacility(Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, ConnectionLocator locator, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
            {
                this.encoder = encoder;
                this.decoder = decoder;
                this.locator = locator;
                this.hookPair = hookPair;
                this.identityAttacher = identityAttacher;
                this.subscriber = null;
                this.myID = Guid.NewGuid().ToString();
            }
            public override void start(Env env)
            {
                subscriber = getMultiplexer(locator).GetSubscriber();
                var channel = subscriber.Subscribe(
                    new RedisChannel(myID, RedisChannel.PatternMode.Literal)
                );
                channel.OnMessage((message) => {
                    var cborDecoded = CBORObject.DecodeFromBytes((byte[]) message.Message);
                    if (cborDecoded.Type != CBORType.Array)
                    {
                        return;
                    }
                    if (cborDecoded.Count != 2)
                    {
                        return;
                    }
                    if (cborDecoded[1].Type != CBORType.Array)
                    {
                        return;
                    }
                    if (cborDecoded[1].Count != 2)
                    {
                        return;
                    }
                    var isFinal = cborDecoded[0].AsBoolean();
                    var id = cborDecoded[1][0].AsString();
                    var b = cborDecoded[1][1].ToObject<byte[]>();
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
                                id
                                , t.Value
                            )
                            , isFinal
                        ));
                    }
                });
            }
            public override void handle(TimedDataWithEnvironment<Env,Key<InT>> data)
            {
                var b = encoder(data.timedData.value.key);
                if (identityAttacher != null && identityAttacher.identityAttacher != null)
                {
                    b = identityAttacher.identityAttacher(b);
                }
                if (hookPair != null && hookPair.userToWireHook != null)
                {
                    b = hookPair.userToWireHook.hook(b);
                }
                var cborObj = CBORObject.NewArray()
                    .Add(myID)
                    .Add(CBORObject.NewArray()
                        .Add(data.timedData.value.id)
                        .Add(b));
                subscriber.Publish(
                    new RedisChannel(locator.Identifier, RedisChannel.PatternMode.Literal)
                    , cborObj.EncodeToBytes()
                );
            }
            public void Dispose()
            {
                if (subscriber != null)
                {
                    subscriber.Unsubscribe(new RedisChannel(myID, RedisChannel.PatternMode.Literal));
                    removeMultiplexer(locator);
                }
            }
        }
        public static AbstractOnOrderFacility<Env,InT,OutT> CreateFacility<InT,OutT>(Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, ConnectionLocator locator, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
        {
            return new ClientFacility<InT,OutT>(encoder, decoder, locator, hookPair, identityAttacher);
        }
        class WrapperDecoderImporter<Identity,InT> : AbstractImporter<Env,Key<(Identity,InT)>>
        {
            private ConnectionLocator locator;
            private Func<byte[],Option<InT>> decoder;
            private HookPair hookPair;
            private ServerSideIdentityChecker<Identity> identityChecker;
            private Dictionary<string, string> replyMap;
            private object mapLock;
            private ISubscriber subscriber;
            public WrapperDecoderImporter(ConnectionLocator locator, Func<byte[],Option<InT>> decoder, Dictionary<string,string> replyMap, object mapLock, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
            {
                this.locator = locator;
                this.decoder = decoder;
                this.hookPair = hookPair;
                this.identityChecker = identityChecker;
                this.replyMap = replyMap;
                this.mapLock = mapLock;
                this.subscriber = null;
            }
            public override void start(Env env)
            {
                subscriber = getMultiplexer(locator).GetSubscriber();
                var channel = subscriber.Subscribe(
                    new RedisChannel(locator.Identifier, RedisChannel.PatternMode.Literal)
                );
                channel.OnMessage((message) => {
                    var cborDecoded = CBORObject.DecodeFromBytes((byte[]) message.Message);
                    if (cborDecoded.Type != CBORType.Array)
                    {
                        return;
                    }
                    if (cborDecoded.Count != 2)
                    {
                        return;
                    }
                    var replyTo = cborDecoded[0].AsString();
                    cborDecoded = CBORObject.DecodeFromBytes(cborDecoded[1].ToObject<byte[]>());
                    if (cborDecoded.Type != CBORType.Array)
                    {
                        return;
                    }
                    if (cborDecoded.Count != 2)
                    {
                        return;
                    }
                    var id = cborDecoded[0].AsString();
                    var b = cborDecoded[1].ToObject<byte[]>();
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
                    lock (mapLock)
                    {
                        if (replyMap.ContainsKey(id))
                        {
                            return;
                        }
                        replyMap.Add(id, replyTo);
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
                });
            }
        }
        class WrapperDecoderImporterWithoutReply<Identity,InT> : AbstractImporter<Env,Key<(Identity,InT)>>
        {
            private ConnectionLocator locator;
            private Func<byte[],Option<InT>> decoder;
            private HookPair hookPair;
            private ServerSideIdentityChecker<Identity> identityChecker;
            private ISubscriber subscriber;
            public WrapperDecoderImporterWithoutReply(ConnectionLocator locator, Func<byte[],Option<InT>> decoder, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
            {
                this.locator = locator;
                this.decoder = decoder;
                this.hookPair = hookPair;
                this.identityChecker = identityChecker;
                this.subscriber = null;
            }
            public override void start(Env env)
            {
                subscriber = getMultiplexer(locator).GetSubscriber();
                var channel = subscriber.Subscribe(
                    new RedisChannel(locator.Identifier, RedisChannel.PatternMode.Literal)
                );
                channel.OnMessage((message) => {
                    var cborDecoded = CBORObject.DecodeFromBytes((byte[]) message.Message);
                    if (cborDecoded.Type != CBORType.Array)
                    {
                        return;
                    }
                    if (cborDecoded.Count != 2)
                    {
                        return;
                    }
                    var replyTo = cborDecoded[0].AsString();
                    cborDecoded = CBORObject.DecodeFromBytes(cborDecoded[1].ToObject<byte[]>());
                    if (cborDecoded.Type != CBORType.Array)
                    {
                        return;
                    }
                    if (cborDecoded.Count != 2)
                    {
                        return;
                    }
                    var id = cborDecoded[0].AsString();
                    var b = cborDecoded[1].ToObject<byte[]>();
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
                });
            }
        }
        public class WrapperEncoderExporter<Identity, InT, OutT> : AbstractExporter<Env, KeyedData<(Identity,InT),OutT>>
        {
            private ConnectionLocator locator;
            private Func<OutT,byte[]> encoder;
            private HookPair hookPair;
            private ServerSideIdentityChecker<Identity> identityChecker;
            private Dictionary<string, string> replyMap;
            private object mapLock;
            private ISubscriber subscriber;

            public WrapperEncoderExporter(ConnectionLocator locator, Func<OutT,byte[]> encoder, Dictionary<string, string> replyMap, object mapLock, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
            {
                this.locator = locator;
                this.encoder = encoder;
                this.hookPair = hookPair;
                this.identityChecker = identityChecker;
                this.replyMap = replyMap;
                this.mapLock = mapLock;
                this.subscriber = null;
            }
            public void start(Env env)
            {
                subscriber = getMultiplexer(locator).GetSubscriber();
            }
            public void handle(TimedDataWithEnvironment<Env, KeyedData<(Identity,InT),OutT>> data)
            {
                string replyTo = null;
                lock (mapLock)
                {
                    if (!replyMap.TryGetValue(data.timedData.value.key.id, out replyTo))
                    {
                        return;
                    }
                    if (data.timedData.finalFlag)
                    {
                        replyMap.Remove(data.timedData.value.key.id);
                    }
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
                var cborObj = CBORObject.NewArray()
                    .Add(data.timedData.finalFlag)
                    .Add(CBORObject.NewArray()
                        .Add(data.timedData.value.key.id)
                        .Add(b));
                subscriber.Publish(
                    new RedisChannel(replyTo, RedisChannel.PatternMode.Literal)
                    , cborObj.EncodeToBytes()
                );
            }
        }
        public static void WrapOnOrderFacility<Identity,InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,(Identity,InT),OutT> facility, Func<byte[],Option<InT>> decoder, Func<OutT,byte[]> encoder, ConnectionLocator locator, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
        {
            var dict = new Dictionary<string,string>();
            var mapLock = new object();
            var importer = new WrapperDecoderImporter<Identity,InT>(
                locator : locator
                , decoder : decoder
                , replyMap : dict
                , mapLock : mapLock
                , hookPair : hookPair
                , identityChecker : identityChecker
            );
            var exporter = new WrapperEncoderExporter<Identity,InT,OutT>(
                locator : locator
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
            var importer = new WrapperDecoderImporterWithoutReply<Identity,InT>(
                locator : locator
                , decoder : decoder
                , hookPair : hookPair
                , identityChecker : identityChecker
            );
            r.placeOrderWithFacilityAndForget(r.importItem(importer), facility);
        }
        class SimpleWrapperDecoderImporter<InT> : AbstractImporter<Env,Key<InT>>
        {
            private ConnectionLocator locator;
            private Func<byte[],Option<InT>> decoder;
            private HookPair hookPair;
            private Dictionary<string, string> replyMap;
            private object mapLock;
            private ISubscriber subscriber;
            public SimpleWrapperDecoderImporter(ConnectionLocator locator, Func<byte[],Option<InT>> decoder, Dictionary<string,string> replyMap, object mapLock, HookPair hookPair = null)
            {
                this.locator = locator;
                this.decoder = decoder;
                this.hookPair = hookPair;
                this.replyMap = replyMap;
                this.mapLock = mapLock;
                this.subscriber = null;
            }
            public override void start(Env env)
            {
                subscriber = getMultiplexer(locator).GetSubscriber();
                var channel = subscriber.Subscribe(
                    new RedisChannel(locator.Identifier, RedisChannel.PatternMode.Literal)
                );
                channel.OnMessage((message) => {
                    var cborDecoded = CBORObject.DecodeFromBytes((byte[]) message.Message);
                    if (cborDecoded.Type != CBORType.Array)
                    {
                        return;
                    }
                    if (cborDecoded.Count != 2)
                    {
                        return;
                    }
                    var replyTo = cborDecoded[0].AsString();
                    cborDecoded = CBORObject.DecodeFromBytes(cborDecoded[1].ToObject<byte[]>());
                    if (cborDecoded.Type != CBORType.Array)
                    {
                        return;
                    }
                    if (cborDecoded.Count != 2)
                    {
                        return;
                    }
                    var id = cborDecoded[0].AsString();
                    var b = cborDecoded[1].ToObject<byte[]>();
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
                    lock (mapLock)
                    {
                        if (replyMap.ContainsKey(id))
                        {
                            return;
                        }
                        replyMap.Add(id, replyTo);
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
                });
            }
        }
        class SimpleWrapperDecoderImporterWithoutReply<InT> : AbstractImporter<Env,Key<InT>>
        {
            private ConnectionLocator locator;
            private Func<byte[],Option<InT>> decoder;
            private HookPair hookPair;
            private ISubscriber subscriber;
            public SimpleWrapperDecoderImporterWithoutReply(ConnectionLocator locator, Func<byte[],Option<InT>> decoder, HookPair hookPair = null)
            {
                this.locator = locator;
                this.decoder = decoder;
                this.hookPair = hookPair;
                this.subscriber = null;
            }
            public override void start(Env env)
            {
                subscriber = getMultiplexer(locator).GetSubscriber();
                var channel = subscriber.Subscribe(
                    new RedisChannel(locator.Identifier, RedisChannel.PatternMode.Literal)
                );
                channel.OnMessage((message) => {
                    var cborDecoded = CBORObject.DecodeFromBytes((byte[]) message.Message);
                    if (cborDecoded.Type != CBORType.Array)
                    {
                        return;
                    }
                    if (cborDecoded.Count != 2)
                    {
                        return;
                    }
                    var replyTo = cborDecoded[0].AsString();
                    cborDecoded = CBORObject.DecodeFromBytes(cborDecoded[1].ToObject<byte[]>());
                    if (cborDecoded.Type != CBORType.Array)
                    {
                        return;
                    }
                    if (cborDecoded.Count != 2)
                    {
                        return;
                    }
                    var id = cborDecoded[0].AsString();
                    var b = cborDecoded[1].ToObject<byte[]>();
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
                });
            }
        }
        public class SimpleWrapperEncoderExporter<InT, OutT> : AbstractExporter<Env, KeyedData<InT,OutT>>
        {
            private ConnectionLocator locator;
            private Func<OutT,byte[]> encoder;
            private HookPair hookPair;
            private Dictionary<string, string> replyMap;
            private object mapLock;
            private ISubscriber subscriber;

            public SimpleWrapperEncoderExporter(ConnectionLocator locator, Func<OutT,byte[]> encoder, Dictionary<string, string> replyMap, object mapLock, HookPair hookPair = null)
            {
                this.locator = locator;
                this.encoder = encoder;
                this.hookPair = hookPair;
                this.replyMap = replyMap;
                this.mapLock = mapLock;
                this.subscriber = null;
            }
            public void start(Env env)
            {
                subscriber = getMultiplexer(locator).GetSubscriber();
            }
            public void handle(TimedDataWithEnvironment<Env, KeyedData<InT,OutT>> data)
            {
                string replyTo = null;
                lock (mapLock)
                {
                    if (!replyMap.TryGetValue(data.timedData.value.key.id, out replyTo))
                    {
                        return;
                    }
                    if (data.timedData.finalFlag)
                    {
                        replyMap.Remove(data.timedData.value.key.id);
                    }
                }
                var b = encoder(data.timedData.value.data);
                if (hookPair != null && hookPair.userToWireHook != null)
                {
                    b = hookPair.userToWireHook.hook(b);
                }
                var cborObj = CBORObject.NewArray()
                    .Add(data.timedData.finalFlag)
                    .Add(CBORObject.NewArray()
                        .Add(data.timedData.value.key.id)
                        .Add(b));
                subscriber.Publish(
                    new RedisChannel(replyTo, RedisChannel.PatternMode.Literal)
                    , cborObj.EncodeToBytes()
                );
            }
        }
        public static void WrapOnOrderFacility<InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,InT,OutT> facility, Func<byte[],Option<InT>> decoder, Func<OutT,byte[]> encoder, ConnectionLocator locator, HookPair hookPair = null)
        {
            var dict = new Dictionary<string,string>();
            var mapLock = new object();
            var importer = new SimpleWrapperDecoderImporter<InT>(
                locator : locator
                , decoder : decoder
                , replyMap : dict
                , mapLock : mapLock
                , hookPair : hookPair
            );
            var exporter = new SimpleWrapperEncoderExporter<InT,OutT>(
                locator : locator
                , encoder : encoder
                , replyMap : dict
                , mapLock : mapLock
                , hookPair : hookPair
            );
            r.placeOrderWithFacility(r.importItem(importer), facility, r.exporterAsSink(exporter));
        }
        public static void WrapOnOrderFacilityWithoutReply<InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,InT,OutT> facility, Func<byte[],Option<InT>> decoder, ConnectionLocator locator, HookPair hookPair = null)
        {
            var importer = new SimpleWrapperDecoderImporterWithoutReply<InT>(
                locator : locator
                , decoder : decoder
                , hookPair : hookPair
            );
            r.placeOrderWithFacilityAndForget(r.importItem(importer), facility);
        }
    }
}
