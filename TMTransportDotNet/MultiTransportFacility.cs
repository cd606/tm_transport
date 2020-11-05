using System;
using System.Threading.Tasks;
using Here;
using Dev.CD606.TM.Infra.RealTimeApp;
using Dev.CD606.TM.Basic;
using Dev.CD606.TM.Infra;

namespace Dev.CD606.TM.Transport
{
    public static class MultiTransportFacility<Env> where Env : ClockEnv
    {
        public static AbstractOnOrderFacility<Env,InT,OutT> CreateFacility<InT,OutT>(Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, Transport transport, ConnectionLocator locator, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
        {
            switch (transport)
            {
                case Transport.RabbitMQ:
                    return RabbitMQComponent<Env>.CreateFacility(encoder, decoder, locator, hookPair, identityAttacher);
                case Transport.Redis:
                    return RedisComponent<Env>.CreateFacility(encoder, decoder, locator, hookPair, identityAttacher);
                default:
                    throw new Exception($"Unknown transport {transport}");
            }
        }
        public static AbstractOnOrderFacility<Env,InT,OutT> CreateFacility<InT,OutT>(Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, string address, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
        {
            var parsedAddr = TransportUtils.ParseAddress(address);
            return CreateFacility(encoder, decoder, parsedAddr.Item1, parsedAddr.Item2, hookPair, identityAttacher);
        }
        class OneShotClient<InT,OutT> : IHandler<Env,KeyedData<InT,OutT>>
        {
            private TaskCompletionSource<OutT> promise;
            public OneShotClient(TaskCompletionSource<OutT> promise)
            {
                this.promise = promise;
            }
            public void handle(TimedDataWithEnvironment<Env,KeyedData<InT,OutT>> data)
            {
                this.promise.SetResult(data.timedData.value.data);
            }
        }
        public static Task<OutT> OneShot<InT,OutT>(Env env, Key<InT> input, Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, string address, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
        {
            var facility = CreateFacility<InT,OutT>(encoder, decoder, address, hookPair, identityAttacher);
            var promise = new TaskCompletionSource<OutT>();
            facility.start(env);
            facility.placeRequest(
                new TimedDataWithEnvironment<Env, Key<InT>>(
                    env 
                    , new WithTime<Key<InT>>(
                        env.now()
                        , input 
                        , true
                    )
                )
                , new OneShotClient<InT,OutT>(promise)
            );
            return promise.Task;
        }
        public static void OneShotNoReply<InT,OutT>(Env env, Key<InT> input, Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, string address, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
        {
            var facility = CreateFacility<InT,OutT>(encoder, decoder, address, hookPair, identityAttacher);
            facility.start(env);
            facility.placeRequestAndForget(
                new TimedDataWithEnvironment<Env, Key<InT>>(
                    env 
                    , new WithTime<Key<InT>>(
                        env.now()
                        , input 
                        , true
                    )
                )
            );
        }
        public class DynamicFacility<InT,OutT> : AbstractOnOrderFacility<Env,InT,OutT>
        {
            private Env env;
            private Func<InT,byte[]> encoder;
            private Func<byte[],Option<OutT>> decoder;
            private string address;
            private HookPair hookPair;
            private ClientSideIdentityAttacher identityAttacher;
            private AbstractOnOrderFacility<Env,InT,OutT> actualFacility;
            public DynamicFacility(Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
            {
                this.env = null;
                this.encoder = encoder;
                this.decoder = decoder;
                this.address = null;
                this.hookPair = hookPair;
                this.identityAttacher = identityAttacher;
                this.actualFacility = null;
            }
            private void createActualFacility()
            {
                var parsed = TransportUtils.ParseAddress(address);
                switch (parsed.Item1)
                {
                    case Transport.RabbitMQ:
                        actualFacility = RabbitMQComponent<Env>.CreateFacility(encoder, decoder, parsed.Item2, hookPair, identityAttacher);
                        if (env != null)
                        {
                            actualFacility.start(env);
                        }
                        break;
                    case Transport.Redis:
                        actualFacility = RedisComponent<Env>.CreateFacility(encoder, decoder, parsed.Item2, hookPair, identityAttacher);
                        if (env != null)
                        {
                            actualFacility.start(env);
                        }
                        break;
                    default:
                        actualFacility = null;
                        break;
                }
            }
            public void changeAddress(string addr)
            {
                lock (this)
                {
                    if (address == null || !address.Equals(addr))
                    {
                        address = addr;
                        createActualFacility();
                    }
                }
            }
            public override void start(Env env)
            {
                lock (this)
                {
                    this.env = env;
                    if (actualFacility != null)
                    {
                        actualFacility.start(env);
                    }
                }
            }
            public override void handle(TimedDataWithEnvironment<Env, Key<InT>> data)
            {
                lock (this)
                {
                    if (actualFacility != null)
                    {
                        actualFacility.handle(data);
                    }
                }
            }
        }
        public static DynamicFacility<InT,OutT> CreateDynamicFacility<InT,OutT>(Func<InT,byte[]> encoder, Func<byte[],Option<OutT>> decoder, HookPair hookPair = null, ClientSideIdentityAttacher identityAttacher = null)
        {
            return new DynamicFacility<InT,OutT>(encoder, decoder, hookPair, identityAttacher);
        }
        public static void WrapOnOrderFacility<Identity,InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,(Identity,InT),OutT> facility, Func<byte[],Option<InT>> decoder, Func<OutT,byte[]> encoder, Transport transport, ConnectionLocator locator, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
        {
            switch (transport)
            {
                case Transport.RabbitMQ:
                    RabbitMQComponent<Env>.WrapOnOrderFacility(r, facility, decoder, encoder, locator, hookPair, identityChecker);
                    break;
                case Transport.Redis:
                    RedisComponent<Env>.WrapOnOrderFacility(r, facility, decoder, encoder, locator, hookPair, identityChecker);
                    break;
                default:
                    throw new Exception($"Unknown transport {transport}");
            }
        }
        public static void WrapOnOrderFacility<Identity,InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,(Identity,InT),OutT> facility, Func<byte[],Option<InT>> decoder, Func<OutT,byte[]> encoder, string address, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
        {
            var parsedAddr = TransportUtils.ParseAddress(address);
            WrapOnOrderFacility(r, facility, decoder, encoder, parsedAddr.Item1, parsedAddr.Item2, hookPair, identityChecker);
        }
        public static void WrapOnOrderFacilityWithoutReply<Identity,InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,(Identity,InT),OutT> facility, Func<byte[],Option<InT>> decoder, Transport transport, ConnectionLocator locator, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
        {
            switch (transport)
            {
                case Transport.RabbitMQ:
                    RabbitMQComponent<Env>.WrapOnOrderFacilityWithoutReply(r, facility, decoder, locator, hookPair, identityChecker);
                    break;
                case Transport.Redis:
                    RedisComponent<Env>.WrapOnOrderFacilityWithoutReply(r, facility, decoder, locator, hookPair, identityChecker);
                    break;
                default:
                    throw new Exception($"Unknown transport {transport}");
            }
        }
        public static void WrapOnOrderFacilityWithoutReply<Identity,InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,(Identity,InT),OutT> facility, Func<byte[],Option<InT>> decoder, string address, HookPair hookPair = null, ServerSideIdentityChecker<Identity> identityChecker = null)
        {
            var parsedAddr = TransportUtils.ParseAddress(address);
            WrapOnOrderFacilityWithoutReply(r, facility, decoder, parsedAddr.Item1, parsedAddr.Item2, hookPair, identityChecker);
        }
        public static void WrapOnOrderFacility<InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,InT,OutT> facility, Func<byte[],Option<InT>> decoder, Func<OutT,byte[]> encoder, Transport transport, ConnectionLocator locator, HookPair hookPair = null)
        {
            switch (transport)
            {
                case Transport.RabbitMQ:
                    RabbitMQComponent<Env>.WrapOnOrderFacility(r, facility, decoder, encoder, locator, hookPair);
                    break;
                case Transport.Redis:
                    RedisComponent<Env>.WrapOnOrderFacility(r, facility, decoder, encoder, locator, hookPair);
                    break;
                default:
                    throw new Exception($"Unknown transport {transport}");
            }
        }
        public static void WrapOnOrderFacility<InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,InT,OutT> facility, Func<byte[],Option<InT>> decoder, Func<OutT,byte[]> encoder, string address, HookPair hookPair = null)
        {
            var parsedAddr = TransportUtils.ParseAddress(address);
            WrapOnOrderFacility(r, facility, decoder, encoder, parsedAddr.Item1, parsedAddr.Item2, hookPair);
        }
        public static void WrapOnOrderFacilityWithoutReply<InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,InT,OutT> facility, Func<byte[],Option<InT>> decoder, Transport transport, ConnectionLocator locator, HookPair hookPair = null)
        {
            switch (transport)
            {
                case Transport.RabbitMQ:
                    RabbitMQComponent<Env>.WrapOnOrderFacilityWithoutReply(r, facility, decoder, locator, hookPair);
                    break;
                case Transport.Redis:
                    RedisComponent<Env>.WrapOnOrderFacilityWithoutReply(r, facility, decoder, locator, hookPair);
                    break;
                default:
                    throw new Exception($"Unknown transport {transport}");
            }
        }
        public static void WrapOnOrderFacilityWithoutReply<InT,OutT>(Runner<Env> r, AbstractOnOrderFacility<Env,InT,OutT> facility, Func<byte[],Option<InT>> decoder, string address, HookPair hookPair = null)
        {
            var parsedAddr = TransportUtils.ParseAddress(address);
            WrapOnOrderFacilityWithoutReply(r, facility, decoder, parsedAddr.Item1, parsedAddr.Item2, hookPair);
        }
    }
}