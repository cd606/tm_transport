using System;
using Here;
using Dev.CD606.TM.Infra.RealTimeApp;
using Dev.CD606.TM.Basic;

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