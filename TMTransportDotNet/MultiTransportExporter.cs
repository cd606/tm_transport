using System;
using Here;
using Dev.CD606.TM.Infra.RealTimeApp;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    public static class MultiTransportExporter<Env> where Env : ClockEnv
    {
        public static AbstractExporter<Env, ByteDataWithTopic> CreateExporter(Transport transport, ConnectionLocator locator, UserToWireHook hook = null)
        {
            switch(transport)
            {
                case Transport.RabbitMQ:
                    return RabbitMQComponent<Env>.CreateExporter(locator, hook);
                case Transport.Redis:
                    return RedisComponent<Env>.CreateExporter(locator, hook);
                case Transport.ZeroMQ:
                    return ZeroMQComponent<Env>.CreateExporter(locator, hook);
                default:
                    throw new Exception($"Unknown transport {transport}");
            }
        }
        public static AbstractExporter<Env, ByteDataWithTopic> CreateExporter(string address, UserToWireHook hook = null)
        {
            var addr = TransportUtils.ParseAddress(address);
            return CreateExporter(addr.Item1, addr.Item2, hook);
        }
        public static AbstractExporter<Env, TypedDataWithTopic<T>> CreateTypedExporter<T>(Func<T,byte[]> encoder, Transport transport, ConnectionLocator locator, UserToWireHook hook = null)
        {
            switch(transport)
            {
                case Transport.RabbitMQ:
                    return RabbitMQComponent<Env>.CreateTypedExporter<T>(encoder, locator, hook);
                case Transport.Redis:
                    return RedisComponent<Env>.CreateTypedExporter<T>(encoder, locator, hook);
                case Transport.ZeroMQ:
                    return ZeroMQComponent<Env>.CreateTypedExporter<T>(encoder, locator, hook);
                default:
                    throw new Exception($"Unknown transport {transport}");
            }
        }
        public static AbstractExporter<Env, TypedDataWithTopic<T>> CreateTypedExporter<T>(Func<T,byte[]> encoder, string address, UserToWireHook hook = null)
        {
            var addr = TransportUtils.ParseAddress(address);
            return CreateTypedExporter<T>(encoder, addr.Item1, addr.Item2, hook);
        }
    }
}