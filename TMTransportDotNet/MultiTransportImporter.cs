using System;
using Here;
using Dev.CD606.TM.Infra.RealTimeApp;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    public static class MultiTransportImporter<Env> where Env : ClockEnv
    {
        public static AbstractImporter<Env, ByteDataWithTopic> CreateImporter(Transport transport, ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
        {
            switch(transport)
            {
                case Transport.RabbitMQ:
                    return RabbitMQComponent<Env>.CreateImporter(locator, topicSpec, hook);
                case Transport.Redis:
                    return RedisComponent<Env>.CreateImporter(locator, topicSpec, hook);
                case Transport.ZeroMQ:
                    return ZeroMQComponent<Env>.CreateImporter(locator, topicSpec, hook);
                default:
                    throw new Exception($"Unknown transport {transport}");
            }
        }
        public static AbstractImporter<Env, ByteDataWithTopic> CreateImporter(string address, string topicStr, WireToUserHook hook = null)
        {
            var addr = TransportUtils.ParseAddress(address);
            var topicSpec = TransportUtils.ParseTopic(addr.Item1, topicStr);
            return CreateImporter(addr.Item1, addr.Item2, topicSpec, hook);
        }
        public static AbstractImporter<Env, TypedDataWithTopic<T>> CreateTypedImporter<T>(Func<byte[],Option<T>> decoder, Transport transport, ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
        {
            switch(transport)
            {
                case Transport.RabbitMQ:
                    return RabbitMQComponent<Env>.CreateTypedImporter<T>(decoder, locator, topicSpec, hook);
                case Transport.Redis:
                    return RedisComponent<Env>.CreateTypedImporter<T>(decoder, locator, topicSpec, hook);
                case Transport.ZeroMQ:
                    return ZeroMQComponent<Env>.CreateTypedImporter<T>(decoder, locator, topicSpec, hook);
                default:
                    throw new Exception($"Unknown transport {transport}");
            }
        }
        public static AbstractImporter<Env, TypedDataWithTopic<T>> CreateTypedImporter<T>(Func<byte[],Option<T>> decoder, string address, string topicStr, WireToUserHook hook = null)
        {
            var addr = TransportUtils.ParseAddress(address);
            var topicSpec = TransportUtils.ParseTopic(addr.Item1, topicStr);
            return CreateTypedImporter<T>(decoder, addr.Item1, addr.Item2, topicSpec, hook);
        }
    }
}