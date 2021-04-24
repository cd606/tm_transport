using System;
using System.Threading.Tasks;
using Here;
using Dev.CD606.TM.Infra;
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
                case Transport.Multicast:
                    return MulticastComponent<Env>.CreateImporter(locator, topicSpec, hook);
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
            var changedTopic = (topicStr==null || topicStr.Equals(""))?TransportUtils.DefaultTopic(addr.Item1):topicStr;
            var topicSpec = TransportUtils.ParseTopic(addr.Item1, changedTopic);
            return CreateImporter(addr.Item1, addr.Item2, topicSpec, hook);
        }
        public static AbstractImporter<Env, TypedDataWithTopic<T>> CreateTypedImporter<T>(Func<byte[],Option<T>> decoder, Transport transport, ConnectionLocator locator, TopicSpec topicSpec, WireToUserHook hook = null)
        {
            switch(transport)
            {
                case Transport.Multicast:
                    return MulticastComponent<Env>.CreateTypedImporter<T>(decoder, locator, topicSpec, hook);
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
            var changedTopic = (topicStr==null || topicStr.Equals(""))?TransportUtils.DefaultTopic(addr.Item1):topicStr;
            var topicSpec = TransportUtils.ParseTopic(addr.Item1, changedTopic);
            return CreateTypedImporter<T>(decoder, addr.Item1, addr.Item2, topicSpec, hook);
        }
        private class FirstUpdateImporterHandler : IHandler<Env, ByteDataWithTopic>
        {
            private TaskCompletionSource<ByteDataWithTopic> promise;
            private AbstractImporter<Env,ByteDataWithTopic> importer;
            public FirstUpdateImporterHandler(TaskCompletionSource<ByteDataWithTopic> promise, AbstractImporter<Env,ByteDataWithTopic> importer)
            {
                this.promise = promise;
                this.importer = importer;
            }
            public void handle(TimedDataWithEnvironment<Env,ByteDataWithTopic> data) 
            {
                promise.SetResult(data.timedData.value);
                if (importer is IDisposable)
                {
                    Task.Run(() => {
                        (importer as IDisposable).Dispose();
                    });
                }
            }
        }
        public static Task<ByteDataWithTopic> FetchFirstUpdateAndDisconnect(Env env, string address, string topicStr, WireToUserHook hook = null)
        {
            var promise = new TaskCompletionSource<ByteDataWithTopic>();
            var importer = CreateImporter(address, topicStr, hook);
            importer.addHandler(new FirstUpdateImporterHandler(promise, importer));
            importer.start(env);
            return promise.Task;
        }
        private class TypedFirstUpdateImporterHandler<T> : IHandler<Env, TypedDataWithTopic<T>>
        {
            private TaskCompletionSource<TypedDataWithTopic<T>> promise;
            private AbstractImporter<Env,TypedDataWithTopic<T>> importer;
            private Predicate<T> predicate;
            public TypedFirstUpdateImporterHandler(TaskCompletionSource<TypedDataWithTopic<T>> promise, AbstractImporter<Env,TypedDataWithTopic<T>> importer, Predicate<T> predicate=null)
            {
                this.promise = promise;
                this.importer = importer;
                this.predicate = predicate;
            }
            public void handle(TimedDataWithEnvironment<Env,TypedDataWithTopic<T>> data) 
            {
                if (predicate == null || predicate(data.timedData.value.content))
                {
                    promise.SetResult(data.timedData.value);
                    if (importer is IDisposable)
                    {
                        Task.Run(() => {
                            (importer as IDisposable).Dispose();
                        });
                    }
                }
            }
        }
        public static Task<TypedDataWithTopic<T>> FetchTypedFirstUpdateAndDisconnect<T>(Env env, Func<byte[],Option<T>> decoder, string address, string topicStr, Predicate<T> predicate=null, WireToUserHook hook = null)
        {
            var promise = new TaskCompletionSource<TypedDataWithTopic<T>>();
            var importer = CreateTypedImporter<T>(decoder, address, topicStr, hook);
            importer.addHandler(new TypedFirstUpdateImporterHandler<T>(promise, importer, predicate));
            importer.start(env);
            return promise.Task;
        }
    }
}