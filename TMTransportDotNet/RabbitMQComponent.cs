using System;
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
            if (locator.GetProperty("ssl", "false") == "true")
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
            private ConnectionLocator locator;
            private TopicSpec topicSpec;
            public BinaryImporter(ConnectionLocator locator, TopicSpec topicSpec)
            {
                this.locator = locator;
                this.topicSpec = topicSpec;
                if (this.topicSpec.MatchType != TopicMatchType.MatchExact)
                {
                    throw new Exception($"RabbitMQ topic must be exact string");
                }
            }
            public override void start(Env env)
            {
                var factory = constructConnectionFactory(locator);
                var connection = factory.CreateConnection();
                var chan = connection.CreateModel();
                chan.ExchangeDeclare(
                    exchange : locator.Identifier
                    , type : ExchangeType.Topic
                    , durable : locator.GetProperty("durable", "false") == "true"
                    , autoDelete : locator.GetProperty("auto_delete", "false") == "true"
                );
                var queueName = chan.QueueDeclare().QueueName;
                chan.QueueBind(
                    queue : queueName
                    , exchange : locator.Identifier
                    , routingKey : topicSpec.ExactString
                );
                var consumer = new EventingBasicConsumer(chan);
                consumer.Received += (model, ea) => {
                    publish(InfraUtils.pureTimedDataWithEnvironment<Env, ByteDataWithTopic>(
                        env
                        , new ByteDataWithTopic(ea.RoutingKey, ea.Body.ToArray())
                        , false
                    ));
                };
                chan.BasicConsume(
                    queue : queueName
                    , autoAck : true
                    , consumer : consumer
                );
            }
        }
        public static AbstractImporter<Env, ByteDataWithTopic> createImporter(ConnectionLocator locator, TopicSpec topicSpec)
        {
            return new BinaryImporter(locator, topicSpec);
        }
    }
}
