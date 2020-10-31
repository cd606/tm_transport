using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Dev.CD606.TM.Transport
{
    public class ConnectionLocator
    {
        private string host = "";
        private int port = 0;
        private string username = "";
        private string password = "";
        private string identifier = "";
        private Dictionary<string, string> properties = new Dictionary<string, string>();

        public ConnectionLocator()
        {
        }
        public ConnectionLocator(string host, int port=0, string username="", string password="", string identifier="", Dictionary<string,string> properties=null)
        {
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.identifier = identifier;
            this.properties = (properties == null)?new Dictionary<string, string>():properties;
        }
        public ConnectionLocator(string locatorStr)
        {
            var idx = locatorStr.IndexOf('[');
            string mainPortion = "";
            string propertyPortion = "";
            if (idx < 0) {
                mainPortion = locatorStr;
                propertyPortion = "";
            } else {
                mainPortion = locatorStr.Substring(0, idx);
                propertyPortion = locatorStr.Substring(idx);
            }
            var mainParts = mainPortion.Split(':');
            Dictionary<string, string> properties = new Dictionary<string, string>();
            if (propertyPortion.Length > 2 && propertyPortion[propertyPortion.Length-1] == ']') {
                var realPropertyPortion = propertyPortion.Substring(1, propertyPortion.Length-2);
                var propertyParts = realPropertyPortion.Split(',');
                foreach (var p in propertyParts) {
                    var nameAndValue = p.Split('=');
                    if (nameAndValue.Length == 2) {
                        var name = nameAndValue[0];
                        var value = nameAndValue[1];
                        properties[name] = value;
                    }
                }
            }
            if (mainParts.Length >= 1) {
                this.host = mainParts[0];
            } else {
                this.host = "";
            }
            if (mainParts.Length >= 2 && mainParts[1] != "") {
                this.port = int.Parse(mainParts[1]);
            } else {
                this.port = 0;
            }
            if (mainParts.Length >= 3) {
                this.username = mainParts[2];
            } else {
                this.username = "";
            }
            if (mainParts.Length >= 4) {
                this.password = mainParts[3];
            } else {
                this.password = "";
            }
            if (mainParts.Length >= 5) {
                this.identifier = mainParts[4];
            } else {
                this.identifier = "";
            }
            this.properties = properties;
        }

        public string Host 
        {
            get 
            {
                return host;
            }
        }
        public int Port
        {
            get
            {
                return port;
            }
        }
        public string Username
        {
            get
            {
                return username;
            }
        }
        public string Password 
        {
            get
            {
                return password;
            }
        }
        public string Identifier 
        {
            get
            {
                return identifier;
            }
        }
        public string GetProperty(string propertyName, string defaultValue="")
        {
            if (properties.TryGetValue(propertyName, out string v))
            {
                return v;
            }
            else
            {
                return defaultValue;
            }
        }
        public override bool Equals(object o)
        {
            if (ReferenceEquals(this, o))
            {
                return true;
            }
            if (!(o is ConnectionLocator))
            {
                return false;
            }
            var l = o as ConnectionLocator;
            if (!(host.Equals(l.host) && port==l.port && username.Equals(l.username) && password.Equals(l.password) && identifier.Equals(l.identifier)))
            {
                return false;
            }
            if (properties == null)
            {
                return l.properties == null;
            }
            if (l.properties == null)
            {
                return false;
            }
            if (properties.Count != l.properties.Count)
            {
                return false;
            }
            foreach (var item in properties.Keys)
            {
                if (!l.properties.TryGetValue(item, out string v))
                {
                    return false;
                }
                if (!properties[item].Equals(v))
                {
                    return false;
                }
            }
            return true;
        }
        public override int GetHashCode()
        {
            return host.GetHashCode()^port.GetHashCode()^username.GetHashCode()^password.GetHashCode()^identifier.GetHashCode()^properties.GetHashCode();
        }
    }

    public enum Transport 
    {
        Multicast
        , RabbitMQ
        , Redis
        , ZeroMQ
        , NNG
    }
    public enum TopicMatchType 
    {
        MatchAll
        , MatchExact
        , MatchRE
    }
    public class TopicSpec
    {
        public TopicMatchType MatchType {get; set;}
        public string ExactString {get; set;}
        public Regex RegEX {get; set;}
        public bool Match(string topic)
        {
            switch (MatchType)
            {
                case TopicMatchType.MatchAll:
                    return true;
                case TopicMatchType.MatchExact:
                    return (topic.Equals(ExactString));
                case TopicMatchType.MatchRE:
                    return RegEX.IsMatch(topic);
                default:
                    return false;
            }
        }
    }

    public static class TransportUtils
    {
        public static (Transport, ConnectionLocator) ParseAddress(string address)
        {
            if (address.StartsWith("multicast://")) 
            {
                return (Transport.Multicast, new ConnectionLocator(address.Substring("multicast://".Length)));
            } 
            else if (address.StartsWith("rabbitmq://")) 
            {
                return (Transport.RabbitMQ, new ConnectionLocator(address.Substring("rabbitmq://".Length)));
            } 
            else if (address.StartsWith("redis://")) 
            {
                return (Transport.Redis, new ConnectionLocator(address.Substring("redis://".Length)));
            } 
            else if (address.StartsWith("zeromq://")) 
            {
                return (Transport.ZeroMQ, new ConnectionLocator(address.Substring("zeromq://".Length)));
            } 
            else if (address.StartsWith("nng://")) 
            {
                return (Transport.NNG, new ConnectionLocator(address.Substring("nng://".Length)));
            } 
            else 
            {
                throw new Exception($"Badly formed address {address}");
            }
        }
        public static TopicSpec ParseComplexTopic(string topic)
        {
            if (topic.Equals(""))
            {
                return new TopicSpec {MatchType = TopicMatchType.MatchAll, ExactString = "", RegEX = null};
            }
            else if (topic.Length > 3 && topic.StartsWith("r/") && topic.EndsWith("/"))
            {
                return new TopicSpec {MatchType = TopicMatchType.MatchRE, ExactString = "", RegEX = new Regex(topic.Substring(2, topic.Length-3))};
            }
            else
            {
                return new TopicSpec {MatchType = TopicMatchType.MatchExact, ExactString = topic, RegEX = null};
            }
        }
        public static TopicSpec ParseTopic(Transport transport, string topic)
        {
            switch (transport)
            {
                case Transport.Multicast:
                case Transport.ZeroMQ:
                case Transport.NNG:
                    return ParseComplexTopic(topic);
                case Transport.RabbitMQ:
                case Transport.Redis:
                    return new TopicSpec {MatchType = TopicMatchType.MatchExact, ExactString = topic, RegEX = null};
                default:
                    throw new Exception($"Unknown transport {transport}");
            }
        }
        public static string DefaultTopic(Transport transport)
        {
            switch (transport) 
            {
                case Transport.Multicast:
                case Transport.ZeroMQ:
                case Transport.NNG:
                    return "";
                case Transport.RabbitMQ:
                    return "#";
                case Transport.Redis:
                    return "*";
                default:
                    return "";
            }
        }
    }
}