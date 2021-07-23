using System;
using System.Collections.Generic;
using Dev.CD606.TM.Infra.RealTimeApp;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    [CborWithFieldNames]
    public class Heartbeat
    {
        [CborWithFieldNames]
        public class DetailedStatus
        {
            public string status {get; set;}
            public string info {get; set;}
        }
        public string uuid_str {get; set;}
        public long timestamp {get; set;}
        public string host {get; set;}
        public int pid {get; set;}
        public string sender_description {get; set;}
        public Dictionary<string, List<string>> broadcast_channels {get; set;}
        public Dictionary<string, string> facility_channels {get; set;}
        public Dictionary<string, DetailedStatus> details {get; set;}
    }

    [CborWithFieldNames]
    public class Alert
    {
        public long alertTime {get; set;}
        public string host {get; set;}
        public int pid {get; set;}
        public string sender {get; set;}
        public string level {get; set;}
        public string message {get; set;}
    }

    public class HeartbeatPublisher 
    {
        private string address;
        private string topic;
        private TimeSpan frequency;
        private Heartbeat value;
        public HeartbeatPublisher(string address, string topic, string description, TimeSpan frequency)
        {
            this.address = address;
            this.topic = topic;
            this.frequency = frequency;
            this.value = new Heartbeat();
            value.sender_description = description;
            value.host = System.Net.Dns.GetHostName();
            value.pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            value.uuid_str = Guid.NewGuid().ToString();
            value.broadcast_channels = new Dictionary<string, List<string>>();
            value.facility_channels = new Dictionary<string, string>();
            value.details = new Dictionary<string, Heartbeat.DetailedStatus>();
        }
        public void RegisterFacility(string facilityName, string channel)
        {
            lock (this)
            {
                value.facility_channels[facilityName] = channel;
            }
        }
        public void RegisterBroadcast(string broadcastName, string channel)
        {
            lock (this)
            {
                List<string> channels = null;
                if (value.broadcast_channels.TryGetValue(broadcastName, out channels))
                {
                    if (!channels.Contains(channel))
                    {
                        channels.Add(channel);
                    }
                }
                else 
                {
                    value.broadcast_channels[broadcastName] = new List<string> {channel};
                }
            }
        }
        public void SetStatus(string title, Heartbeat.DetailedStatus status)
        {
            lock (this)
            {
                value.details[title] = status;
            }
        }
        public void AddToRunner<Env>(Runner<Env> runner) where Env : ClockEnv
        {
            var exporter = MultiTransportExporter<Env>.CreateTypedExporter<Heartbeat>(
                (h) => CborEncoder<Heartbeat>.Encode(h).EncodeToBytes()
                , address
            );
            var timer = ClockImporter<Env>.createRecurringClockImporter<TypedDataWithTopic<Heartbeat>>(
                DateTimeOffset.Now
                , DateTimeOffset.Now.AddHours(24)
                , (long) Math.Round(frequency.TotalMilliseconds)
                , (d) => {
                    value.timestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                    return new TypedDataWithTopic<Heartbeat>(topic, value);
                }
            );
            runner.exportItem(exporter, runner.importItem(timer));
        }
    }
}