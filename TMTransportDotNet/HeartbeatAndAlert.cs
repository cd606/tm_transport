using System;
using System.Collections.Generic;
using Here;
using PeterO.Cbor;
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
}