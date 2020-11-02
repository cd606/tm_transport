using System;
using System.Threading;
using System.Collections.Generic;
using Here;
using NetMQ;
using NetMQ.Sockets;
using PeterO.Cbor;
using Dev.CD606.TM.Infra;
using Dev.CD606.TM.Infra.RealTimeApp;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    public class Heartbeat : FromCbor<Heartbeat>, ToCbor
    {
        public string uuid_str {get; set;}
        public long timestamp {get; set;}
        public string host {get; set;}
        public int pid {get; set;}
        public string sender_description {get; set;}
        public Dictionary<string, List<string>> broadcast_channels {get; set;}
        public Dictionary<string, string> facility_channels {get; set;}
        public Dictionary<string, (string, string)> details {get; set;}
        public CBORObject asCborObject()
        {
            var ret = CBORObject.NewMap()
                .Add("uuid_str", uuid_str)
                .Add("timestamp", timestamp)
                .Add("host", host)
                .Add("pid", pid)
                .Add("sender_description", sender_description);
            var o = CBORObject.NewMap();
            foreach (var c in broadcast_channels)
            {
                var l = CBORObject.NewArray();
                foreach (var s in c.Value)
                {
                    l.Add(s);
                }
                o.Add(c.Key, l);
            }
            ret.Add("broadcast_channels", o);
            o = CBORObject.NewMap();
            foreach (var c in facility_channels)
            {
                o.Add(c.Key, c.Value);
            }
            ret.Add("facility_channels", o);
            o = CBORObject.NewMap();
            foreach (var c in details)
            {
                o.Add(c.Key, CBORObject.NewMap().Add("status", c.Value.Item1).Add("info", c.Value.Item2));
            }
            ret.Add("details", o);
            return ret;
        }
        public Option<Heartbeat> fromCborObject(CBORObject o)
        {
            try
            {
                var h = new Heartbeat();
                h.uuid_str = o["uuid_str"].AsString();
                h.timestamp = o["timestamp"].AsNumber().ToInt64Checked();
                h.host = o["host"].AsString();
                h.pid = o["pid"].AsNumber().ToInt32Checked();
                h.sender_description = o["sender_description"].AsString();
                h.broadcast_channels = new Dictionary<string, List<string>>();
                var ch = o["broadcast_channels"];
                foreach (var c in ch.Keys)
                {
                    var l = new List<string>();
                    foreach (var v in ch[c].Values)
                    {
                        l.Add(v.AsString());
                    }
                    h.broadcast_channels.Add(c.AsString(), l);
                }
                h.facility_channels = new Dictionary<string, string>();
                ch = o["facility_channels"];
                foreach (var c in ch.Keys)
                {
                    h.facility_channels.Add(c.AsString(), ch[c].AsString());
                }
                h.details = new Dictionary<string, (string, string)>();
                ch = o["details"];
                foreach (var c in ch.Keys)
                {
                    h.details.Add(c.AsString(), (ch[c]["status"].AsString(), ch[c]["info"].AsString()));
                }
                return h;
            }
            catch (Exception)
            {
                return Option.None;
            }
        }
    }

    public class Alert : FromCbor<Alert>, ToCbor
    {
        public long alertTime {get; set;}
        public string host {get; set;}
        public int pid {get; set;}
        public string sender {get; set;}
        public string level {get; set;}
        public string message {get; set;}
        public CBORObject asCborObject()
        {
            return CBORObject.NewMap()
                .Add("alertTime", alertTime)
                .Add("host", host)
                .Add("pid", pid)
                .Add("sender", sender)
                .Add("level", level)
                .Add("message", message)
                ;
        }
        public Option<Alert> fromCborObject(CBORObject o)
        {
            try
            {
                var a = new Alert();
                a.alertTime = o["alertTime"].AsNumber().ToInt64Checked();
                a.host = o["host"].AsString();
                a.pid = o["pid"].AsNumber().ToInt32Checked();
                a.sender = o["sender"].AsString();
                a.level = o["level"].AsString();
                a.message = o["message"].AsString();
                return a;
            }
            catch (Exception)
            {
                return Option.None;
            }
        }
    }
}