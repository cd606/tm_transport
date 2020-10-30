using System;
using Here;
using Dev.CD606.TM.Basic;

namespace Dev.CD606.TM.Transport
{
    public class WireToUserHook
    {
        public readonly Func<byte[],Option<byte[]>> hook;
        public WireToUserHook(Func<byte[],Option<byte[]>> hook)
        {
            this.hook = hook;
        }
    }
    public class UserToWireHook
    {
        public readonly Func<byte[],byte[]> hook;
        public UserToWireHook(Func<byte[],byte[]> hook)
        {
            this.hook = hook;
        }
    }
    public class HookPair
    {
        public readonly UserToWireHook userToWireHook;
        public readonly WireToUserHook wireToUserHook;
        public HookPair(UserToWireHook userToWireHook, WireToUserHook wireToUserHook)
        {
            this.userToWireHook = userToWireHook;
            this.wireToUserHook = wireToUserHook;
        }
    }
    public class ClientSideIdentityAttacher
    {
        public readonly Func<byte[],byte[]> identityAttacher;
        public readonly Func<byte[],Option<byte[]>> processIncomingData;
        public ClientSideIdentityAttacher(Func<byte[],byte[]> identityAttacher, Func<byte[],Option<byte[]>> processIncomingData)
        {
            this.identityAttacher = identityAttacher;
            this.processIncomingData = processIncomingData;
        }
    }
    public class ServerSideIdentityChecker<Identity>
    {
        public readonly Func<byte[],Option<(Identity,byte[])>> identityChecker;
        public readonly Func<Identity,byte[],byte[]> processOutgoingData;
        public ServerSideIdentityChecker(Func<byte[],Option<(Identity,byte[])>> identityChecker, Func<Identity,byte[],byte[]> processOutgoingData)
        {
            this.identityChecker = identityChecker;
            this.processOutgoingData = processOutgoingData;
        }
    }
}