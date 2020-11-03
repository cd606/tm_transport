using System;
using System.Collections.Generic;
using Here;
using PeterO.Cbor;

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
        public static ClientSideIdentityAttacher SimpleIdentityAttacher(string identity)
        {
            return new ClientSideIdentityAttacher(
                (input) => CBORObject.NewArray().Add(identity).Add(input).EncodeToBytes()
                , null
            );
        }
        public static ClientSideIdentityAttacher SignatureBasedIdentityAttacher(byte[] privateKey)
        {
            byte[] realPrivateKey;
            if (privateKey.Length == 64)
            {
                realPrivateKey = privateKey;
            }
            else if (privateKey.Length == 32)
            {
                realPrivateKey = Sodium.PublicKeyAuth.GenerateKeyPair(privateKey).PrivateKey;
            }
            else
            {
                throw new Exception("Privatey key for signature must be either 64-byte long (a fully derived private key) or 32-byte long (a secret key)");
            }
            return new ClientSideIdentityAttacher(
                (input) => CBORObject.NewMap()
                    .Add("signature", Sodium.PublicKeyAuth.SignDetached(input, realPrivateKey))
                    .Add("data", input)
                    .EncodeToBytes()
                , null
            );
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
        public static ServerSideIdentityChecker<string> SimpleIdentityChecker()
        {
            return new ServerSideIdentityChecker<string>(
                (data) => {
                    var cborObj = CBORObject.DecodeFromBytes(data);
                    if (cborObj.Type != CBORType.Array || cborObj.Count != 2)
                    {
                        return Option.None;
                    }
                    return (cborObj[0].AsString(), cborObj[1].ToObject<byte[]>());
                }
                , null
            );
        }
        public static ServerSideIdentityChecker<string> SignatureBasedIdentityChecker(IDictionary<string,byte[]> publicKeys)
        {
            return new ServerSideIdentityChecker<string>(
                (input) => {
                    var cborObj = CBORObject.DecodeFromBytes(input);
                    if (cborObj.Type != CBORType.Map)
                    {
                        return Option.None;
                    }
                    var sig = cborObj["signature"];
                    if (sig == null) 
                    {
                        return Option.None;
                    }
                    var sigBytes = sig.ToObject<byte[]>();
                    var data = cborObj["data"];
                    if (data == null)
                    {
                        return Option.None;
                    }
                    var dataBytes = data.ToObject<byte[]>();
                    foreach (var item in publicKeys)
                    {
                        if (Sodium.PublicKeyAuth.VerifyDetached(sigBytes, dataBytes, item.Value))
                        {
                            return (item.Key, dataBytes);
                        }
                    }
                    return Option.None;
                }
                , null
            );
        }
    }
}