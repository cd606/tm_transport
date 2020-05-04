#ifndef TM_KIT_TRANSPORT_BYTE_DATA_HOOK_HPP_
#define TM_KIT_TRANSPORT_BYTE_DATA_HOOK_HPP_

#include <functional>
#include <optional>
#include <tm_kit/basic/ByteData.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    //Please note that ByteDataHook's are passed around by value
    using ByteDataHook = std::function<std::optional<basic::ByteData>(basic::ByteData &&)>;
    //The reason we create two identical structs is for type safety
    struct UserToWireHook {
        ByteDataHook hook;
    };
    struct WireToUserHook {
        ByteDataHook hook;
    };
    struct ByteDataHookPair {
        UserToWireHook userToWire;
        WireToUserHook wireToUser;
    };
    //the order is data flow through h1 first, then h2
    inline ByteDataHook composeByteDataHook(ByteDataHook h1, ByteDataHook h2) {
        return [h1,h2](basic::ByteData &&b) -> std::optional<basic::ByteData> {
            auto x = h1(std::move(b));
            if (x) {
                return h2(std::move(*x));
            } else {
                return std::nullopt;
            }
        };
    }
    //the two composition directions are opposite
    inline ByteDataHookPair composeByteDataHookPair(ByteDataHookPair h1, ByteDataHookPair h2) {
        return {
            UserToWireHook {composeByteDataHook(h2.userToWire.hook, h1.userToWire.hook)}
            , WireToUserHook {composeByteDataHook(h1.wireToUser.hook, h2.wireToUser.hook)}
        };
    }


} } } }

#endif
