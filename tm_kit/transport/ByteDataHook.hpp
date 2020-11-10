#ifndef TM_KIT_TRANSPORT_BYTE_DATA_HOOK_HPP_
#define TM_KIT_TRANSPORT_BYTE_DATA_HOOK_HPP_

#include <functional>
#include <optional>
#include <tm_kit/basic/ByteData.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    struct UserToWireHook {
        std::function<basic::ByteData(basic::ByteData &&)> hook;
    };
    struct WireToUserHook {
        std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> hook;
    };
    struct ByteDataHookPair {
        std::optional<UserToWireHook> userToWire;
        std::optional<WireToUserHook> wireToUser;
    };
    //the order is data flow through h1 first, then h2
    inline UserToWireHook composeUserToWireHook(UserToWireHook h1, UserToWireHook h2) {
        return UserToWireHook { [h1,h2](basic::ByteData &&b) -> basic::ByteData {
            return h2.hook(h1.hook(std::move(b)));
        } };
    }
    inline WireToUserHook composeWireToUserHook(WireToUserHook h1, WireToUserHook h2) {
        return WireToUserHook { [h1,h2](basic::ByteDataView const &b) -> std::optional<basic::ByteData> {
            auto x = h1.hook(b);
            if (x) {
                return h2.hook(basic::byteDataView(*x));
            } else {
                return std::nullopt;
            }
        } };
    }
    inline std::optional<UserToWireHook> composeUserToWireHook(std::optional<UserToWireHook> h1, std::optional<UserToWireHook> h2) {
        if (!h1) {
            return h2;
        } 
        if (!h2) {
            return h1;
        }
        return UserToWireHook { [h1,h2](basic::ByteData &&b) -> basic::ByteData {
            return h2->hook(h1->hook(std::move(b)));
        } };
    }
    inline std::optional<WireToUserHook> composeWireToUserHook(std::optional<WireToUserHook> h1, std::optional<WireToUserHook> h2) {
        if (!h1) {
            return h2;
        } 
        if (!h2) {
            return h1;
        }
        return WireToUserHook { [h1,h2](basic::ByteDataView const &b) -> std::optional<basic::ByteData> {
            auto x = h1->hook(b);
            if (x) {
                return h2->hook(basic::byteDataView(*x));
            } else {
                return std::nullopt;
            }
        } };
    }
    
    //The order is that, when data flows out, it goes through h1 then h2, when data comes back in
    //, it goes through h2 then h1
    inline ByteDataHookPair composeByteDataHookPair(ByteDataHookPair h1, ByteDataHookPair h2) {
        return {
            composeUserToWireHook(h1.userToWire, h2.userToWire)
            , composeWireToUserHook(h2.wireToUser, h1.wireToUser)
        };
    }


} } } }

#endif
