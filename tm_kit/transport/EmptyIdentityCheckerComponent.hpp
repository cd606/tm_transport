#ifndef TM_KIT_TRANSPORT_EMPTY_IDENTITY_CHECKER_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_EMPTY_IDENTITY_CHECKER_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <typename Identity>
    class EmptyIdentityCheckerComponent {
    public:
        using IdentityType = Identity;
        template <class T>
        using WithIdentityAttached = std::tuple<Identity, T>;
        //The reason we put in a "notUsed" parameter is to allow some
        //identity checker components to overload against different types.
        //This parameter will always be nullptr when called, only its type
        //is used.
        template <class T>
        static basic::ByteData attach_identity(basic::ByteData &&d, T *notUsed) {
            return std::move(d);
        }
        static std::optional<WithIdentityAttached<basic::ByteData>> check_identity(basic::ByteData &&d) {
            return WithIdentityAttached<basic::ByteData> {Identity {}, std::move(d)};
        }
    };

} } } }

#endif