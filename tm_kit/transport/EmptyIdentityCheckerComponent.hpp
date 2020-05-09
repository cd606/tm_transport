#ifndef TM_KIT_TRANSPORT_EMPTY_IDENTITY_CHECKER_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_EMPTY_IDENTITY_CHECKER_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    //In both components, the Request * parameter is never used and only
    //there for type differentiation. This is the norm for all identity
    //checker components. 

    template <class Identity, class Request>
    class ClientSideEmptyIdentityAttacherComponent {
    public:
        static basic::ByteData attach_identity(basic::ByteData &&d, Request *notUsed) {
            return std::move(d);
        }
    };

    //The server-side component is indexed by request type AND 
    //identity type.

    template <class Identity, class Request>
    class ServerSideEmptyIdentityCheckerComponent {
    public:
        static std::optional<std::tuple<Identity, basic::ByteData>> check_identity(basic::ByteData &&d, Request *notUsed) {
            return std::tuple<Identity, basic::ByteData> {Identity {}, std::move(d)};
        }
    };

} } } }

#endif