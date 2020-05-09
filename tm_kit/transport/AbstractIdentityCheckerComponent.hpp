#ifndef TM_KIT_TRANSPORT_ABSTRACT_IDENTITY_CHECKER_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_ABSTRACT_IDENTITY_CHECKER_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    template <class Identity, class Request>
    class ClientSideAbstractIdentityAttacherComponent {
    public:
        virtual basic::ByteData attach_identity(basic::ByteData &&d) = 0;
    };

    template <class Identity, class Request>
    class ServerSideAbstractIdentityCheckerComponent {
    public:
        virtual std::optional<std::tuple<Identity, basic::ByteData>> check_identity(basic::ByteData &&d) = 0;
    };

} } } }

#endif