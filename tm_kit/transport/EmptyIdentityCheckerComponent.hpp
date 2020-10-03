#ifndef TM_KIT_TRANSPORT_EMPTY_IDENTITY_CHECKER_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_EMPTY_IDENTITY_CHECKER_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    template <class Identity, class Request>
    class ClientSideEmptyIdentityAttacherComponent : public ClientSideAbstractIdentityAttacherComponent<Identity,Request> {
    public:
        virtual basic::ByteData attach_identity(basic::ByteData &&d) override final {
            return std::move(d);
        }
        virtual std::optional<basic::ByteData> process_incoming_data(basic::ByteData &&d) override final {
            return {std::move(d)};
        }
    };

    template <class Identity, class Request>
    class ServerSideEmptyIdentityCheckerComponent : public ServerSideAbstractIdentityCheckerComponent<Identity,Request> {
    public:
        virtual std::optional<std::tuple<Identity, basic::ByteData>> check_identity(basic::ByteData &&d) override final {
            return std::tuple<Identity, basic::ByteData> {Identity {}, std::move(d)};
        }
        virtual basic::ByteData process_outgoing_data(Identity const &identity, basic::ByteData &&d) override final {
            return std::move(d);
        }
    };

} } } }

#endif