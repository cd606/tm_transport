#ifndef TM_KIT_TRANSPORT_SIMPLE_IDENTITY_CHECKER_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_SIMPLE_IDENTITY_CHECKER_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>
#include <sstream>
#include <cstring>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class Identity, class Request>
    class ClientSideSimpleIdentityAttacherComponent : public ClientSideAbstractIdentityAttacherComponent<Identity,Request> {
    private:
        Identity identity_;
    public:
        ClientSideSimpleIdentityAttacherComponent() {}
        ClientSideSimpleIdentityAttacherComponent(Identity const &identity) 
            : identity_(identity)
        {  
        }
        ClientSideSimpleIdentityAttacherComponent(ClientSideSimpleIdentityAttacherComponent const &) = default;
        ClientSideSimpleIdentityAttacherComponent &operator=(ClientSideSimpleIdentityAttacherComponent const &) = default;
        ClientSideSimpleIdentityAttacherComponent(ClientSideSimpleIdentityAttacherComponent &&) = default;
        ClientSideSimpleIdentityAttacherComponent &operator=(ClientSideSimpleIdentityAttacherComponent &&) = default;
        ~ClientSideSimpleIdentityAttacherComponent() {}

        virtual basic::ByteData attach_identity(basic::ByteData &&d) override final {
            std::tuple<Identity const *, basic::ByteData const *> t {
                &identity_, &d
            };
            return basic::ByteData {
                basic::bytedata_utils::RunSerializer<
                    basic::CBOR<std::tuple<Identity const *, basic::ByteData const *>>
                >::apply({t})
            };
        }
        virtual std::optional<basic::ByteData> process_incoming_data(basic::ByteData &&d) override final {
            return {std::move(d)};
        }
    };

    template <class Identity, class Request>
    class ServerSideSimpleIdentityCheckerComponent : public ServerSideAbstractIdentityCheckerComponent<Identity,Request> {
    public:
        virtual std::optional<std::tuple<Identity, basic::ByteData>> check_identity(basic::ByteData &&d) override final {
            basic::CBOR<std::tuple<Identity, basic::ByteData>> ret;
            auto t = basic::bytedata_utils::RunDeserializer<
                basic::CBOR<std::tuple<Identity, basic::ByteData>>
            >::applyInPlace(ret, d.content);
            if (t) {
                return {std::move(ret.value)};
            } else {
                return std::nullopt;
            }
        }
        virtual basic::ByteData process_outgoing_data(Identity const &identity, basic::ByteData &&d) override final {
            return std::move(d);
        }
    };

} } } }

#endif