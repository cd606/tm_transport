#ifndef TM_KIT_TRANSPORT_SIMPLE_IDENTITY_CHECKER_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_SIMPLE_IDENTITY_CHECKER_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>
#include <sstream>
#include <cstring>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class Identity, class Request>
    class ClientSideSimpleIdentityAttacherComponent {
    private:
        std::string serializedIdentity_;
        uint64_t serializedIdentityLength_;
    public:
        ClientSideSimpleIdentityAttacherComponent() : serializedIdentity_(), serializedIdentityLength_(0) {}
        ClientSideSimpleIdentityAttacherComponent(Identity const &identity) 
            : serializedIdentity_(
                basic::SerializationFunctions::serializeFunc<Identity>(identity)
            )
            , serializedIdentityLength_(serializedIdentity_.length())
        {  
        }
        ClientSideSimpleIdentityAttacherComponent(ClientSideSimpleIdentityAttacherComponent const &) = default;
        ClientSideSimpleIdentityAttacherComponent &operator=(ClientSideSimpleIdentityAttacherComponent const &) = default;
        ClientSideSimpleIdentityAttacherComponent(ClientSideSimpleIdentityAttacherComponent &&) = default;
        ClientSideSimpleIdentityAttacherComponent &operator=(ClientSideSimpleIdentityAttacherComponent &&) = default;
        ~ClientSideSimpleIdentityAttacherComponent() {}

        basic::ByteData attach_identity(basic::ByteData &&d, Request *notUsed) {
            std::ostringstream oss;
            oss.write(reinterpret_cast<char const *>(&serializedIdentityLength_), sizeof(uint64_t));
            oss.write(serializedIdentity_.c_str(), serializedIdentityLength_);
            oss.write(d.content.c_str(), d.content.length());
            return {oss.str()};
        }
    };

    template <class Identity, class Request>
    class ServerSideSimpleIdentityCheckerComponent {
    public:
        static std::optional<std::tuple<Identity, basic::ByteData>> check_identity(basic::ByteData &&d, Request *notUsed) {
            std::size_t sizeLeft = d.content.length();
            const char *p = d.content.c_str();
            if (sizeLeft < sizeof(uint64_t)) {
                return std::nullopt;
            }
            uint64_t prefixLen;
            std::memcpy(&prefixLen, p, sizeof(uint64_t));
            sizeLeft -= sizeof(uint64_t);
            p += sizeof(uint64_t);
            if (sizeLeft < prefixLen) {
                return std::nullopt;
            }
            std::optional<Identity> identity = basic::SerializationFunctions::deserializeFunc<Identity>(
                std::string(p, p+prefixLen)
            );
            if (!identity) {
                return std::nullopt;
            }
            sizeLeft -= prefixLen;
            p += prefixLen;
            basic::ByteData content { std::string(p, p+sizeLeft) };
            return std::tuple<Identity, basic::ByteData> {std::move(*identity), std::move(content)};
        }
    };

} } } }

#endif