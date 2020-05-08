#ifndef TM_KIT_TRANSPORT_SIMPLE_IDENTITY_CHECKER_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_SIMPLE_IDENTITY_CHECKER_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>
#include <sstream>
#include <cstring>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <typename Identity>
    class SimpleIdentityCheckerComponent {
    public:
        using IdentityType = Identity;
        template <class T>
        using WithIdentityAttached = std::tuple<Identity, T>;
        static basic::ByteData attach_identity(WithIdentityAttached<basic::ByteData> &&d) {
            std::ostringstream oss;
            std::string prefix = basic::SerializationFunctions::serializeFunc<Identity>(
                std::get<0>(d)
            );
            uint64_t prefixLen = prefix.length();
            oss.write(reinterpret_cast<char const *>(&prefixLen), sizeof(uint64_t));
            oss.write(prefix.c_str(), prefixLen);
            oss.write(std::get<1>(d).content.c_str(), std::get<1>(d).content.length());
            return {oss.str()};
        }
        static std::optional<WithIdentityAttached<basic::ByteData>> check_identity(basic::ByteData &&d) {
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
            return WithIdentityAttached<basic::ByteData> {std::move(*identity), std::move(content)};
        }
    };

} } } }

#endif