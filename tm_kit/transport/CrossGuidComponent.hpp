#ifndef TM_KIT_TRANSPORT_CROSS_GUID_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_CROSS_GUID_COMPONENT_HPP_

#include <crossguid/guid.hpp>

#include <tm_kit/basic/ByteData.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    struct CrossGuidComponent {
        using IDType = xg::Guid;
        using IDHash = std::hash<IDType>;
        static IDType new_id() {
            return xg::newGuid();
        }
        static std::string id_to_string(IDType const &id) {
            return (std::string) id;
        }
        static IDType id_from_string(std::string const &s) {
            return IDType {s};
        }
        static std::string id_to_bytes(IDType const &id) {
            std::array<unsigned char,16> ret = id.bytes();
            return std::string {reinterpret_cast<char const *>(ret.data()), reinterpret_cast<char const *>(ret.data()+16)};
        }
        static bool less_comparison_id(IDType const &a, IDType const &b) {
            return a.bytes() < b.bytes();
        }
    };
} } } }

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace bytedata_utils {

    template <>
    struct RunSerializer<xg::Guid, void> {
        static std::string apply(xg::Guid const &id) {
            return (std::string) id;
        }
    };
    template <>
    struct RunDeserializer<xg::Guid, void> {
        static std::optional<xg::Guid> apply(std::string const &s) {
            return xg::Guid(s);
        }
    };
    template <>
    struct RunCBORSerializer<xg::Guid, void> {
        static std::vector<std::uint8_t> apply(xg::Guid const &id) {
            return RunCBORSerializer<std::string>::apply((std::string) id);
        }
        static std::size_t apply(xg::Guid const &id, char *output) {
            return RunCBORSerializer<std::string>::apply((std::string) id, output);
        }
        static std::size_t calculateSize(xg::Guid const &id) {
            return RunCBORSerializer<std::string>::calculateSize((std::string) id);
        }
    };
    template <>
    struct RunCBORDeserializer<xg::Guid, void> {
        static std::optional<std::tuple<xg::Guid,size_t>> apply(std::string_view const &s, size_t start) {
            auto idStr = RunCBORDeserializer<std::string>::apply(s, start);
            if (!idStr) {
                return std::nullopt;
            }
            xg::Guid id {std::get<0>(*idStr)};
            return std::tuple<xg::Guid,size_t> {std::move(id), std::get<1>(*idStr)};
        }
    };

} } } } }

#endif