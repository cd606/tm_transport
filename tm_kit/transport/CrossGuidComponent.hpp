#ifndef TM_KIT_TRANSPORT_CROSS_GUID_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_CROSS_GUID_COMPONENT_HPP_

#include <crossguid/guid.hpp>

#include <tm_kit/basic/ByteData.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    struct CrossGuidComponent {
        using IDType = xg::Guid;
        using IDHash = std::hash<IDType>;
        static constexpr std::size_t IDByteRepresentationSize = 16;
        static IDType new_id() {
            return xg::newGuid();
        }
        static std::string id_to_string(IDType const &id) {
            return (std::string) id;
        }
        static IDType id_from_string(std::string const &s) {
            try {
                return IDType {s};
            } catch (...) {
                return IDType {};
            }
        }
        static basic::ByteData id_to_bytes(IDType const &id) {
            std::array<unsigned char,16> ret = id.bytes();
            return basic::ByteData {std::string {reinterpret_cast<char const *>(ret.data()), reinterpret_cast<char const *>(ret.data()+16)}};
        }
        static IDType id_from_bytes(basic::ByteDataView const &bytes) {
            if (bytes.content.length() != 16) {
                return IDType {};
            }
            std::array<unsigned char,16> x;
            std::memcpy(x.data(), bytes.content.data(), 16);
            return IDType {x};
        }
        static bool less_comparison_id(IDType const &a, IDType const &b) {
            return a.bytes() < b.bytes();
        }
    };
} } } }

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace bytedata_utils {
    template <>
    struct RunCBORSerializer<xg::Guid, void> {
        static std::string apply(xg::Guid const &id) {
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
        static std::optional<size_t> applyInPlace(xg::Guid &output, std::string_view const &s, size_t start) {
            auto idStr = RunCBORDeserializer<std::string>::apply(s, start);
            if (!idStr) {
                return std::nullopt;
            }
            output = xg::Guid {std::get<0>(*idStr)};
            return std::get<1>(*idStr);
        }
    };

} } } } }

#endif