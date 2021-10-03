#ifndef TM_KIT_TRANSPORT_CROSS_GUID_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_CROSS_GUID_COMPONENT_HPP_

#include <crossguid/guid.hpp>

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/ProtoInterop.hpp>
#include <tm_kit/basic/NlohmannJsonInterop.hpp>

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

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace proto_interop {
    template <>
    class ProtoEncoder<xg::Guid, void> {
    public:
        static constexpr uint64_t thisFieldNumber(uint64_t inputFieldNumber) {
            return inputFieldNumber;
        }
        static constexpr uint64_t nextFieldNumber(uint64_t inputFieldNumber) {
            return inputFieldNumber+1;
        }
        static void write(std::optional<uint64_t> fieldNumber, xg::Guid const &id, std::ostream &os, bool writeDefaultValue) {
            ProtoEncoder<std::string>::write(fieldNumber, (std::string) id, os, false);
        }
    };
    template <>
    struct ProtoWrappable<xg::Guid, void> {
        static constexpr bool value = true;
    };
    template <>
    class ProtoDecoder<xg::Guid, void> final : public IProtoDecoder<xg::Guid> {
    private:
        uint64_t baseFieldNumber_;
    public:
        ProtoDecoder(xg::Guid *output, uint64_t baseFieldNumber) : IProtoDecoder<xg::Guid>(output), baseFieldNumber_(baseFieldNumber) {}
        static std::vector<uint64_t> responsibleForFieldNumbers(uint64_t baseFieldNumber) {
            return {baseFieldNumber};
        }
        std::optional<std::size_t> read(xg::Guid &output, internal::FieldHeader const &fh, std::string_view const &input, std::size_t start) override final {
            std::string s;
            ProtoDecoder<std::string> subDec(&s, baseFieldNumber_);
            auto res = subDec.handle(fh, input, start);
            if (res) {
                try {
                    output = (xg::Guid) s;
                } catch (...) {
                    return std::nullopt;
                }
            }
            return res;
        }
    };
    
} } } } }

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace nlohmann_json_interop {
    template <>
    class JsonEncoder<xg::Guid, void> {
    public:
        static void write(nlohmann::json &output, std::optional<std::string> const &key, xg::Guid const &data) {
            auto &o = (key?output[*key]:output);
            o = (std::string) data;
        }
    };
    template <>
    struct JsonWrappable<xg::Guid, void> {
        static constexpr bool value = true;
    };
    template <>
    class JsonDecoder<xg::Guid, void> {
    public:
        static void read(nlohmann::json const &input, std::optional<std::string> const &key, xg::Guid &data, JsonFieldMapping const &mapping=JsonFieldMapping {}) {
            auto const &i = (key?input.at(*key):input);
            if (i.is_null()) {
                data = xg::Guid {};
            } else {
                std::string s;
                i.get_to(s);
                try {
                    data = (xg::Guid) s;
                } catch (...) {
                    data = xg::Guid {};
                }
            }
        }
    };
    
} } } } }

#endif