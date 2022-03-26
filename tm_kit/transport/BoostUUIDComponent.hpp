#ifndef TM_KIT_TRANSPORT_BOOST_UUID_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_BOOST_UUID_COMPONENT_HPP_

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/functional/hash.hpp>
#include <boost/lexical_cast.hpp>

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/ProtoInterop.hpp>
#include <tm_kit/basic/NlohmannJsonInterop.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    struct BoostUUIDComponent {
        using IDType = boost::uuids::uuid;
        using IDHash = boost::hash<boost::uuids::uuid>;
        static constexpr std::size_t IDByteRepresentationSize = 16;
        static IDType new_id() {
            return boost::uuids::random_generator()();
        }
        static std::string id_to_string(IDType const &id) {
            return boost::lexical_cast<std::string>(id);
        }
        static IDType id_from_string(std::string const &s) {
            try {
                return boost::lexical_cast<IDType>(s);
            } catch (...) {
                return IDType {};
            }
        }
        static basic::ByteData id_to_bytes(IDType const &id) {
            std::array<char,16> ret;
            std::memcpy(ret.data(), &id, 16);
            return basic::ByteData {std::string {ret.data(), ret.data()+16}};
        }
        static IDType id_from_bytes(basic::ByteDataView const &bytes) {
            if (bytes.content.length() != 16) {
                return IDType {};
            }
            IDType id;
            std::memcpy(&id, bytes.content.data(), 16);
            return id;
        }
        static bool less_comparison_id(IDType const &a, IDType const &b) {
            return std::less<IDType>()(a,b);
        }
    };

} } } }

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace bytedata_utils {

    template <>
    struct RunCBORSerializer<boost::uuids::uuid, void> {
        static std::string apply(boost::uuids::uuid const &id) {
            return RunCBORSerializer<std::string>::apply(boost::lexical_cast<std::string>(id));
        }
        static std::size_t apply(boost::uuids::uuid const &id, char *output) {
            return RunCBORSerializer<std::string>::apply(boost::lexical_cast<std::string>(id), output);
        }
        static std::size_t calculateSize(boost::uuids::uuid const &id) {
            return RunCBORSerializer<std::string>::calculateSize(boost::lexical_cast<std::string>(id));
        }
    };
    template <>
    struct RunCBORDeserializer<boost::uuids::uuid, void> {
        static std::optional<std::tuple<boost::uuids::uuid,size_t>> apply(std::string_view const &s, size_t start) {
            try {
                auto idStr = RunCBORDeserializer<std::string>::apply(s, start);
                if (!idStr) {
                    return std::nullopt;
                }
                boost::uuids::uuid id = boost::lexical_cast<boost::uuids::uuid>(std::get<0>(*idStr));
                return std::tuple<boost::uuids::uuid,size_t> {std::move(id), std::get<1>(*idStr)};
            } catch (boost::bad_lexical_cast const &) {
                return std::nullopt;
            }
        }
        static std::optional<size_t> applyInPlace(boost::uuids::uuid &output, std::string_view const &s, size_t start) {
            try {
                auto idStr = RunCBORDeserializer<std::string>::apply(s, start);
                if (!idStr) {
                    return std::nullopt;
                }
                output = boost::lexical_cast<boost::uuids::uuid>(std::get<0>(*idStr));
                return std::get<1>(*idStr);
            } catch (boost::bad_lexical_cast const &) {
                return std::nullopt;
            }
        }
    };

} } } } }

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace proto_interop {
    template <>
    class ProtoEncoder<boost::uuids::uuid, void> {
    public:
        static constexpr uint64_t thisFieldNumber(uint64_t inputFieldNumber) {
            return inputFieldNumber;
        }
        static constexpr uint64_t nextFieldNumber(uint64_t inputFieldNumber) {
            return inputFieldNumber+1;
        }
        static void write(std::optional<uint64_t> fieldNumber, boost::uuids::uuid const &id, std::ostream &os, bool /*writeDefaultValue*/) {
            ProtoEncoder<std::string>::write(fieldNumber, boost::lexical_cast<std::string>(id), os, false);
        }
    };
    template <>
    struct ProtoWrappable<boost::uuids::uuid, void> {
        static constexpr bool value = true;
    };
    template <>
    class ProtoDecoder<boost::uuids::uuid, void> final : public IProtoDecoder<boost::uuids::uuid> {
    private:
        uint64_t baseFieldNumber_;
    public:
        ProtoDecoder(boost::uuids::uuid *output, uint64_t baseFieldNumber) : IProtoDecoder<boost::uuids::uuid>(output), baseFieldNumber_(baseFieldNumber) {}
        static std::vector<uint64_t> responsibleForFieldNumbers(uint64_t baseFieldNumber) {
            return {baseFieldNumber};
        }
        std::optional<std::size_t> read(boost::uuids::uuid &output, internal::FieldHeader const &fh, std::string_view const &input, std::size_t start) override final {
            std::string s;
            ProtoDecoder<std::string> subDec(&s, baseFieldNumber_);
            auto res = subDec.handle(fh, input, start);
            if (res) {
                try {
                    output = boost::lexical_cast<boost::uuids::uuid>(s);
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
    class JsonEncoder<boost::uuids::uuid, void> {
    public:
        static void write(nlohmann::json &output, std::optional<std::string> const &key, boost::uuids::uuid const &data) {
            auto &o = (key?output[*key]:output);
            o = boost::lexical_cast<std::string>(data);
        }
    };
    template <>
    struct JsonWrappable<boost::uuids::uuid, void> {
        static constexpr bool value = true;
    };
    template <>
    class JsonDecoder<boost::uuids::uuid, void> {
    public:
        static void fillFieldNameMapping(JsonFieldMapping const &mapping=JsonFieldMapping {}) {}
        static bool read(nlohmann::json const &input, std::optional<std::string> const &key, boost::uuids::uuid &data, JsonFieldMapping const &/*mapping*/=JsonFieldMapping {}) {
            auto const &i = (key?input.at(*key):input);
            if (i.is_null()) {
                data = boost::uuids::uuid {};
                return true;
            } else {
                std::string s;
                i.get_to(s);
                try {
                    data = boost::lexical_cast<boost::uuids::uuid>(s);
                    return true;
                } catch (...) {
                    data = boost::uuids::uuid {};
                    return false;
                }
            }
        }
        static bool read_simd(simdjson::dom::element const &input, std::optional<std::string> const &key, boost::uuids::uuid &data, JsonFieldMapping const &/*mapping*/=JsonFieldMapping {}) {
            try {
                if (key) {
                    if (input[*key].error() == simdjson::NO_SUCH_FIELD) {
                        data = boost::uuids::uuid {};
                        return true;
                    }
                    try {
                        data = boost::lexical_cast<boost::uuids::uuid>((std::string) input[*key]);
                        return true;
                    } catch (...) {
                        data = boost::uuids::uuid {};
                        return false;
                    }
                } else {
                    try {
                        data = boost::lexical_cast<boost::uuids::uuid>((std::string) input);
                        return true;
                    } catch (...) {
                        data = boost::uuids::uuid {};
                        return false;
                    }
                }
            } catch (simdjson::simdjson_error) {
                data = boost::uuids::uuid {};
                return false;
            }
        }
    };
    
} } } } }

#endif