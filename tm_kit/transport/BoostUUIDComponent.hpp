#ifndef TM_KIT_TRANSPORT_BOOST_UUID_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_BOOST_UUID_COMPONENT_HPP_

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/functional/hash.hpp>
#include <boost/lexical_cast.hpp>

#include <tm_kit/basic/ByteData.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    struct BoostUUIDComponent {
        using IDType = boost::uuids::uuid;
        using IDHash = boost::hash<boost::uuids::uuid>;
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
    };

} } } } }

#endif