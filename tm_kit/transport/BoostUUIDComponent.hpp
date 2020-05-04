#ifndef TM_KIT_TRANSPORT_BOOST_UUID_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_BOOST_UUID_COMPONENT_HPP_

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/functional/hash.hpp>
#include <boost/lexical_cast.hpp>

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
            return boost::lexical_cast<IDType>(s);
        }
        static std::string id_to_bytes(IDType const &id) {
            std::array<char,16> ret;
            std::copy(id.begin(), id.end(), ret.begin());
            return std::string {ret.data(), ret.data()+16};
        }
        static bool less_comparison_id(IDType const &a, IDType const &b) {
            return std::less<IDType>()(a,b);
        }
    };

} } } }

#endif