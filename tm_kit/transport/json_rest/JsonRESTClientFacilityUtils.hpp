#ifndef TM_KIT_TRANSPORT_JSON_REST_JSON_REST_CLIENT_FACILITY_UTILS_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_JSON_REST_CLIENT_FACILITY_UTILS_HPP_

#include <array>
#include <cctype>
#include <string>
#include <iostream>
#include <sstream>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    class JsonRESTClientFacilityFactoryUtils {
    private:
        static std::array<bool, 128> createUnreservedCharactersArray() {
            std::array<bool, 128> ret;
            for (int ii=0; ii<128; ++ii) {
                char c = (char) ii;
                ret[ii] = std::isalnum(c) || (c == '-') || (c == '.') || (c == '_') || (c == '~');
            }
            return ret;
        }
    public:
        static void urlEscape(std::ostream &os, std::string const &input) {
            static std::array<bool,128> unreservedCharacters = createUnreservedCharactersArray();
            for (char c : input) {
                if (c == ' ') {
                    os << '+';
                } else if ((int) c < 0 || (int) c >= 128) {
                    os << '%' << std::setw(2) << std::setfill('0') << std::hex << std::uppercase << (unsigned) c;
                } else if (!unreservedCharacters[(int) c]) {
                    os << '%' << std::setw(2) << std::setfill('0') << std::hex << std::uppercase << (unsigned) c;
                } else {
                    os << c;
                }
            }
        }
    };

}}}}}

#endif