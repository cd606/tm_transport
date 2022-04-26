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
        template <class T>
        static void toQueryString(std::ostream &os, T const &t, bool encodeConnectors=false) {
            if constexpr (basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<T>) {
                std::ostringstream oss;
                bool start = true;
                basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvOutput<T>
                    ::outputNameValuePairs(
                        t
                        , [&start,&os,encodeConnectors](std::string const &name, std::string const &value) {
                            if (!start) {
                                if (encodeConnectors) {
                                    os << "%26";
                                } else {
                                    os << '&';
                                }
                            }
                            JsonRESTClientFacilityFactoryUtils::urlEscape(os, name);
                            if (encodeConnectors) {
                                os << "%3D";
                            } else {
                                os << '=';
                            }
                            JsonRESTClientFacilityFactoryUtils::urlEscape(os, value);
                            start = false;
                        }
                    );
            }
        }
        template <class T>
        static std::string toQueryString(T const &t, bool encodeConnectors=false) {
            std::ostringstream oss;
            toQueryString(oss, t, encodeConnectors);
            return oss.str();
        }
    };

}}}}}

#endif