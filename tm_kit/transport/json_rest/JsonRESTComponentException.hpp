#ifndef TM_KIT_TRANSPORT_JSON_REST_JSON_REST_COMPONENT_EXCEPTION_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_JSON_REST_COMPONENT_EXCEPTION_HPP_

#include <exception>
#include <stdexcept>
#include <string>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    class JsonRESTComponentException : public std::runtime_error {
    public:
        JsonRESTComponentException(std::string const &info) : std::runtime_error(info) {}
    };

} } } } }

#endif