#ifndef TM_KIT_TRANSPORT_WEB_SOCKET_WEB_SOCKET_COMPONENT_EXCEPTION_HPP_
#define TM_KIT_TRANSPORT_WEB_SOCKET_WEB_SOCKET_COMPONENT_EXCEPTION_HPP_

#include <exception>
#include <stdexcept>
#include <string>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace web_socket {

    class WebSocketComponentException : public std::runtime_error {
    public:
        WebSocketComponentException(std::string const &info) : std::runtime_error(info) {}
    };

} } } } }

#endif