#ifndef TM_KIT_TRANSPORT_SRC_MULTICAST_INTERFACE_TO_IP_HPP_
#define TM_KIT_TRANSPORT_SRC_MULTICAST_INTERFACE_TO_IP_HPP_

#include <string>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace multicast {
    extern std::string getAddressForInterface(std::string const &interface);
}}}}}

#endif