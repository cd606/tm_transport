#include "InterfaceToIP.hpp"
#include <stdexcept>

#ifndef _MSC_VER
#include <arpa/inet.h>
#include <sys/socket.h>
#include <ifaddrs.h>
#endif

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace multicast {
    
    std::string getAddressForInterface(std::string const &interface) {
#ifdef _MSC_VER
        throw std::runtime_error("getAddressForInterface is not supported under Windows");
#else
        //from https://stackoverflow.com/questions/4139405/how-can-i-get-to-know-the-ip-address-for-interfaces-in-c
        struct ifaddrs *ifap, *ifa;
        struct sockaddr_in *sa;
        char *addr;

        getifaddrs(&ifap);
        for (ifa = ifap; ifa; ifa = ifa->ifa_next) {
            if (ifa->ifa_addr && ifa->ifa_addr->sa_family==AF_INET) {
                sa = (struct sockaddr_in *) ifa->ifa_addr;
                addr = inet_ntoa(sa->sin_addr);
                if (std::string_view(ifa->ifa_name) == std::string_view(interface)) {
                    return std::string(addr);
                }
            }
        }

        freeifaddrs(ifap);

        return "";
#endif
    }

}}}}}