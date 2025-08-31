#ifndef TM_KIT_TRANSPORT_CONVERT_CHAIN_ID_STRING_TO_GROUP_HPP_
#define TM_KIT_TRANSPORT_CONVERT_CHAIN_ID_STRING_TO_GROUP_HPP_

#include <string_view>
#include <stdint.h>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    namespace chain_utils {
        extern uint32_t convertChainIDStringToGroup(std::string_view const &id, uint32_t totalGroups);
    }
} } } }

#endif
