#include <tm_kit/transport/ConvertChainIDStringToGroup.hpp>

#include <sodium/crypto_generichash.h>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    namespace chain_utils {
        uint32_t convertChainIDStringToGroup(std::string_view const &id, uint32_t totalGroups) {
            static_assert((crypto_generichash_BYTES % 4 == 0), "generic hash bytes must be a multiple of 4");
            if (totalGroups == 0) {
                return 0;
            }
            unsigned char hash[crypto_generichash_BYTES];
            crypto_generichash(
                hash, crypto_generichash_BYTES 
                , reinterpret_cast<const unsigned char *>(id.data())
                , id.length()
                , nullptr, 0
            );
            uint32_t ret = 0;
            if (totalGroups <= 256) {
                for (std::size_t ii=0; ii<crypto_generichash_BYTES; ++ii) {
                    ret = ret ^ ((uint32_t) hash[ii]);
                }
            } else if (totalGroups <= 65536) {
                for (std::size_t ii=0; ii<crypto_generichash_BYTES; ii+=2) {
                    uint32_t x = (uint32_t) *(reinterpret_cast<const uint16_t *>(&hash[ii]));
                    ret = ret ^ x;
                }
            } else {
                for (std::size_t ii=0; ii<crypto_generichash_BYTES; ii+=4) {
                    uint32_t x = *(reinterpret_cast<const uint32_t *>(&hash[ii]));
                    ret = ret ^ x;
                }
            }
            return (ret % totalGroups);
        }
    }
} } } }
