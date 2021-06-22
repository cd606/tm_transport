#ifndef TM_KIT_TRANSPORT_BCL_COMPAT_GUID_HPP_
#define TM_KIT_TRANSPORT_BCL_COMPAT_GUID_HPP_

#include <tm_kit/basic/ByteData.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace bcl_compat {
    template <class Env>
    class GuidConverter {
    public:
        template <class BclGuidProtoType>
        static typename Env::IDType read(BclGuidProtoType const &d) {
            basic::ByteData ret;
            ret.content.resize(16);
            char *p = ret.content.data();
            uint64_t lo = d.lo();
            uint64_t hi = d.hi();
            std::memcpy(p, (char *) &lo, sizeof(uint64_t));
            std::memcpy(p+sizeof(uint64_t), (char *) &hi, sizeof(uint64_t));
            std::swap(p[0],p[3]);
            std::swap(p[1],p[2]);
            std::swap(p[4],p[5]);
            std::swap(p[6],p[7]);
            return Env::id_from_bytes(basic::ByteDataView {ret.content});
        }
        template <class BclGuidProtoType>
        static void write(BclGuidProtoType &d, typename Env::IDType const &input) {
            basic::ByteData bytes = Env::id_to_bytes(input);
            auto l = bytes.content.length();
            if (l != 16) {
                bytes.content.resize(16);
            }
            char *p = bytes.content.data();
            std::swap(p[0],p[3]);
            std::swap(p[1],p[2]);
            std::swap(p[4],p[5]);
            std::swap(p[6],p[7]);
            uint64_t lo=0, hi=0;
            std::memcpy((char *) &lo, p, sizeof(uint64_t));
            std::memcpy((char *) &hi, p+sizeof(uint64_t), sizeof(uint64_t));
            d.set_lo(lo);
            d.set_hi(hi);
        }
    };

}}}}}

#endif