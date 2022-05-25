#ifndef TM_KIT_TRANSPORT_BCL_COMPAT_DECIMAL_HPP_
#define TM_KIT_TRANSPORT_BCL_COMPAT_DECIMAL_HPP_

#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace bcl_compat {
    class DecimalConverter {
    public:
        template <class BclDecimalProtoType>
        static boost::multiprecision::cpp_dec_float_100 read(BclDecimalProtoType const &d) {
            uint64_t lo = d.lo();
            uint32_t hi = d.hi();
            boost::multiprecision::int128_t base = hi;
            base = base << 64;
            base = base | boost::multiprecision::int128_t(lo);
            uint32_t signScale = d.signscale();
            uint32_t scale = (signScale >> 1);
            boost::multiprecision::cpp_dec_float_100 ret(base);
            ret /= boost::multiprecision::pow(boost::multiprecision::cpp_dec_float_100(10), scale);
            if ((signScale & 1) == 0) {
                return ret;
            } else {
                return -ret;
            }
        }
        template <class BclDecimalProtoType>
        static void write(BclDecimalProtoType &d, boost::multiprecision::cpp_dec_float_100 const &input) {
            static const double maxIntRep = std::pow(2.0, 96.0);
            bool isNeg = (input < 0);
            uint32_t exp = 0;
            boost::multiprecision::cpp_dec_float_100 x = (isNeg?-input:input);
            boost::multiprecision::int128_t intRep = x.convert_to<boost::multiprecision::int128_t>();
            uint32_t expCap = (uint32_t) (
                std::log10(maxIntRep/(double) x)
            );
            if (expCap > 100) {
                expCap = 100;
            }
            for (; exp<expCap; ++exp) {
                if (x == intRep.convert_to<boost::multiprecision::cpp_dec_float_100>()) {
                    break;
                }
                x = x*10;
                intRep = x.convert_to<boost::multiprecision::int128_t>();
            }
            d.set_lo((uint64_t) (intRep & (uint64_t) 0xffffffffffffffff));
            d.set_hi((uint32_t) ((intRep >> 64) & (uint32_t) 0xffffffff));
            d.set_signscale(
                (exp << 1) | (isNeg?1:0)
            );
        }
    };
    

}}}}}

#endif