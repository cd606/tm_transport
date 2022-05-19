#ifndef TM_KIT_TRANSPORT_BCL_COMPAT_DURATION_HPP_
#define TM_KIT_TRANSPORT_BCL_COMPAT_DURATION_HPP_

#include <chrono>
#include <type_traits>
#include <tm_kit/basic/ProtoInterop.hpp>
#include <tm_kit/basic/SerializationHelperMacros.hpp>
#include <tm_kit/basic/ConvertibleWithString.hpp>
#include <tm_kit/basic/PrintHelper.hpp>
#include <boost/lexical_cast.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace bcl_compat {

    TM_BASIC_CBOR_CAPABLE_ENUM_AS_STRING(
        TimeSpanScale
        , (DAYS) (HOURS) (MINUTES) (SECONDS) (MILLISECONDS) (TICKS) (MINMAX)
    );

#define BCL_COMPAT_BCL_TIME_SPAN_PROTO_FIELDS \
    ((dev::cd606::tm::basic::proto_interop::SInt64, value)) \
    ((dev::cd606::tm::transport::bcl_compat::TimeSpanScale, scale))

    TM_BASIC_CBOR_CAPABLE_STRUCT(BclTimeSpanProto, BCL_COMPAT_BCL_TIME_SPAN_PROTO_FIELDS);

    class DurationConverter {
    public:
        template <class ProtoType, class Rep, class Period>
        static void write(ProtoType &p, std::chrono::duration<Rep,Period> const &data) {
            if constexpr (std::is_same_v<ProtoType, BclTimeSpanProto>) {
                if constexpr (std::is_same_v<Period, std::ratio<1>>) {
                    p.value = static_cast<int64_t>(data.count());
                    p.scale = transport::bcl_compat::TimeSpanScale::SECONDS;
                } else if constexpr (std::is_same_v<Period, std::milli>) {
                    p.value = static_cast<int64_t>(data.count());
                    p.scale = transport::bcl_compat::TimeSpanScale::MILLISECONDS;
                } else if constexpr (std::is_same_v<Period, std::micro>) {
                    p.value = static_cast<int64_t>(data.count()*10);
                    p.scale = transport::bcl_compat::TimeSpanScale::TICKS;
                } else if constexpr (std::is_same_v<Period, std::nano>) {
                    p.value = static_cast<int64_t>(data.count()/100);
                    p.scale = transport::bcl_compat::TimeSpanScale::TICKS;
                } else if constexpr (std::is_same_v<Period, std::ratio<60>>) {
                    p.value = static_cast<int64_t>(data.count());
                    p.scale = transport::bcl_compat::TimeSpanScale::MINUTES;
                } else if constexpr (std::is_same_v<Period, std::ratio<3600>>) {
                    p.value = static_cast<int64_t>(data.count());
                    p.scale = transport::bcl_compat::TimeSpanScale::HOURS;
                } else if constexpr (std::is_same_v<Period, std::ratio<86400>>) {
                    p.value = static_cast<int64_t>(data.count());
                    p.scale = transport::bcl_compat::TimeSpanScale::DAYS;
                } else if constexpr (std::is_same_v<Period, std::ratio<604800>>) {
                    p.value = static_cast<int64_t>(data.count()*7);
                    p.scale = transport::bcl_compat::TimeSpanScale::DAYS;
                } else if constexpr (std::is_same_v<Period, std::ratio<2629746>>) {
                    p.value = static_cast<int64_t>(data.count()*2629746/86400);
                    p.scale = transport::bcl_compat::TimeSpanScale::DAYS;
                } else if constexpr (std::is_same_v<Period, std::ratio<31556952>>) {
                    p.value = static_cast<int64_t>(data.count()*31556952/86400);
                    p.scale = transport::bcl_compat::TimeSpanScale::DAYS;
                } else if constexpr (Period::num > 1) {
                    auto sec = std::chrono::duration_cast<std::chrono::seconds>(data);
                    p.value = static_cast<int64_t>(sec.count());
                    p.scale = transport::bcl_compat::TimeSpanScale::SECONDS;
                } else {
                    auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(data);
                    p.value = static_cast<int64_t>(nsec.count()/100);
                    p.scale = transport::bcl_compat::TimeSpanScale::TICKS;
                }
            } else {
                using ScaleType = std::decay_t<decltype(p.scale())>;
                if constexpr (std::is_same_v<Period, std::ratio<1>>) {
                    p.set_value(static_cast<int64_t>(data.count()));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::SECONDS)));
                } else if constexpr (std::is_same_v<Period, std::milli>) {
                    p.set_value(static_cast<int64_t>(data.count()));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::MILLISECONDS)));
                } else if constexpr (std::is_same_v<Period, std::micro>) {
                    p.set_value(static_cast<int64_t>(data.count()*10));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::TICKS)));
                } else if constexpr (std::is_same_v<Period, std::nano>) {
                    p.set_value(static_cast<int64_t>(data.count()/100));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::TICKS)));
                } else if constexpr (std::is_same_v<Period, std::ratio<60>>) {
                    p.set_value(static_cast<int64_t>(data.count()));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::MINUTES)));
                } else if constexpr (std::is_same_v<Period, std::ratio<3600>>) {
                    p.set_value(static_cast<int64_t>(data.count()));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::HOURS)));
                } else if constexpr (std::is_same_v<Period, std::ratio<86400>>) {
                    p.set_value(static_cast<int64_t>(data.count()));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::DAYS)));
                } else if constexpr (std::is_same_v<Period, std::ratio<604800>>) {
                    p.set_value(static_cast<int64_t>(data.count()*7));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::DAYS)));
                } else if constexpr (std::is_same_v<Period, std::ratio<2629746>>) {
                    p.set_value(static_cast<int64_t>(data.count()*2629746/86400));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::DAYS)));
                } else if constexpr (std::is_same_v<Period, std::ratio<31556952>>) {
                    p.set_value(static_cast<int64_t>(data.count()*31556952/86400));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::DAYS)));
                } else if constexpr (Period::num > 1) {
                    auto sec = std::chrono::duration_cast<std::chrono::seconds>(data);
                    p.set_value(static_cast<int64_t>(sec.count()));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::SECONDS)));
                } else {
                    auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(data);
                    p.set_value(static_cast<int64_t>(nsec.count()/100));
                    p.set_scale(static_cast<ScaleType>(static_cast<int32_t>(transport::bcl_compat::TimeSpanScale::TICKS)));
                }
            }
        }
        template <class ProtoType, class Rep, class Period>
        static bool read(std::chrono::duration<Rep,Period> &output, ProtoType const &p) {
            if constexpr (std::is_same_v<ProtoType, BclTimeSpanProto>) {
                switch (p.scale) {
                case transport::bcl_compat::TimeSpanScale::DAYS:
#if __cplusplus > 201703L
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::days(p.value)
                    );
#else
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::hours(p.value*24)
                    );
#endif
                    break;
                case transport::bcl_compat::TimeSpanScale::HOURS:
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::hours(p.value)
                    );
                    break;
                case transport::bcl_compat::TimeSpanScale::MINUTES:
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::minutes(p.value)
                    );
                    break;
                case transport::bcl_compat::TimeSpanScale::SECONDS:
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::seconds(p.value)
                    );
                    break;
                case transport::bcl_compat::TimeSpanScale::MILLISECONDS:
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::milliseconds(p.value)
                    );
                    break;
                case transport::bcl_compat::TimeSpanScale::TICKS:
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::nanoseconds(p.value*100)
                    );
                    break;
                default:
                    return false;
                    break;
                }
                return true;
            } else {
                switch (static_cast<transport::bcl_compat::TimeSpanScale>(static_cast<int32_t>(p.scale()))) {
                case transport::bcl_compat::TimeSpanScale::DAYS:
    #if __cplusplus > 201703L
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::days(p.value())
                    );
    #else
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::hours(p.value()*24)
                    );
    #endif
                    break;
                case transport::bcl_compat::TimeSpanScale::HOURS:
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::hours(p.value())
                    );
                    break;
                case transport::bcl_compat::TimeSpanScale::MINUTES:
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::minutes(p.value())
                    );
                    break;
                case transport::bcl_compat::TimeSpanScale::SECONDS:
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::seconds(p.value())
                    );
                    break;
                case transport::bcl_compat::TimeSpanScale::MILLISECONDS:
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::milliseconds(p.value())
                    );
                    break;
                case transport::bcl_compat::TimeSpanScale::TICKS:
                    output = std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                        std::chrono::nanoseconds(p.value()*100)
                    );
                    break;
                default:
                    return false;
                    break;
                }
                return true;
            }
        }
    };

} } } } }

TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::bcl_compat::BclTimeSpanProto, BCL_COMPAT_BCL_TIME_SPAN_PROTO_FIELDS);

#undef BCL_COMPAT_BCL_TIME_SPAN_PROTO_FIELDS

namespace dev { namespace cd606 { namespace tm { namespace basic { 
    template <class Rep, class Period>
    class ConvertibleWithString<std::chrono::duration<Rep,Period>> {
    public:
        static constexpr bool value = true; 
        static std::string toString(std::chrono::duration<Rep,Period> const &data) {
            std::ostringstream oss;
            if constexpr (std::is_same_v<Period, std::ratio<1>>) {
                oss << data.count() << 's';
            } else if constexpr (std::is_same_v<Period, std::milli>) {
                oss << data.count() << "ms";
            } else if constexpr (std::is_same_v<Period, std::micro>) {
                oss << data.count() << "us";
            } else if constexpr (std::is_same_v<Period, std::nano>) {
                oss << data.count() << "ns";
            } else if constexpr (std::is_same_v<Period, std::ratio<60>>) {
                oss << data.count() << "min";
            } else if constexpr (std::is_same_v<Period, std::ratio<3600>>) {
                oss << data.count() << "hr";
            } else if constexpr (std::is_same_v<Period, std::ratio<86400>>) {
                oss << data.count() << "d";
            } else if constexpr (std::is_same_v<Period, std::ratio<604800>>) {
                oss << data.count() << "wk";
            } else if constexpr (std::is_same_v<Period, std::ratio<2629746>>) {
                oss << data.count() << "mon";
            } else if constexpr (std::is_same_v<Period, std::ratio<31556952>>) {
                oss << data.count() << "yr";
            } else if constexpr (Period::num > 1) {
                auto sec = std::chrono::duration_cast<std::chrono::seconds>(data);
                oss << sec.count() << "s";
            } else {
                auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(data);
                oss << nsec.count() << "ns";
            }
            return oss.str();
        }
        static std::chrono::duration<Rep,Period> fromString(std::string_view const &s) {
            if (s.length() == 0) {
                return std::chrono::duration<Rep,Period> {};
            }
            int ii = s.length()-1;
            while (ii >= 0 && std::isalpha(s[ii])) {
                --ii;
            }
            if (ii == s.length()-1) {
                return std::chrono::duration<Rep,Period> {};
            }
            if (ii < 0) {
                return std::chrono::duration<Rep,Period> {};
            }
            auto unit = s.substr(ii+1);
            auto countStr = s.substr(0, ii+1);
            Rep count;
            try {
                count = boost::lexical_cast<Rep>(std::string {countStr});
            } catch (boost::bad_lexical_cast const &) {
                return std::chrono::duration<Rep,Period> {};
            }
            if (unit == "s") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::seconds(count)
                );
            } else if (unit == "ms") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::milliseconds(count)
                );
            } else if (unit == "us") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::microseconds(count)
                );
            } else if (unit == "ns") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::nanoseconds(count)
                );
            } else if (unit == "min") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::minutes(count)
                );
            } else if (unit == "hr") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::hours(count)
                );
#if __cplusplus > 201703L
            } else if (unit == "d") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::days(count)
                );
            } else if (unit == "wk") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::weeks(count)
                );
            } else if (unit == "mon") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::months(count)
                );
            } else if (unit == "yr") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::years(count)
                );
#else 
            } else if (unit == "d") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::hours(count*24)
                );
            } else if (unit == "wk") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::hours(count*24*7)
                );
            } else if (unit == "mon") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::hours(count*2629646/3600)
                );
            } else if (unit == "yr") {
                return std::chrono::duration_cast<std::chrono::duration<Rep,Period>>(
                    std::chrono::hours(count*31556952/3600)
                );
#endif
            } else {
                return std::chrono::duration<Rep,Period> {};
            }
        }
    };

    template <class Rep, class Period>
    class PrintHelper<std::chrono::duration<Rep,Period>> {
    public:
        static void print(std::ostream &os, std::chrono::duration<Rep,Period> const &value) {
            os << ConvertibleWithString<std::chrono::duration<Rep,Period>>::toString(value);
        }
    };

    namespace proto_interop {
        template <class Rep, class Period>
        class IgnoreProxiesForProtoInterop<std::chrono::duration<Rep,Period>> {
        public:
            static constexpr bool value = true;
        };
        template <class Rep, class Period>
        class ProtoEncoder<std::chrono::duration<Rep,Period>, void> {
        public:
            static constexpr uint64_t thisFieldNumber(uint64_t inputFieldNumber) {
                return inputFieldNumber;
            }
            static constexpr uint64_t nextFieldNumber(uint64_t inputFieldNumber) {
                return inputFieldNumber+1;
            }
            static void write(std::optional<uint64_t> fieldNumber, std::chrono::duration<Rep,Period> const &data, std::ostream &os, bool writeDefaultValue) {
                transport::bcl_compat::BclTimeSpanProto p;
                transport::bcl_compat::DurationConverter::write(p, data);
                ProtoEncoder<transport::bcl_compat::BclTimeSpanProto>::write(fieldNumber, p, os, false);
            }
        };
        template <class Rep, class Period>
        struct ProtoWrappable<std::chrono::duration<Rep,Period>, void> {
            static constexpr bool value = true;
        };
        template <class Rep, class Period>
        class ProtoDecoder<std::chrono::duration<Rep,Period>, void> final : public IProtoDecoder<std::chrono::duration<Rep,Period>> {
        private:
            uint64_t baseFieldNumber_;
        public:
            ProtoDecoder(std::chrono::duration<Rep,Period> *output, uint64_t baseFieldNumber) : IProtoDecoder<std::chrono::duration<Rep,Period>>(output), baseFieldNumber_(baseFieldNumber) {}
            static std::vector<uint64_t> responsibleForFieldNumbers(uint64_t baseFieldNumber) {
                return {baseFieldNumber};
            }
            std::optional<std::size_t> read(std::chrono::duration<Rep,Period> &output, internal::FieldHeader const &fh, std::string_view const &input, std::size_t start) override final {
                transport::bcl_compat::BclTimeSpanProto p;
                ProtoDecoder<transport::bcl_compat::BclTimeSpanProto> subDec(&p, baseFieldNumber_);
                auto res = subDec.handle(fh, input, start);
                if (res) {
                    if (!transport::bcl_compat::DurationConverter::read(output, p)) {
                        return std::nullopt;
                    }
                }
                return res;
            }
        };
    }
    namespace bytedata_utils {
        template <class Rep, class Period>
        struct RunCBORSerializer<std::chrono::duration<Rep,Period>, void> {
            static std::string apply(std::chrono::duration<Rep,Period> const &d) {
                return RunCBORSerializer<std::string>::apply(ConvertibleWithString<std::chrono::duration<Rep,Period>>::toString(d));
            }
            static std::size_t apply(std::chrono::duration<Rep,Period> const &d, char *output) {
                return RunCBORSerializer<std::string>::apply(ConvertibleWithString<std::chrono::duration<Rep,Period>>::toString(d), output);
            }
            static std::size_t calculateSize(std::chrono::duration<Rep,Period> const &d) {
                return RunCBORSerializer<std::string>::calculateSize(ConvertibleWithString<std::chrono::duration<Rep,Period>>::toString(d));
            }
        };
        template <class Rep, class Period>
        struct RunCBORDeserializer<std::chrono::duration<Rep,Period>, void> {
            static std::optional<std::tuple<std::chrono::duration<Rep,Period>,size_t>> apply(std::string_view const &s, size_t start) {
                auto durationStr = RunCBORDeserializer<std::string>::apply(s, start);
                if (!durationStr) {
                    return std::nullopt;
                }
                return std::tuple<std::chrono::duration<Rep,Period>,size_t> {ConvertibleWithString<std::chrono::duration<Rep,Period>>::fromString(std::get<0>(*durationStr)), std::get<1>(*durationStr)};
            }
            static std::optional<size_t> applyInPlace(std::chrono::duration<Rep,Period> &output, std::string_view const &s, size_t start) {
                auto durationStr = RunCBORDeserializer<std::string>::apply(s, start);
                if (!durationStr) {
                    return std::nullopt;
                }
                output = ConvertibleWithString<std::chrono::duration<Rep,Period>>::fromString(std::get<0>(*durationStr));
                return std::get<1>(*durationStr);
            }
        };
    }
} } } }

#endif