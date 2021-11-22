#ifndef TM_KIT_TRANSPORT_BCL_COMPAT_DATE_TIME_HPP_
#define TM_KIT_TRANSPORT_BCL_COMPAT_DATE_TIME_HPP_

#include <tm_kit/infra/ChronoUtils.hpp>
#include <tm_kit/basic/DateHolder.hpp>
#include <tm_kit/transport/bcl_compat/Duration.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace bcl_compat {

    TM_BASIC_CBOR_CAPABLE_ENUM_AS_STRING(
        DateTimeKind
        , (UNSPECIFIED) (UTC) (LOCAL)
    );

#define BCL_COMPAT_BCL_DATE_TIME_PROTO_FIELDS \
    ((dev::cd606::tm::basic::proto_interop::SInt64, value)) \
    ((dev::cd606::tm::transport::bcl_compat::TimeSpanScale, scale)) \
    ((dev::cd606::tm::transport::bcl_compat::DateTimeKind, kind)) 

    TM_BASIC_CBOR_CAPABLE_STRUCT(BclDateTimeProto, BCL_COMPAT_BCL_DATE_TIME_PROTO_FIELDS);

    class DateTimeConverter {
    public:
        template <class ProtoType>
        static void write(ProtoType &p, std::chrono::system_clock::time_point const &data) {
            if constexpr (std::is_same_v<ProtoType, BclDateTimeProto>) {
                auto duration = data.time_since_epoch();
                BclTimeSpanProto tsp;
                DurationConverter::write(tsp, duration);
                p.value = tsp.value;
                p.scale = tsp.scale;
                p.kind = DateTimeKind::UTC;
            } else {
                using DTKindType = std::decay_t<decltype(p.kind())>;
                auto duration = data.time_since_epoch();
                DurationConverter::write(p, duration);
                p.set_kind(static_cast<DTKindType>(static_cast<int32_t>(DateTimeKind::UTC)));
            }
        }
    private:
        static std::chrono::system_clock::time_point gm_to_local(std::chrono::system_clock::time_point const &tp) {
            //The input is the result of "local time tm converted as gm time into time_t"
            //So suppose the correct local time is 10:00AM, and UTC time is 15:00PM, 
            //the input is actually a time point corresponding to 10:00AM UTC
            //=5:00AM local, and we need to add the (UTC-local) difference 
            //(=5 hours) to get to correct time point
            auto t = std::chrono::system_clock::to_time_t(tp);
            std::tm *m = std::gmtime(&t); //in the example, this will show up as 10AM
            std::tm m1;
            m1.tm_year = m->tm_year;
            m1.tm_mon = m->tm_mon;
            m1.tm_mday = m->tm_mday;
            m1.tm_hour = m->tm_hour;
            m1.tm_min = m->tm_min;
            m1.tm_sec = m->tm_sec;
            m1.tm_isdst = -1;
            auto t1 = std::mktime(&m1); //this is a time_t corresponding to 10AM local=15PM UTC
            auto diff = std::chrono::system_clock::from_time_t(t1)-std::chrono::system_clock::from_time_t(t); //in example, this is five hours
            return tp+diff;
        }
    public:
        template <class ProtoType>
        static bool read(std::chrono::system_clock::time_point &output, ProtoType const &p) {
            if constexpr (std::is_same_v<ProtoType, BclDateTimeProto>) {
                BclTimeSpanProto tsp;
                tsp.value = p.value;
                tsp.scale = p.scale;
                std::chrono::system_clock::duration d;
                if (!DurationConverter::read(d, tsp)) {
                    return false;
                }
                //when we see an "unspecified" value, we treat it
                //as a "local tm converted as gm value", this is
                //at least how protobuf-net treats a value such as
                //DateTime.Now
                if (p.kind == DateTimeKind::UNSPECIFIED) {
                    output = gm_to_local(std::chrono::system_clock::time_point {d});
                } else {
                    output = std::chrono::system_clock::time_point {d};
                }
                return true;
            } else {
                std::chrono::system_clock::duration d;
                if (!DurationConverter::read(d, p)) {
                    return false;
                }
                if (static_cast<int32_t>(p.kind()) == static_cast<int32_t>(DateTimeKind::UNSPECIFIED)) {
                    output = gm_to_local(std::chrono::system_clock::time_point {d});
                } else {
                    output = std::chrono::system_clock::time_point {d};
                }
                return true;
            }
        }
        template <class ProtoType>
        static void write(ProtoType &p, std::tm const &data) {
            std::tm tmCopy = data;
            auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tmCopy));
            write(p, tp);
        }
        template <class ProtoType>
        static bool read(std::tm &output, ProtoType const &p) {
            std::chrono::system_clock::time_point tp;
            if (!read(tp, p)) {
                return false;
            }
            std::time_t t = std::chrono::system_clock::to_time_t(tp);
            std::tm *m = std::localtime(&t);
            output = *m;
            return true;
        }
        template <class ProtoType>
        static void write(ProtoType &p, basic::DateHolder const &data) {
            write(p, infra::withtime_utils::parseLocalTime(data.year, data.month, data.day, 0, 0, 0));
        }
        template <class ProtoType>
        static bool read(basic::DateHolder &output, ProtoType const &p) {
            std::chrono::system_clock::time_point tp;
            if (!read(tp, p)) {
                return false;
            }
            std::time_t t = std::chrono::system_clock::to_time_t(tp);
            std::tm *m = std::localtime(&t);
            output.year = m->tm_year+1900;
            output.month = m->tm_mon+1;
            output.day = m->tm_mday;
            return true;
        }
    };

} } } } }

TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::bcl_compat::BclDateTimeProto, BCL_COMPAT_BCL_DATE_TIME_PROTO_FIELDS);

#undef BCL_COMPAT_BCL_TIME_SPAN_PROTO_FIELDS

namespace dev { namespace cd606 { namespace tm { namespace basic { 
    namespace proto_interop {
        #define ADD_ONE_BCL_DATETIME_SUPPORT(t) \
        template <> \
        class ProtoEncoder<t, void> {\
        public:\
            static constexpr uint64_t thisFieldNumber(uint64_t inputFieldNumber) {\
                return inputFieldNumber;\
            }\
            static constexpr uint64_t nextFieldNumber(uint64_t inputFieldNumber) {\
                return inputFieldNumber+1;\
            }\
            static void write(std::optional<uint64_t> fieldNumber, t const &data, std::ostream &os, bool writeDefaultValue) {\
                transport::bcl_compat::BclDateTimeProto p;\
                transport::bcl_compat::DateTimeConverter::write(p, data);\
                ProtoEncoder<transport::bcl_compat::BclDateTimeProto>::write(fieldNumber, p, os, false);\
            }\
        };\
        template <>\
        struct ProtoWrappable<t, void> {\
            static constexpr bool value = true;\
        };\
        template <>\
        class ProtoDecoder<t, void> final : public IProtoDecoder<t> {\
        private:\
            uint64_t baseFieldNumber_;\
        public:\
            ProtoDecoder(t *output, uint64_t baseFieldNumber) : IProtoDecoder<t>(output), baseFieldNumber_(baseFieldNumber) {}\
            static std::vector<uint64_t> responsibleForFieldNumbers(uint64_t baseFieldNumber) {\
                return {baseFieldNumber};\
            }\
            std::optional<std::size_t> read(t &output, internal::FieldHeader const &fh, std::string_view const &input, std::size_t start) override final {\
                transport::bcl_compat::BclDateTimeProto p;\
                ProtoDecoder<transport::bcl_compat::BclDateTimeProto> subDec(&p, baseFieldNumber_);\
                auto res = subDec.handle(fh, input, start);\
                if (res) {\
                    if (!transport::bcl_compat::DateTimeConverter::read(output, p)) {\
                        return std::nullopt;\
                    }\
                }\
                return res;\
            }\
        };

        ADD_ONE_BCL_DATETIME_SUPPORT(std::chrono::system_clock::time_point);
        ADD_ONE_BCL_DATETIME_SUPPORT(basic::DateHolder);
        ADD_ONE_BCL_DATETIME_SUPPORT(std::tm);

        #undef ADD_ONE_BCL_DATETIME_SUPPORT
    }
    
} } } }

#endif