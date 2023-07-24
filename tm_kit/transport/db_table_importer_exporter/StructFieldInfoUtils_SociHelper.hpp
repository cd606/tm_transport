#ifndef TM_KIT_TRANSPORT_STRUCT_FIELD_INFO_UTILS_SOCI_HELPER_HPP_
#define TM_KIT_TRANSPORT_STRUCT_FIELD_INFO_UTILS_SOCI_HELPER_HPP_
#include <string>
#include <string_view>
#include <sstream>
#include <optional>
#include <chrono>
#include <limits>
#include <time.h>

#include <boost/lexical_cast.hpp>

#include <tm_kit/infra/ChronoUtils.hpp>
#include <tm_kit/basic/DateHolder.hpp>
#include <tm_kit/basic/ChronoUtils_AddOn.hpp>
#include <tm_kit/basic/ConvertibleWithString.hpp>

#include <soci/row.h>
#include <soci/blob.h>
#include <soci/mysql/soci-mysql.h>
#include <soci/sqlite3/soci-sqlite3.h>

#ifdef _MSC_VER
#undef max
#undef min
#endif

namespace dev::cd606::tm::transport::struct_field_info_utils::db_table_importer_exporter
{
    namespace db_traits
    {
        class MysqlTraits
        {
        public:
            static constexpr bool HasDateFormatSupport = true;
            static std::string ISO8601FormatedField(std::string_view const &fieldName)
            {
                std::ostringstream oss;
                oss << "DATE_FORMAT(" << fieldName << ",'%Y-%m-%dT%H:%i:%S.%f')";
                return oss.str();
            }
            static std::string toISO8601FieldValue(std::string_view const &fieldName)
            {
                std::ostringstream oss;
                oss << "STR_TO_DATE(:" << fieldName << ",'%Y-%m-%dT%H:%i:%S.%f')";
                return oss.str();
            }
        };

        class Sqlite3Traits
        {
        public:
            static constexpr bool HasDateFormatSupport = true;
            static std::string ISO8601FormatedField(std::string_view const &fieldName)
            {
                /*
                std::ostringstream oss;
                oss << "strftime('%Y-%m-%dT%H:%M:%f'," << fieldName << ')';
                return oss.str();
                */
                return std::string {fieldName};
            }
            static std::string toISO8601FieldValue(std::string_view const &fieldName)
            {
                std::ostringstream oss;
                oss << ":" << fieldName;
                return oss.str();
            }
        };
    }

    namespace soci_helper
    {
        inline const char *sociDataTypeString(soci::data_type dt)
        {
            switch (dt)
            {
            case soci::dt_string:
                return "soci::dt_string";
            case soci::dt_date:
                return "soci::dt_date";
            case soci::dt_double:
                return "soci::dt_double";
            case soci::dt_integer:
                return "soci::dt_integer";
            case soci::dt_long_long:
                return "soci::dt_long_long";
            case soci::dt_unsigned_long_long:
                return "soci::dt_unsigned_long_long";
            case soci::dt_blob:
                return "soci::dt_blob";
            case soci::dt_xml:
                return "soci::dt_xml";
            default:
                return "unknown";
            }
        }

        template <typename T, typename Enable = void>
        class SociValueExtractor
        {
        };

        template <>
        class SociValueExtractor<std::tm, void>
        {
        public:
            static std::tm extract(soci::row const &row, std::size_t index)
            {
                auto const &props = row.get_properties(index);
                switch (props.get_data_type())
                {
                case soci::dt_date:
                {
                    return row.get<std::tm>(index);
                }
                break;
                default:
                    throw std::runtime_error(std::string("cannot convert from '") + sociDataTypeString(props.get_data_type()) + "' to std::tm");
                }
            }
        };

        template <>
        class SociValueExtractor<basic::DateHolder, void>
        {
        public:
            static basic::DateHolder extract(soci::row const &row, std::size_t index)
            {
                auto const &props = row.get_properties(index);
                switch (props.get_data_type())
                {
                case soci::dt_string:
                    return basic::parseDateHolder(row.get<std::string>(index));
                    break;
                case soci::dt_date:
                {
                    auto tm = row.get<std::tm>(index);
                    return basic::DateHolder{
                        (uint16_t)(tm.tm_year + 1900),
                        (uint8_t)(tm.tm_mon + 1),
                        (uint8_t)tm.tm_mday};
                }
                break;
                default:
                    throw std::runtime_error(std::string("cannot convert from '") + sociDataTypeString(props.get_data_type()) + "' to dev::cd606::tm::basic::DateHolder");
                }
            }
        };

        template <>
        class SociValueExtractor<std::chrono::system_clock::time_point, void>
        {
        public:
            static std::chrono::system_clock::time_point extract(soci::row const &row, std::size_t index)
            {
                auto const &props = row.get_properties(index);
                switch (props.get_data_type())
                {
                case soci::dt_integer:
                    return infra::withtime_utils::epochDurationToTime<std::chrono::nanoseconds>(row.get<int>(index));
                case soci::dt_long_long:
                    return infra::withtime_utils::epochDurationToTime<std::chrono::nanoseconds>(row.get<long long>(index));
                case soci::dt_unsigned_long_long:
                    return infra::withtime_utils::epochDurationToTime<std::chrono::nanoseconds>(row.get<unsigned long long>(index));
                case soci::dt_date:
                {
                    auto tm = row.get<std::tm>(index);
                    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
                }
                case soci::dt_string:
                    return infra::withtime_utils::parseLocalTime(row.get<std::string>(index));
                default:
                    throw std::runtime_error(std::string("cannot convert from '") + sociDataTypeString(props.get_data_type()) + "' to std::chrono::system_clock::time_point");
                }
            }
        };

        template <typename T>
        class SociValueExtractor<T, std::enable_if_t<std::is_integral_v<T>, void>>
        {
        public:
            static T extract(soci::row const &row, std::size_t index)
            {
                auto const &props = row.get_properties(index);
                switch (props.get_data_type())
                {
                case soci::dt_integer:
                    return extractInternal<T>(row.get<int>(index));
                case soci::dt_long_long:
                    return extractInternal<T>(row.get<long long>(index));
                case soci::dt_unsigned_long_long:
                    return extractInternal<T>(row.get<unsigned long long>(index));
                case soci::dt_string:
                    return boost::lexical_cast<T>(row.get<std::string>(index));
                default:
                    throw std::runtime_error(std::string("cannot convert from '") + sociDataTypeString(props.get_data_type()) + "' to integral value");
                }
            }

        private:
            template <typename U>
            static T extractInternal(U u)
            {
                if constexpr (std::is_same_v<T, U>) {
                    return u;
                } else {
                    if (u <= std::numeric_limits<T>::max() && u >= std::numeric_limits<T>::min())
                    {
                        return (T)u;
                    }
                    else
                    {
                        throw std::runtime_error("data range exceeds");
                    }
                }
            }
        };

        template <typename T>
        class SociValueExtractor<T, std::enable_if_t<std::is_floating_point_v<T>, void>>
        {
        public:
            static T extract(soci::row const &row, std::size_t index)
            {
                auto const &props = row.get_properties(index);
                switch (props.get_data_type())
                {
                case soci::dt_integer:
                    return static_cast<T>(row.get<int>(index));
                case soci::dt_long_long:
                    return static_cast<T>(row.get<long long>(index));
                case soci::dt_unsigned_long_long:
                    return static_cast<T>(row.get<unsigned long long>(index));
                case soci::dt_double:
                    return static_cast<T>(row.get<double>(index));
                case soci::dt_string:
                    return boost::lexical_cast<T>(row.get<std::string>(index));
                default:
                    throw std::runtime_error(std::string("cannot convert from '") + sociDataTypeString(props.get_data_type()) + "' to floating point value");
                }
            }
        };

        template <>
        class SociValueExtractor<std::string, void>
        {
        public:
            static std::string extract(soci::row const &row, std::size_t index)
            {
                auto const &props = row.get_properties(index);
                switch (props.get_data_type())
                {
                case soci::dt_string:
                    return row.get<std::string>(index);
                /*
                ** sadlly soci doesn't support blob in rowset api, there's open issue related to this: https://github.com/SOCI/soci/issues/922
                case soci::dt_blob:
                {
                    auto data = row.get<soci::blob>(index);
                    std::string dest;
                    dest.resize(data.get_len());
                    auto dataRead = data.read_from_start(dest.data(), dest.size());
                    if (dataRead < data.get_len())
                    {
                        throw std::runtime_error("read partial data from blob");
                    }
                    return dest;
                }
                */
                default:
                    throw std::runtime_error(std::string("cannot convert from '") + sociDataTypeString(props.get_data_type()) + "' to std::string");
                }
            }
        };

        template <typename T>
        class SociValueExtractor<T, std::enable_if_t<basic::ConvertibleWithString<T>::value, void>>
        {
        public:
            static T extract(soci::row const &row, std::size_t index)
            {
                return basic::ConvertibleWithString<T>::fromString(SociValueExtractor<std::string>::extract(row, index));
            }
        };

        template <typename T>
        class SociValueExtractor<std::optional<T>, void>
        {
        public:
            static T extract(soci::row const &row, std::size_t index)
            {
                switch (row.get_indicator(index))
                {
                case soci::i_ok:
                    return SociValueExtractor<T>::extract(row, index);
                case soci::i_null:
                case soci::i_truncated:
                    return std::nullopt;
                }
            }
        };
    } // namespace soci_helper

    class SociHelper
    {
    public:
        template <typename T>
        static void fillData(soci::row const &row, std::size_t index, T &data)
        {
            data = soci_helper::SociValueExtractor<T>::extract(row, index);
        }
    };

    template <class T, typename DBTraits=void>
    class StructFieldInfoBasedDataFiller {
    private:
        template <class R, int FieldCount, int FieldIndex>
        static void fillData_internal(T &data, R const &source, int startSourceIndex) {
            if constexpr (FieldIndex >= 0 && FieldIndex < FieldCount) {
                SociHelper::fillData(source, FieldIndex+startSourceIndex, dev::cd606::tm::basic::StructFieldTypeInfo<T,FieldIndex>::access(data));
                if constexpr (FieldIndex < FieldCount-1) {
                    fillData_internal<R,FieldCount,FieldIndex+1>(data, source, startSourceIndex);
                }
            }
        }
        template <std::size_t FieldIndex>
        static void addToCommaSeparatedFieldNamesForSelect(std::ostream &oss, bool &begin) {
            if constexpr (FieldIndex < dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES.size()) {
                if (!begin) {
                    oss << ", ";
                }
                begin = false;
                if constexpr (std::is_same_v<typename dev::cd606::tm::basic::StructFieldTypeInfo<T, FieldIndex>::TheType, std::chrono::system_clock::time_point> ||
                              dev::cd606::tm::basic::IsTimePointAsString<typename dev::cd606::tm::basic::StructFieldTypeInfo<T, FieldIndex>::TheType>::Value)
                {
                    if constexpr (std::is_same_v<DBTraits, void>)
                    {
                        oss << "DATE_FORMAT(" << dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex] << ",'%Y-%m-%dT%H:%i:%S.%f')";
                    }
                    else if constexpr (DBTraits::HasDateFormatSupport)
                    {
                        oss << DBTraits::ISO8601FormatedField(dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex]);
                    }
                    else 
                    {
                        oss << dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex];
                    }
                }
                else
                {
                    oss << dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex];
                }
                addToCommaSeparatedFieldNamesForSelect<FieldIndex+1>(oss, begin);
            }
        }
        template <std::size_t FieldIndex>
        static void addValueFieldsToInsertValueList_internal(std::ostream &oss, bool &begin) {
            if constexpr (FieldIndex < dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES.size()) {
                if (!begin) {
                    oss << ',';
                }
                begin = false;
                if constexpr (std::is_same_v<typename dev::cd606::tm::basic::StructFieldTypeInfo<T, FieldIndex>::TheType, std::chrono::system_clock::time_point> ||
                              dev::cd606::tm::basic::IsTimePointAsString<typename dev::cd606::tm::basic::StructFieldTypeInfo<T, FieldIndex>::TheType>::Value)
                {
                    if constexpr (std::is_same_v<DBTraits, void>)
                    {
                        oss << "STR_TO_DATE(:" << dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex] << ",'%Y-%m-%dT%H:%i:%S.%f')";
                    }
                    else if constexpr (DBTraits::HasDateFormatSupport)
                    {
                        oss << DBTraits::toISO8601FieldValue(dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex]);
                    }
                    else 
                    {
                        oss << ':' << dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex];
                    }
                } else {
                    oss << ':' << dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex];
                }
                addValueFieldsToInsertValueList_internal<FieldIndex+1>(oss, begin);
            }
        }
        template <std::size_t FieldIndex>
        static void addValueFieldsToWhereClauseList_internal(std::ostream &oss, bool &begin) {
            if constexpr (FieldIndex < basic::StructFieldInfo<T>::FIELD_NAMES.size()) {
                if (!begin) {
                    oss << " AND ";
                }
                begin = false;
                oss << dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex] << " = ";
                if constexpr (std::is_same_v<typename dev::cd606::tm::basic::StructFieldTypeInfo<T, FieldIndex>::TheType, std::chrono::system_clock::time_point> ||
                              dev::cd606::tm::basic::IsTimePointAsString<typename dev::cd606::tm::basic::StructFieldTypeInfo<T, FieldIndex>::TheType>::Value)
                {
                    if constexpr (std::is_same_v<DBTraits, void>)
                    {
                        oss << "STR_TO_DATE(:" << dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex] << ",'%Y-%m-%dT%H:%i:%S.%f')";
                    }
                    else if constexpr (DBTraits::HasDateFormatSupport)
                    {
                        oss << DBTraits::toISO8601FieldValue(dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex]);
                    }
                    else 
                    {
                        oss << ':' << dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex];
                    }
                } else {
                    oss << ':' << dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex];
                }
                addValueFieldsToWhereClauseList_internal<FieldIndex+1>(oss, begin);
            }
        }
    public:
        static constexpr std::size_t FieldCount = dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES.size();
        static std::string commaSeparatedFieldNames() {
            bool begin = true;
            std::ostringstream oss;
            for (auto const &n : dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES) {
                if (!begin) {
                    oss << ", ";
                }
                begin = false;
                oss << n;
            }
            return oss.str();
        }
        static std::string commaSeparatedFieldNamesForSelect() {
            bool begin = true;
            std::ostringstream oss;
            addToCommaSeparatedFieldNamesForSelect<0>(oss, begin);
            return oss.str();
        }
        static void addValueFieldsToInsertValueList(std::ostream &oss, bool &begin) {
            addValueFieldsToInsertValueList_internal<0>(oss, begin);
        }
        static void addValueFieldsToWhereClauseValueList(std::ostream &oss, bool &begin) {
            addValueFieldsToWhereClauseList_internal<0>(oss, begin);
        }
        template <class R>
        static void fillData(T &data, R const &source, int startSourceIndex=0) {
            fillData_internal<R, dev::cd606::tm::basic::StructFieldInfo<T>::FIELD_NAMES.size(), 0>(data, source, startSourceIndex);
        }
        template <class R>
        static T retrieveData(R const &source, int startSourceIndex=0) {
            T ret;
            fillData(ret, source, startSourceIndex);
            return ret;
        }
    };
}

#endif // TM_KIT_BASIC_STRUCT_FIELD_INFO_UTILS_SOCI_HELPER_HPP_