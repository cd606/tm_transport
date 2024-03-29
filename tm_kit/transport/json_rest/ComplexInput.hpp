#ifndef TM_KIT_TRANSPORT_JSON_REST_COMPLEX_INPUT_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_COMPLEX_INPUT_HPP_

#include <tm_kit/basic/SingleLayerWrapper.hpp>
#include <tm_kit/basic/SerializationHelperMacros.hpp>
#include <tm_kit/basic/NlohmannJsonInterop.hpp>
#include <tm_kit/transport/json_rest/JsonRESTClientFacilityUtils.hpp>
#include <string>
#include <sstream>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    #define JSON_REST_COMPLEX_INPUT_ENCODE_FORMAT \
        (json_body) (simple_body) (url_query)
#ifdef _MSC_VER
    #define JSON_REST_COMPLEX_INPUT_FIELDS \
        ((std::string, method)) \
        ((std::string, path)) \
        ((TM_BASIC_CBOR_CAPABLE_STRUCT_PROTECT_TYPE(std::unordered_map<std::string,std::string>), headers)) \
        ((std::string, contentType)) \
        ((std::string, query)) \
        ((std::string, body)) \
        ((std::optional<std::string>, auth_token))
#else
    #define JSON_REST_COMPLEX_INPUT_FIELDS \
        ((std::string, method)) \
        ((std::string, path)) \
        (((std::unordered_map<std::string,std::string>), headers)) \
        ((std::string, contentType)) \
        ((std::string, query)) \
        ((std::string, body)) \
        ((std::optional<std::string>, auth_token))
#endif

    #define JSON_REST_COMPLEX_INPUT_WITH_DATA_FIELDS \
        JSON_REST_COMPLEX_INPUT_FIELDS \
        ((T, data))

    TM_BASIC_CBOR_CAPABLE_ENUM_AS_STRING(ComplexInputEncodeFormat, JSON_REST_COMPLEX_INPUT_ENCODE_FORMAT);
    TM_BASIC_CBOR_CAPABLE_STRUCT(ComplexInput, JSON_REST_COMPLEX_INPUT_FIELDS);
    TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT(((typename, T)), ComplexInputWithData, JSON_REST_COMPLEX_INPUT_WITH_DATA_FIELDS);

    template <class X>
    struct IsComplexInputWithData {
        static constexpr bool value = false;
    };
    template <class T>
    struct IsComplexInputWithData<ComplexInputWithData<T>> {
        static constexpr bool value = true;
        using DataType = T;
    };

    template <class T, class ComplexInputLikeStruct>
    inline void encodeDataForComplexInput(
        ComplexInputEncodeFormat encodeFormat
        , T const &t
        , ComplexInputLikeStruct &output
    ) {
        std::ostringstream oss;
        bool encodeInURL = (encodeFormat == ComplexInputEncodeFormat::url_query);
        bool simplePost = (encodeFormat == ComplexInputEncodeFormat::simple_body);
        if constexpr (basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<T> || std::is_empty_v<T>) {
            if (encodeInURL || simplePost) {
                if constexpr (!std::is_empty_v<T>) {
                    bool start = true;
                    basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvOutput<T>
                        ::outputNameValuePairs(
                            t
                            , [&start,&oss](std::string const &name, std::string const &value) {
                                if (!start) {
                                    oss << '&';
                                }
                                JsonRESTClientFacilityFactoryUtils::urlEscape(oss, name);
                                oss << '=';
                                JsonRESTClientFacilityFactoryUtils::urlEscape(oss, value);
                                start = false;
                            }
                        );
                }
            } else {
                basic::nlohmann_json_interop::Json<T const *>(&t).writeToStream(oss);
            }
        } else {
            basic::nlohmann_json_interop::Json<T const *>(&t).writeToStream(oss);
        }
        if (encodeInURL) {
            output.query = oss.str();
            output.body = "";
        } else {
            output.query = "";
            output.body = oss.str();
        }
        output.contentType = (simplePost?"application/x-www-form-urlencoded":"application/json");
    }   

} } } } }

TM_BASIC_CBOR_CAPABLE_ENUM_AS_STRING_SERIALIZE(dev::cd606::tm::transport::json_rest::ComplexInputEncodeFormat, JSON_REST_COMPLEX_INPUT_ENCODE_FORMAT);
TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::json_rest::ComplexInput, JSON_REST_COMPLEX_INPUT_FIELDS);
TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT_SERIALIZE(((typename, T)), dev::cd606::tm::transport::json_rest::ComplexInputWithData, JSON_REST_COMPLEX_INPUT_WITH_DATA_FIELDS);
#undef JSON_REST_COMPLEX_INPUT_ENCODE_FORMAT
#undef JSON_REST_COMPLEX_INPUT_FIELDS
#undef JSON_REST_COMPLEX_INPUT_WITH_DATA_FIELDS

#endif