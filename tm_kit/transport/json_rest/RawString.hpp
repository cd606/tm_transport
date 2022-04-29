#ifndef TM_KIT_TRANSPORT_JSON_REST_RAW_STRING_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_RAW_STRING_HPP_

#include <tm_kit/basic/SingleLayerWrapper.hpp>
#include <tm_kit/basic/SerializationHelperMacros.hpp>
#include <string>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    struct RawStringMark {};
    using RawString = basic::SingleLayerWrapperWithTypeMark<RawStringMark, std::string>;

    using HeaderFields = std::unordered_map<std::string, std::string>;

    #define RAW_STRING_WITH_STATUS_FIELDS \
        ((unsigned, status)) \
        ((dev::cd606::tm::transport::json_rest::HeaderFields, headerFields)) \
        ((std::string, body))
    TM_BASIC_CBOR_CAPABLE_STRUCT(RawStringWithStatus, RAW_STRING_WITH_STATUS_FIELDS);

} } } } }

TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::json_rest::RawStringWithStatus, RAW_STRING_WITH_STATUS_FIELDS);
#undef RAW_STRING_WITH_STATUS_FIELDS

#endif