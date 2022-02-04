#ifndef TM_KIT_TRANSPORT_JSON_REST_RAW_STRING_HPP_
#define TM_KIT_TRANSPORT_JSON_REST_RAW_STRING_HPP_

#include <tm_kit/basic/SingleLayerWrapper.hpp>
#include <string>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    struct RawStringMark {};
    using RawString = basic::SingleLayerWrapperWithTypeMark<RawStringMark, std::string>;

} } } } }

#endif