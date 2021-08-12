#ifndef TM_KIT_TRANSPORT_BCL_COMPAT_BCL_STRUCTS_HPP_
#define TM_KIT_TRANSPORT_BCL_COMPAT_BCL_STRUCTS_HPP_

#include <tm_kit/basic/ProtoInterop.hpp>
#include <tm_kit/basic/SerializationHelperMacros.hpp>
#include <tm_kit/basic/PrintHelper.hpp>

#include <tm_kit/transport/bcl_compat/Guid.hpp>
#include <tm_kit/transport/bcl_compat/Decimal.hpp>

#include <boost/lexical_cast.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace bcl_compat {

#ifdef _MSC_VER
    #define BCL_COMPAT_BCL_GUID_PROTO_FIELDS \
        ((TM_BASIC_CBOR_CAPABLE_STRUCT_PROTECT_TYPE(dev::cd606::tm::basic::SingleLayerWrapperWithTypeMark<dev::cd606::tm::basic::proto_interop::Fixed,uint64_t>), lo)) \
        ((TM_BASIC_CBOR_CAPABLE_STRUCT_PROTECT_TYPE(dev::cd606::tm::basic::SingleLayerWrapperWithTypeMark<dev::cd606::tm::basic::proto_interop::Fixed,uint64_t>), hi))
#else
    #define BCL_COMPAT_BCL_GUID_PROTO_FIELDS \
        (((dev::cd606::tm::basic::SingleLayerWrapperWithTypeMark<dev::cd606::tm::basic::proto_interop::Fixed,uint64_t>), lo)) \
        (((dev::cd606::tm::basic::SingleLayerWrapperWithTypeMark<dev::cd606::tm::basic::proto_interop::Fixed,uint64_t>), hi))
#endif

    #define BCL_COMPAT_BCL_DECIMAL_PROTO_FIELDS \
        ((uint64_t, lo)) \
        ((uint32_t, hi)) \
        ((uint32_t, signScale))

    TM_BASIC_CBOR_CAPABLE_STRUCT(BclGuidProto, BCL_COMPAT_BCL_GUID_PROTO_FIELDS);
    TM_BASIC_CBOR_CAPABLE_STRUCT(BclDecimalProto, BCL_COMPAT_BCL_DECIMAL_PROTO_FIELDS);

    template <class Env>
    class BclGuid {
    private:
        typename Env::IDType id_;
        class BclGuidProtoWrapper {
        private:
            BclGuidProto p_;
        public:
            BclGuidProtoWrapper() : p_() {
                p_.lo.value = 0;
                p_.hi.value = 0;
            }
            BclGuidProtoWrapper(BclGuidProto const &p) : p_(p) {}
            uint64_t lo() const {
                return p_.lo.value;
            }
            uint64_t hi() const {
                return p_.hi.value;
            }
            void set_lo(uint64_t l) {
                p_.lo.value = l;
            }
            void set_hi(uint64_t h) {
                p_.hi.value = h;
            }
            BclGuidProto const &value() const {
                return p_;
            }
        };
    public:
        BclGuid() : id_(Env::new_id()) {}
        BclGuid(typename Env::IDType const &id) : id_(id) {}
        BclGuid(typename Env::IDType &&id) : id_(std::move(id)) {}
        BclGuid(BclGuid const &) = default;
        BclGuid(BclGuid &&) = default;
        BclGuid &operator=(BclGuid const &) = default;
        BclGuid &operator=(BclGuid &&) = default;
        ~BclGuid() = default;

        BclGuid &operator=(typename Env::IDType const &id) {
            id_ = id;
            return *this;
        }
        BclGuid &operator=(typename Env::IDType &&id) {
            id_ = std::move(id);
            return *this;
        }
        bool operator==(BclGuid const &b) const {
            return (id_ == b.id_);
        }
        typename Env::IDType &operator*() {
            return id_;
        }
        typename Env::IDType const &operator*() const {
            return id_;
        }
        typename Env::IDType *operator->() {
            return &id_;
        }
        typename Env::IDType const *operator->() const {
            return &id_;
        }
        BclGuidProto toProto() const {
            BclGuidProtoWrapper w;
            GuidConverter<Env>::write(w, id_);
            return w.value();
        }
        void fromProto(BclGuidProto const &p) {
            BclGuidProtoWrapper w(p);
            id_ = GuidConverter<Env>::read(w);
        }
    };

    class BclDecimal {
    private:
        boost::multiprecision::cpp_dec_float_100 value_;
        class BclDecimalProtoWrapper {
        private:
            BclDecimalProto p_;
        public:
            BclDecimalProtoWrapper() : p_() {
                p_.lo = 0;
                p_.hi = 0;
                p_.signScale = 0;
            }
            BclDecimalProtoWrapper(BclDecimalProto const &p) : p_(p) {}
            uint64_t lo() const {
                return p_.lo;
            }
            uint32_t hi() const {
                return p_.hi;
            }
            uint32_t signscale() const {
                return p_.signScale;
            }
            void set_lo(uint64_t l) {
                p_.lo = l;
            }
            void set_hi(uint32_t h) {
                p_.hi = h;
            }
            void set_signscale(uint32_t s) {
                p_.signScale = s;
            }
            BclDecimalProto const &value() const {
                return p_;
            }
        };
    public:
        BclDecimal() : value_(0) {}
        BclDecimal(boost::multiprecision::cpp_dec_float_100 const &value) : value_(value) {}
        BclDecimal(boost::multiprecision::cpp_dec_float_100 &&value) : value_(std::move(value)) {}
        BclDecimal(BclDecimal const &) = default;
        BclDecimal(BclDecimal &&) = default;
        BclDecimal &operator=(BclDecimal const &) = default;
        BclDecimal &operator=(BclDecimal &&) = default;
        ~BclDecimal() = default;

        BclDecimal &operator=(boost::multiprecision::cpp_dec_float_100 const &value) {
            value_ = value;
            return *this;
        }
        BclDecimal &operator=(boost::multiprecision::cpp_dec_float_100 &&value) {
            value_ = std::move(value);
            return *this;
        }
        bool operator==(BclDecimal const &d) const {
            return (value_ == d.value_);
        }
        typename boost::multiprecision::cpp_dec_float_100 &operator*() {
            return value_;
        }
        typename boost::multiprecision::cpp_dec_float_100 const &operator*() const {
            return value_;
        }
        typename boost::multiprecision::cpp_dec_float_100 *operator->() {
            return &value_;
        }
        typename boost::multiprecision::cpp_dec_float_100 const *operator->() const {
            return &value_;
        }
        BclDecimalProto toProto() const {
            BclDecimalProtoWrapper w;
            DecimalConverter::write(w, value_);
            return w.value();
        }
        void fromProto(BclDecimalProto const &p) {
            BclDecimalProtoWrapper w(p);
            value_ = DecimalConverter::read(w);
        }
    };

} } } } }

TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::bcl_compat::BclGuidProto, BCL_COMPAT_BCL_GUID_PROTO_FIELDS);
TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::bcl_compat::BclDecimalProto, BCL_COMPAT_BCL_DECIMAL_PROTO_FIELDS);

namespace std {
    template <class Env>
    class hash<dev::cd606::tm::transport::bcl_compat::BclGuid<Env>> {
    public:
        std::size_t operator()(dev::cd606::tm::transport::bcl_compat::BclGuid<Env> const &id) const {
            return std::hash<typename Env::IDType>()(*id);
        }
    };
    template <>
    class hash<dev::cd606::tm::transport::bcl_compat::BclDecimal> {
    public:
        std::size_t operator()(dev::cd606::tm::transport::bcl_compat::BclDecimal const &value) const {
            return std::hash<std::string>()(boost::lexical_cast<std::string>(*value));
        }
    };
}

namespace dev { namespace cd606 { namespace tm { namespace basic { 
    template <class Env>
    class PrintHelper<transport::bcl_compat::BclGuid<Env>> {
    public:
        static void print(std::ostream &os, transport::bcl_compat::BclGuid<Env> const &id) {
            os << Env::id_to_string(*id);
        }
    };
    template <>
    class PrintHelper<transport::bcl_compat::BclDecimal> {
    public:
        static void print(std::ostream &os, transport::bcl_compat::BclDecimal const &value) {
            os << *value;
        }
    };
    namespace bytedata_utils {
        template <class Env>
        struct RunCBORSerializer<transport::bcl_compat::BclGuid<Env>, void> {
            static std::string apply(transport::bcl_compat::BclGuid<Env> const &id) {
                return RunCBORSerializer<std::string>::apply(Env::id_to_string(*id));
            }
            static std::size_t apply(transport::bcl_compat::BclGuid<Env> const &id, char *output) {
                return RunCBORSerializer<std::string>::apply(Env::id_to_string(*id), output);
            }
            static std::size_t calculateSize(transport::bcl_compat::BclGuid<Env> const &id) {
                return RunCBORSerializer<std::string>::calculateSize(Env::id_to_string(*id));
            }
        };
        template <class Env>
        struct RunCBORDeserializer<transport::bcl_compat::BclGuid<Env>, void> {
            static std::optional<std::tuple<transport::bcl_compat::BclGuid<Env>,size_t>> apply(std::string_view const &s, size_t start) {
                auto idStr = RunCBORDeserializer<std::string>::apply(s, start);
                if (!idStr) {
                    return std::nullopt;
                }
                return std::tuple<transport::bcl_compat::BclGuid<Env>,size_t> {transport::bcl_compat::BclGuid<Env> {Env::id_from_string(std::get<0>(*idStr))}, std::get<1>(*idStr)};
            }
            static std::optional<size_t> applyInPlace(transport::bcl_compat::BclGuid<Env> &output, std::string_view const &s, size_t start) {
                auto idStr = RunCBORDeserializer<std::string>::apply(s, start);
                if (!idStr) {
                    return std::nullopt;
                }
                output = Env::id_from_string(std::get<0>(*idStr));
                return std::get<1>(*idStr);
            }
        };
        template <>
        struct RunCBORSerializer<transport::bcl_compat::BclDecimal, void> {
            static std::string apply(transport::bcl_compat::BclDecimal const &value) {
                return RunCBORSerializer<std::string>::apply(boost::lexical_cast<std::string>(*value));
            }
            static std::size_t apply(transport::bcl_compat::BclDecimal const &value, char *output) {
                return RunCBORSerializer<std::string>::apply(boost::lexical_cast<std::string>(*value), output);
            }
            static std::size_t calculateSize(transport::bcl_compat::BclDecimal const &value) {
                return RunCBORSerializer<std::string>::calculateSize(boost::lexical_cast<std::string>(*value));
            }
        };
        template <>
        struct RunCBORDeserializer<transport::bcl_compat::BclDecimal, void> {
            static std::optional<std::tuple<transport::bcl_compat::BclDecimal,size_t>> apply(std::string_view const &s, size_t start) {
                auto valueStr = RunCBORDeserializer<std::string>::apply(s, start);
                if (!valueStr) {
                    return std::nullopt;
                }
                return std::tuple<transport::bcl_compat::BclDecimal,size_t> {transport::bcl_compat::BclDecimal {boost::lexical_cast<boost::multiprecision::cpp_dec_float_100>(std::get<0>(*valueStr))}, std::get<1>(*valueStr)};
            }
            static std::optional<size_t> applyInPlace(transport::bcl_compat::BclDecimal &output, std::string_view const &s, size_t start) {
                auto valueStr = RunCBORDeserializer<std::string>::apply(s, start);
                if (!valueStr) {
                    return std::nullopt;
                }
                output = boost::lexical_cast<boost::multiprecision::cpp_dec_float_100>(std::get<0>(*valueStr));
                return std::get<1>(*valueStr);
            }
        };
    }
    namespace proto_interop {
        template <class Env>
        class ProtoEncoder<transport::bcl_compat::BclGuid<Env>, void> {
        public:
            static void write(std::optional<uint64_t> fieldNumber, transport::bcl_compat::BclGuid<Env> const &id, std::ostream &os) {
                ProtoEncoder<transport::bcl_compat::BclGuidProto>::write(fieldNumber, id.toProto(), os);
            }
        };
        template <class Env>
        class ProtoDecoder<transport::bcl_compat::BclGuid<Env>, void> final : public IProtoDecoder<transport::bcl_compat::BclGuid<Env>> {
        public:
            ProtoDecoder(transport::bcl_compat::BclGuid<Env> *output) : IProtoDecoder<transport::bcl_compat::BclGuid<Env>>(output) {}
            std::optional<std::size_t> read(transport::bcl_compat::BclGuid<Env> &output, internal::ProtoWireType wt, std::string_view const &input, std::size_t start) override final {
                transport::bcl_compat::BclGuidProto p;
                ProtoDecoder<transport::bcl_compat::BclGuidProto> subDec(&p);
                auto res = subDec.handle(wt, input, start);
                if (res) {
                    output.fromProto(p);
                }
                return res;
            }
        };
        template <>
        class ProtoEncoder<transport::bcl_compat::BclDecimal, void> {
        public:
            static void write(std::optional<uint64_t> fieldNumber, transport::bcl_compat::BclDecimal const &id, std::ostream &os) {
                ProtoEncoder<transport::bcl_compat::BclDecimalProto>::write(fieldNumber, id.toProto(), os);
            }
        };
        template <>
        class ProtoDecoder<transport::bcl_compat::BclDecimal, void> final : public IProtoDecoder<transport::bcl_compat::BclDecimal> {
        public:
            ProtoDecoder(transport::bcl_compat::BclDecimal *output) : IProtoDecoder<transport::bcl_compat::BclDecimal>(output) {}
            std::optional<std::size_t> read(transport::bcl_compat::BclDecimal &output, internal::ProtoWireType wt, std::string_view const &input, std::size_t start) override final {
                transport::bcl_compat::BclDecimalProto p;
                ProtoDecoder<transport::bcl_compat::BclDecimalProto> subDec(&p);
                auto res = subDec.handle(wt, input, start);
                if (res) {
                    output.fromProto(p);
                }
                return res;
            }
        };
    }

} } } } 

#endif