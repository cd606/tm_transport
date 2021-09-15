#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERIALIZATION_HELPER_HPP_
#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_SERIALIZATION_HELPER_HPP_

#include <tm_kit/basic/ByteData.hpp>
#include <grpcpp/impl/codegen/config_protobuf.h>
#include <grpcpp/impl/codegen/serialization_traits.h>
#include <grpcpp/impl/codegen/proto_buffer_reader.h>

namespace grpc {
    template <class X>
    class SerializationTraits<
        X
        , std::enable_if_t<
            (
                dev::cd606::tm::basic::bytedata_utils::ProtobufStyleSerializableChecker<X>::IsProtobufStyleSerializable()
                &&
                !std::is_convertible_v<X *, grpc::protobuf::MessageLite *>
            )
            , void
        >
    > {
    public:
        static Status Serialize(const X &x, ByteBuffer *buffer, bool *own_buffer) {
            *own_buffer = 1;
            std::string serialized;
            x.SerializeToString(&serialized);
            Slice slice(serialized);
            ByteBuffer tmp(&slice, 1);
            buffer->Swap(&tmp);
            return Status::OK;
        }
        static Status Deserialize(ByteBuffer *buffer, X *x) {
            std::string bufferCopy;
            bufferCopy.resize(buffer->Length());
            ProtoBufferReader r(buffer);
            char *copyPtr = bufferCopy.data();
            const void *p;
            int sz;
            while (r.Next(&p, &sz)) {
                if (sz > 0) {
                    std::memcpy(copyPtr, p, sz);
                    copyPtr += sz;
                }
            }
            if (x->ParseFromString(bufferCopy)) {
                return Status::OK;
            } else {
                return Status(StatusCode::INVALID_ARGUMENT, "Failed to parse");
            }
        }
    };

    template <>
    class SerializationTraits<
        dev::cd606::tm::basic::ByteData, void
    > {
    public:
        static Status Serialize(const dev::cd606::tm::basic::ByteData &x, ByteBuffer *buffer, bool *own_buffer) {
            *own_buffer = 1;
            Slice slice(x.content);
            ByteBuffer tmp(&slice, 1);
            buffer->Swap(&tmp);
            return Status::OK;
        }
        static Status Deserialize(ByteBuffer *buffer, dev::cd606::tm::basic::ByteData *x) {
            x->content.resize(buffer->Length());
            ProtoBufferReader r(buffer);
            char *copyPtr = x->content.data();
            const void *p;
            int sz;
            while (r.Next(&p, &sz)) {
                if (sz > 0) {
                    std::memcpy(copyPtr, p, sz);
                    copyPtr += sz;
                }
            }
            return Status::OK;
        }
    };
}

#endif