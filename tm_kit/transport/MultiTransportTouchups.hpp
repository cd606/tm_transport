#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_TOUCHUPS_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_TOUCHUPS_HPP_

#include <tm_kit/transport/MultiTransportBroadcastListenerManagingUtils.hpp>
#include <tm_kit/transport/MultiTransportBroadcastPublisherManagingUtils.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace multi_transport_touchups {

    struct PublisherTouchupSpec {
        std::string channelSpec;
        std::optional<UserToWireHook> hook = std::nullopt;
        bool threaded = false;
    };

    template <class R, class T>
    struct PublisherTouchup {
        PublisherTouchup(R &r, PublisherTouchupSpec const &spec) {
            auto groupName = std::string("publisher_touchup_")+typeid(T).name();
            auto pub = MultiTransportBroadcastPublisherManagingUtils<R>
                ::template oneBroadcastPublisher<T>
                (
                    r
                    , groupName 
                    , spec.channelSpec
                    , spec.hook 
                    , spec.threaded
                );
            r.template connectTypedSinkToAllNodes<basic::TypedDataWithTopic<T>>(pub);
        }
    };
    template <class R>
    struct PublisherTouchup<R, basic::ByteData> {
        PublisherTouchup(R &r, PublisherTouchupSpec const &spec) {
            std::string groupName = "publisher_touchup_bytedata";
            auto pub = MultiTransportBroadcastPublisherManagingUtils<R>
                ::oneByteDataBroadcastPublisher
                (
                    r
                    , groupName 
                    , spec.channelSpec
                    , spec.hook 
                    , spec.threaded
                );
            r.template connectTypedSinkToAllNodes<basic::ByteDataWithTopic>(pub);
        }
    };

} } } } }

#endif