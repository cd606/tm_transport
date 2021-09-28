#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_TOUCHUPS_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_TOUCHUPS_HPP_

#include <tm_kit/transport/MultiTransportBroadcastListenerManagingUtils.hpp>
#include <tm_kit/transport/MultiTransportBroadcastPublisherManagingUtils.hpp>
#include <tm_kit/basic/WrapFacilitioidConnectorForSerialization.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace multi_transport_touchups {

    struct PublisherTouchupSpec {
        std::string channelSpec;
        std::optional<UserToWireHook> hook = std::nullopt;
        bool threaded = false;
        std::string publisherName = "";
    };

    template <class R, class T>
    struct PublisherTouchup {
        PublisherTouchup(R &r, PublisherTouchupSpec const &spec) {
            auto publisherName = ((spec.publisherName=="")?(std::string("__publisher_touchup_")+typeid(T).name()):spec.publisherName);
            auto pub = MultiTransportBroadcastPublisherManagingUtils<R>
                ::template oneBroadcastPublisher<T>
                (
                    r
                    , publisherName 
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
            auto publisherName = ((spec.publisherName=="")?"__publisher_touchup_bytedata":spec.publisherName);
            auto pub = MultiTransportBroadcastPublisherManagingUtils<R>
                ::oneByteDataBroadcastPublisher
                (
                    r
                    , publisherName 
                    , spec.channelSpec
                    , spec.hook 
                    , spec.threaded
                );
            r.template connectTypedSinkToAllNodes<basic::ByteDataWithTopic>(pub);
        }
    };

    template <class R, template<class... Ts> class ProtocolWrapper, class T>
    struct PublisherTouchupWithProtocol {
        PublisherTouchupWithProtocol(R &r, PublisherTouchupSpec const &spec) {
            auto publisherName = ((spec.publisherName=="")?(std::string("__publisher_touchup_")+typeid(T).name()):spec.publisherName);
            using W = basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,T>;
            if constexpr(std::is_same_v<W,T>) {
                auto pub = MultiTransportBroadcastPublisherManagingUtils<R>
                    ::template oneBroadcastPublisher<T>
                    (
                        r
                        , publisherName 
                        , spec.channelSpec
                        , spec.hook 
                        , spec.threaded
                    );
                r.template connectTypedSinkToAllNodes<basic::TypedDataWithTopic<T>>(pub);
            } else {
                auto pub = MultiTransportBroadcastPublisherManagingUtils<R>
                    ::template oneBroadcastPublisher<W>
                    (
                        r
                        , publisherName 
                        , spec.channelSpec
                        , spec.hook 
                        , spec.threaded
                    );
                auto converter = R::AppType::template liftPure<basic::TypedDataWithTopic<T>>(
                    [](basic::TypedDataWithTopic<T> &&t) -> basic::TypedDataWithTopic<W> {
                        return {std::move(t.topic), W {std::move(t.content)}};
                    }
                );
                auto converterName = publisherName+"__converter";
                r.registerAction(converterName, converter);
                r.connect(r.actionAsSource(converter), pub);
                r.template connectTypedSinkToAllNodes<basic::TypedDataWithTopic<T>>(r.actionAsSink(converter));
            }
        }
    };

    struct ListenerTouchupSpec {
        std::string channelSpec;
        bool withTopic = true;
        std::optional<std::string> topicDescription = std::nullopt;
        std::optional<WireToUserHook> hook = std::nullopt;
    };

    template <class R, class T>
    struct ListenerTouchup {
        ListenerTouchup(R &r, ListenerTouchupSpec const &spec) {
            auto groupName = std::string("__listener_touchup_")+typeid(T).name();
            if (spec.withTopic) {
                auto sub = MultiTransportBroadcastListenerManagingUtils<R>
                    ::template oneBroadcastListenerWithTopic<T>(
                        r 
                        , groupName 
                        , spec.channelSpec
                        , spec.topicDescription
                        , spec.hook 
                    );
                r.template connectSourceToAllUnusedSinks<basic::TypedDataWithTopic<T>>(std::move(sub));
            } else {
                auto sub = MultiTransportBroadcastListenerManagingUtils<R>
                    ::template oneBroadcastListener<T>(
                        r 
                        , groupName 
                        , spec.channelSpec
                        , spec.topicDescription
                        , spec.hook 
                    );
                r.template connectSourceToAllUnusedSinks<T>(std::move(sub));
            }
        }
    };
    template <class R>
    struct ListenerTouchup<R, basic::ByteData> {
        ListenerTouchup(R &r, ListenerTouchupSpec const &spec) {
            auto groupName = std::string("__listener_touchup_bytedata");
            auto sub = MultiTransportBroadcastListenerManagingUtils<R>
                ::oneByteDataBroadcastListener(
                    r 
                    , groupName 
                    , spec.channelSpec
                    , spec.topicDescription
                    , spec.hook 
                );
            if (spec.withTopic) {
                r.template connectSourceToAllUnusedSinks<basic::ByteDataWithTopic>(std::move(sub));
            } else {
                auto removeTopic = R::AppType::template liftPure<basic::ByteDataWithTopic>(
                    [](basic::ByteDataWithTopic &&d) -> basic::ByteData {
                        return basic::ByteData { std::move(d.content) };
                    }
                );
                r.registerAction(groupName+"/removeTopic", removeTopic);
                r.template connectSourceToAllUnusedSinks<basic::ByteData>(r.execute(removeTopic, std::move(sub)));
            }
        }
    };

    struct HeartbeatDirectedListenerTouchupSpec {
        ListenerTouchupSpec heartbeatSpec;
        std::regex serverNameRE;
        std::string broadcastSourceLookupName;
        std::string broadcastTopic;
        bool withTopic = true;
        std::optional<WireToUserHook> hook = std::nullopt;
    };
    template <class R, class T>
    class HeartbeatDirectedListenerTouchup {
    public:
        HeartbeatDirectedListenerTouchup(R &r, HeartbeatDirectedListenerTouchupSpec const &spec) {
            auto groupName = std::string("__heartbeat_directed_listener_touchup_")+typeid(T).name();
            auto heartbeatSub = MultiTransportBroadcastListenerManagingUtils<R>
                ::template oneBroadcastListener<HeartbeatMessage>(
                    r 
                    , groupName+"/heartbeat" 
                    , spec.heartbeatSpec.channelSpec
                    , spec.heartbeatSpec.topicDescription
                    , spec.heartbeatSpec.hook 
                );
            if (spec.withTopic) {
                auto sub = MultiTransportBroadcastListenerManagingUtils<R>
                    ::template setupBroadcastListenerWithTopicThroughHeartbeat<T>(
                        r
                        , heartbeatSub.clone()
                        , spec.serverNameRE
                        , spec.broadcastSourceLookupName
                        , spec.broadcastTopic
                        , groupName
                        , spec.hook
                    );
                if constexpr (std::is_same_v<T, basic::ByteData>) {
                    auto conv = R::AppType::template liftPure<basic::TypedDataWithTopic<basic::ByteData>>(
                        [](basic::TypedDataWithTopic<basic::ByteData> &&t) -> basic::ByteDataWithTopic {
                            return {
                                std::move(t.topic)
                                , std::move(t.content.content)
                            };
                        }
                    );
                    auto convSrc = r.execute(groupName+"/conv", conv, sub.clone());
                    r.template connectSourceToAllUnusedSinks<basic::ByteDataWithTopic>(std::move(convSrc));
                } else {
                    r.template connectSourceToAllUnusedSinks<basic::TypedDataWithTopic<T>>(std::move(sub));
                }
            } else {
                auto sub = MultiTransportBroadcastListenerManagingUtils<R>
                    ::template setupBroadcastListenerThroughHeartbeat<T>(
                        r
                        , heartbeatSub.clone()
                        , spec.serverNameRE
                        , spec.broadcastSourceLookupName
                        , spec.broadcastTopic
                        , groupName
                        , spec.hook
                    );
                r.template connectSourceToAllUnusedSinks<T>(std::move(sub));
            }
        }
    };
} } } } }

#endif