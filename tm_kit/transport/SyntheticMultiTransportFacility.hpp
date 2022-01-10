#ifndef TM_KIT_TRANSPORT_SYNTHETIC_MULTI_TRANSPORT_FACILITY_HPP_
#define TM_KIT_TRANSPORT_SYNTHETIC_MULTI_TRANSPORT_FACILITY_HPP_

#include <tm_kit/infra/AppClassifier.hpp>

#include <tm_kit/basic/AppRunnerUtils.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/EncodeKeyThroughProxy.hpp>

#include <tm_kit/transport/MultiTransportBroadcastListenerManagingUtils.hpp>
#include <tm_kit/transport/MultiTransportBroadcastPublisherManagingUtils.hpp>
#include <tm_kit/transport/AbstractHookFactoryComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    template <class R>
    class SyntheticMultiTransportFacility {
    private:
        using M = typename R::AppType;
        using Env = typename M::EnvironmentType;
    public:
        static_assert(infra::app_classification_v<M> == infra::AppClassification::RealTime, "SyntheticMultiTransportFacility can only be used for real time app");

        template <
            template<class... Xs> class OutgoingProtocolWrapper
            , template<class... Xs> class IncomingProtocolWrapper
            , class A 
            , class B 
        >
        static auto client(
            R &r 
            , std::string const &prefix
            , std::string const &outgoingSpec 
            , std::string const &outgoingTopic
            , std::string const &incomingSpec 
            , std::optional<std::string> const &incomingTopic
            , bool multiCallback = false
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) -> typename R::template FacilitioidConnector<A,B>
        {
            std::optional<UserToWireHook> outgoingHook = DefaultHookFactory<Env>::template supplyOutgoingHook<A>(
                r.environment()
                , hookPair?hookPair->userToWire:std::nullopt
            );
            std::optional<WireToUserHook> incomingHook = DefaultHookFactory<Env>::template supplyIncomingHook<B>(
                r.environment()
                , hookPair?hookPair->wireToUser:std::nullopt
            );
            auto addTopic = M::template liftPure<typename M::template Key<A>>(
                [outgoingTopic](typename M::template Key<A> &&x) -> basic::TypedDataWithTopic<typename M::template Key<A>> {
                    return {outgoingTopic, std::move(x)};
                }
            );
            r.registerAction(prefix+"/addTopic", addTopic);
            auto sink = MultiTransportBroadcastPublisherManagingUtils<R>::template oneBroadcastPublisherWithProtocol<
                OutgoingProtocolWrapper
                , typename M::template Key<A>
            >(
                r, prefix+"/exporter", outgoingSpec, outgoingHook
            );
            r.connect(r.actionAsSource(addTopic), sink);
            auto source = MultiTransportBroadcastListenerManagingUtils<R>::template oneBroadcastListenerWithProtocol<
                IncomingProtocolWrapper
                , typename M::template Key<B>
            >(
                r, prefix+"/importer", incomingSpec, incomingTopic, incomingHook
            );
            return basic::AppRunnerUtilComponents<R>::template syntheticRemoteFacility<A,B>(
                prefix
                , r.sinkAsSinkoid(r.actionAsSink(addTopic))
                , r.sourceAsSourceoid(std::move(source))
                , multiCallback
            );
        }

        template <
            template<class... Xs> class IncomingProtocolWrapper
            , template<class... Xs> class OutgoingProtocolWrapper
            , class A 
            , class B 
        >
        static void serverWithFacility(
            R &r 
            , std::string const &prefix
            , std::string const &incomingSpec 
            , std::optional<std::string> const &incomingTopic
            , std::string const &outgoingSpec 
            , std::string const &outgoingTopic
            , typename R::template FacilitioidConnector<A,B> const &facility
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) 
        {
            std::optional<WireToUserHook> incomingHook = DefaultHookFactory<Env>::template supplyIncomingHook<A>(
                r.environment()
                , hookPair?hookPair->wireToUser:std::nullopt
            );
            std::optional<UserToWireHook> outgoingHook = DefaultHookFactory<Env>::template supplyOutgoingHook<B>(
                r.environment()
                , hookPair?hookPair->userToWire:std::nullopt
            );
            auto createOutgoingKey = M::template liftPure<typename M::template KeyedData<A,B>>(
                [outgoingTopic](typename M::template KeyedData<A,B> &&x) -> basic::TypedDataWithTopic<typename M::template Key<B>> {
                    return {outgoingTopic, {x.key.id(), std::move(x.data)}};
                }                
            );
            r.registerAction(prefix+"/createOutgoingKey", createOutgoingKey);
            auto sink = MultiTransportBroadcastPublisherManagingUtils<R>::template oneBroadcastPublisherWithProtocol<
                OutgoingProtocolWrapper
                , typename M::template Key<B>
            >(
                r, prefix+"/exporter", outgoingSpec, outgoingHook
            );
            r.connect(r.actionAsSource(createOutgoingKey), sink);
            auto source = MultiTransportBroadcastListenerManagingUtils<R>::template oneBroadcastListenerWithProtocol<
                IncomingProtocolWrapper
                , typename M::template Key<A>
            >(
                r, prefix+"/importer", incomingSpec, incomingTopic, incomingHook
            );
            facility(
                r, std::move(source), r.actionAsSink(createOutgoingKey)
            );
        }

        template <
            template<class... Xs> class IncomingProtocolWrapper
            , template<class... Xs> class OutgoingProtocolWrapper
            , class A 
            , class B 
        >
        static void serverWithPathway(
            R &r 
            , std::string const &prefix
            , std::string const &incomingSpec 
            , std::optional<std::string> const &incomingTopic
            , std::string const &outgoingSpec 
            , std::string const &outgoingTopic
            , typename R::template Pathway<typename M::template Key<A>,typename M::template Key<B>> const &pathway
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) 
        {
            std::optional<WireToUserHook> incomingHook = DefaultHookFactory<Env>::template supplyIncomingHook<A>(
                r.environment()
                , hookPair?hookPair->wireToUser:std::nullopt
            );
            std::optional<UserToWireHook> outgoingHook = DefaultHookFactory<Env>::template supplyOutgoingHook<B>(
                r.environment()
                , hookPair?hookPair->userToWire:std::nullopt
            );
            auto addTopic = M::template liftPure<typename M::template Key<B>>(
                [outgoingTopic](typename M::template Key<B> &&x) -> basic::TypedDataWithTopic<typename M::template Key<B>> {
                    return {outgoingTopic, std::move(x)};
                }
            );
            r.registerAction(prefix+"/addTopic", addTopic);
            auto sink = MultiTransportBroadcastPublisherManagingUtils<R>::template oneBroadcastPublisherWithProtocol<
                OutgoingProtocolWrapper
                , typename M::template Key<B>
            >(
                r, prefix+"/exporter", outgoingSpec, outgoingHook
            );
            r.connect(r.actionAsSource(addTopic), sink);
            auto source = MultiTransportBroadcastListenerManagingUtils<R>::template oneBroadcastListenerWithProtocol<
                IncomingProtocolWrapper
                , typename M::template Key<A>
            >(
                r, prefix+"/importer", incomingSpec, incomingTopic, incomingHook
            );
            pathway(
                r, std::move(source), r.actionAsSink(addTopic)
            );
        }
    };
}}}}

#endif