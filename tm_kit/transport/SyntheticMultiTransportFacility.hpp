#ifndef TM_KIT_TRANSPORT_SYNTHETIC_MULTI_TRANSPORT_FACILITY_HPP_
#define TM_KIT_TRANSPORT_SYNTHETIC_MULTI_TRANSPORT_FACILITY_HPP_

#include <tm_kit/infra/AppClassifier.hpp>

#include <tm_kit/basic/AppRunnerUtils.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/EncodeKeyThroughProxy.hpp>

#include <tm_kit/transport/MultiTransportBroadcastListenerManagingUtils.hpp>
#include <tm_kit/transport/MultiTransportBroadcastPublisherManagingUtils.hpp>
#include <tm_kit/transport/AbstractHookFactoryComponent.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>

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
            , bool MultiCallback
        >
        static auto client(
            R &r 
            , std::string const &prefix
            , std::string const &outgoingSpec 
            , std::string const &outgoingTopic
            , std::string const &incomingSpec 
            , std::optional<std::string> const &incomingTopic
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
            if constexpr (DetermineClientSideIdentityForRequest<Env, A>::HasIdentity) {
                auto addTopic = M::template kleisli<typename M::template Key<A>>(
                    [outgoingTopic](typename M::template InnerData<typename M::template Key<A>> &&x) -> typename M::template Data<basic::ByteDataWithTopic> {
                        auto ret = basic::WrapFacilitioidConnectorForSerializationHelpers::encodeRawWithTopic<
                            OutgoingProtocolWrapper, typename M::template Key<A>
                        >(
                            basic::TypedDataWithTopic<typename M::template Key<A>> {
                                outgoingTopic, std::move(x.timedData.value)
                            }
                        );
                        ret.content = static_cast<
                            typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *
                        >(x.environment)->attach_identity(basic::ByteData {std::move(ret.content)}).content;
                        return M::template pureInnerData<basic::ByteDataWithTopic>(x.environment, std::move(ret));
                    }
                );
                r.registerAction(prefix+"/addTopic", addTopic);
                auto sink = MultiTransportBroadcastPublisherManagingUtils<R>::oneByteDataBroadcastPublisher(
                    r, prefix+"/exporter", outgoingSpec, outgoingHook
                );
                r.connect(r.actionAsSource(addTopic), sink);
                auto source = MultiTransportBroadcastListenerManagingUtils<R>::oneByteDataBroadcastListener(
                    r, prefix+"/importer", incomingSpec, incomingTopic, incomingHook
                );
                auto decode = M::template kleisli<basic::ByteDataWithTopic>(
                    [](typename M::template InnerData<basic::ByteDataWithTopic> &&x) 
                        -> typename M::template Data<typename M::template Key<
                            std::conditional_t<MultiCallback, std::tuple<B,bool>, B>
                        >>
                    {
                        auto y = static_cast<
                            typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *
                        >(x.environment)->process_incoming_data(basic::ByteData {std::move(x.timedData.value.content)});
                        if (!y) {
                            return std::nullopt;
                        }
                        typename M::template Key<
                            std::conditional_t<MultiCallback, std::tuple<B,bool>, B>
                        > k;
                        if (!basic::WrapFacilitioidConnectorForSerializationHelpers::decodeRaw<
                            IncomingProtocolWrapper
                            , typename M::template Key<
                                std::conditional_t<MultiCallback, std::tuple<B,bool>, B>
                            >
                        >(k, *y)) {
                            return std::nullopt;
                        }
                        return M::template pureInnerData<typename M::template Key<
                            std::conditional_t<MultiCallback, std::tuple<B,bool>, B>
                        >>(x.environment, std::move(k));
                    }
                );
                r.registerAction(prefix+"/decode", decode);
                return basic::AppRunnerUtilComponents<R>::template syntheticRemoteFacility<A,B,MultiCallback>(
                    prefix
                    , r.sinkAsSinkoid(r.actionAsSink(addTopic))
                    , r.sourceAsSourceoid(r.execute(decode, std::move(source)))
                );
            } else {
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
                    , typename M::template Key<
                        std::conditional_t<MultiCallback, std::tuple<B,bool>, B>
                    >
                >(
                    r, prefix+"/importer", incomingSpec, incomingTopic, incomingHook
                );
                return basic::AppRunnerUtilComponents<R>::template syntheticRemoteFacility<A,B,MultiCallback>(
                    prefix
                    , r.sinkAsSinkoid(r.actionAsSink(addTopic))
                    , r.sourceAsSourceoid(std::move(source))
                );
            }
        }

        template <
            template<class... Xs> class IncomingProtocolWrapper
            , template<class... Xs> class OutgoingProtocolWrapper
            , class A 
            , class B 
            , bool MultiCallback = false
        >
        static void server(
            R &r 
            , std::string const &prefix
            , std::string const &incomingSpec 
            , std::optional<std::string> const &incomingTopic
            , std::string const &outgoingSpec 
            , std::string const &outgoingTopic
            , typename R::template FacilitioidConnector<
                typename DetermineServerSideIdentityForRequest<Env,A>::FullRequestType
                , B
            > const &facility
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
            if constexpr (DetermineServerSideIdentityForRequest<Env,A>::HasIdentity) {
                using A1 = typename DetermineServerSideIdentityForRequest<Env,A>::FullRequestType;
                auto createOutgoingKey = M::template kleisli<typename M::template KeyedData<A1,B>>(
                    [outgoingTopic](typename M::template InnerData<typename M::template KeyedData<A1,B>> &&x) 
                        -> typename M::template Data<basic::ByteDataWithTopic> {
                        basic::ByteDataWithTopic b;
                        if constexpr (MultiCallback) {
                            b = basic::WrapFacilitioidConnectorForSerializationHelpers::encodeRawWithTopic<
                                OutgoingProtocolWrapper
                                , typename M::template Key<std::tuple<B,bool>>
                            >(basic::TypedDataWithTopic<typename M::template Key<std::tuple<B,bool>>> {
                                outgoingTopic 
                                , {x.timedData.value.key.id(), {std::move(x.timedData.value.data), x.timedData.finalFlag}}
                            });
                        } else {
                            b = basic::WrapFacilitioidConnectorForSerializationHelpers::encodeRawWithTopic<
                                OutgoingProtocolWrapper
                                , typename M::template Key<B>
                            >(basic::TypedDataWithTopic<typename M::template Key<B>> {
                                outgoingTopic 
                                , {x.timedData.value.key.id(), std::move(x.timedData.value.data)}
                            });
                        }
                        b.content = static_cast<
                            typename DetermineServerSideIdentityForRequest<Env, A>::ComponentType *
                        >(x.environment)->process_outgoing_data(
                            std::get<0>(x.timedData.value.key.key()), basic::ByteData {std::move(b.content)}
                        ).content;
                        return M::template pureInnerData<basic::ByteDataWithTopic>(
                            x.environment, std::move(b)
                        );
                    }                
                );
                r.registerAction(prefix+"/createOutgoingKey", createOutgoingKey);
                auto sink = MultiTransportBroadcastPublisherManagingUtils<R>::oneByteDataBroadcastPublisher(
                    r, prefix+"/exporter", outgoingSpec, outgoingHook
                );
                r.connect(r.actionAsSource(createOutgoingKey), sink);
                auto source = MultiTransportBroadcastListenerManagingUtils<R>::oneByteDataBroadcastListener(
                    r, prefix+"/importer", incomingSpec, incomingTopic, incomingHook
                );
                auto decode = M::template kleisli<basic::ByteDataWithTopic>(
                    [](typename M::template InnerData<basic::ByteDataWithTopic> &&x) -> typename M::template Data<typename M::template Key<A1>> {
                        auto y = static_cast<
                            typename DetermineServerSideIdentityForRequest<Env, A>::ComponentType *
                        >(x.environment)->check_identity(basic::ByteData {std::move(x.timedData.value.content)});
                        if (!y) {
                            return std::nullopt;
                        }
                        typename M::template Key<A> k;
                        if (!basic::WrapFacilitioidConnectorForSerializationHelpers::decodeRaw<
                            IncomingProtocolWrapper
                            , typename M::template Key<A>
                        >(k, std::get<1>(*y))) {
                            return std::nullopt;
                        }
                        return M::template pureInnerData<typename M::template Key<A1>>(x.environment, typename M::template Key<A1> {
                            k.id(), {std::get<0>(*y), k.key()}
                        });
                    }
                );
                r.registerAction(prefix+"/decode", decode);
                facility(
                    r, r.execute(decode, std::move(source)), r.actionAsSink(createOutgoingKey)
                );
            } else {
                auto createOutgoingKey = M::template kleisli<typename M::template KeyedData<A,B>>(
                    [outgoingTopic](typename M::template InnerData<typename M::template KeyedData<A,B>> &&x) 
                        -> typename M::template Data<basic::TypedDataWithTopic<typename M::template Key<
                            std::conditional_t<MultiCallback, std::tuple<B,bool>, B>
                        >>> {
                        if constexpr (MultiCallback) {
                            return typename M::template InnerData<basic::TypedDataWithTopic<typename M::template Key<std::tuple<B,bool>>>> {
                                x.environment
                                , {
                                    x.environment->resolveTime()
                                    , {
                                        outgoingTopic, {x.timedData.value.key.id(), {std::move(x.timedData.value.data), x.timedData.finalFlag}}
                                    }
                                    , false
                                }
                            };
                        } else {
                            return typename M::template InnerData<basic::TypedDataWithTopic<typename M::template Key<B>>> {
                                x.environment
                                , {
                                    x.environment->resolveTime()
                                    , {
                                        outgoingTopic, {x.timedData.value.key.id(), std::move(x.timedData.value.data)}
                                    }
                                    , false
                                }
                            };
                        }
                    }                
                );
                r.registerAction(prefix+"/createOutgoingKey", createOutgoingKey);
                auto sink = MultiTransportBroadcastPublisherManagingUtils<R>::template oneBroadcastPublisherWithProtocol<
                    OutgoingProtocolWrapper
                    , typename M::template Key<std::conditional_t<MultiCallback,std::tuple<B,bool>,B>>
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
        }
    };
}}}}

#endif