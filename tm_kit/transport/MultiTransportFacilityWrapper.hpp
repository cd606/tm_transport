#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_FACILITY_WRAPPER_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_FACILITY_WRAPPER_HPP_

#include <tm_kit/transport/MultiTransportRemoteFacility.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    enum class MultiTransportFacilityWrapperOption {
        Default
        , NoReply
    };
    template <class R>
    class MultiTransportFacilityWrapper {
    private:
        using M = typename R::AppType;
        using Env = typename R::EnvironmentType;
    public:
        template <class A, class B, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrap(
            R &runner
            , std::shared_ptr<typename M::template OnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
            >> const &toBeWrapped
            , MultiTransportRemoteFacilityConnectionType rpcConnType
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            switch (rpcConnType) {
            case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        rabbitmq::RabbitMQOnOrderFacility<Env>::template wrapOnOrderFacilityWithoutReply<A,B>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        rabbitmq::RabbitMQOnOrderFacility<Env>::template wrapOnOrderFacility<A,B>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(onOrderFacility)] trying to wrap a facility with rabbitmq channel '" << rpcQueueLocator << "', but rabbitmq is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        redis::RedisOnOrderFacility<Env>::template wrapOnOrderFacilityWithoutReply<A,B>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        redis::RedisOnOrderFacility<Env>::template wrapOnOrderFacility<A,B>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(onOrderFacility)] trying to wrap a facility with redis channel '" << rpcQueueLocator << "', but redis is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            default:
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(onOrderFacility)] Unknown connection type");
                break;
            }
        }
        template <class A, class B, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrap(
            R &runner
            , std::shared_ptr<typename M::template OnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
            >> const &toBeWrapped
            , std::string const &channelSpec
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            auto parsed = parseMultiTransportRemoteFacilityChannel(channelSpec);
            if (parsed) {
                wrap<A,B,Option>(runner, toBeWrapped, std::get<0>(*parsed), std::get<1>(*parsed), wrapperItemsNamePrefix, hooks);
            } else {
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(onOrderFacility)] unknown channel spec '"+channelSpec+"'");
            }
        }
        template <class A, class B, class C, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrap(
            R &runner
            , std::shared_ptr<typename M::template LocalOnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
                , C
            >> const &toBeWrapped
            , MultiTransportRemoteFacilityConnectionType rpcConnType
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            switch (rpcConnType) {
            case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        rabbitmq::RabbitMQOnOrderFacility<Env>::template wrapLocalOnOrderFacilityWithoutReply<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        rabbitmq::RabbitMQOnOrderFacility<Env>::template wrapLocalOnOrderFacility<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(localOnOrderFacility)] trying to wrap a facility with rabbitmq channel '" << rpcQueueLocator << "', but rabbitmq is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        redis::RedisOnOrderFacility<Env>::template wrapLocalOnOrderFacilityWithoutReply<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        redis::RedisOnOrderFacility<Env>::template wrapLocalOnOrderFacility<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(localOnOrderFacility)] trying to wrap a facility with redis channel '" << rpcQueueLocator << "', but redis is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            default:
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(localOnOrderFacility)] Unknown connection type");
                break;
            }
        }
        template <class A, class B, class C, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrap(
            R &runner
            , std::shared_ptr<typename M::template LocalOnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
                , C
            >> const &toBeWrapped
            , std::string const &channelSpec
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            auto parsed = parseMultiTransportRemoteFacilityChannel(channelSpec);
            if (parsed) {
                wrap<A,B,C,Option>(runner, toBeWrapped, std::get<0>(*parsed), std::get<1>(*parsed), wrapperItemsNamePrefix, hooks);
            } else {
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(localOnOrderFacility)] unknown channel spec '"+channelSpec+"'");
            }
        }
        template <class A, class B, class C, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrap(
            R &runner
            , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
                , C
            >> const &toBeWrapped
            , MultiTransportRemoteFacilityConnectionType rpcConnType
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            switch (rpcConnType) {
            case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        rabbitmq::RabbitMQOnOrderFacility<Env>::template wrapOnOrderFacilityWithExternalEffectsWithoutReply<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        rabbitmq::RabbitMQOnOrderFacility<Env>::template wrapOnOrderFacilityWithExternalEffects<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(onOrderFacilityWithExternalEffects)] trying to wrap a facility with rabbitmq channel '" << rpcQueueLocator << "', but rabbitmq is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        redis::RedisOnOrderFacility<Env>::template wrapOnOrderFacilityWithExternalEffectsWithoutReply<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        redis::RedisOnOrderFacility<Env>::template wrapOnOrderFacilityWithExternalEffects<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(onOrderFacilityWithExternalEffects)] trying to wrap a facility with redis channel '" << rpcQueueLocator << "', but redis is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            default:
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(onOrderFacilityWithExternalEffects)] Unknown connection type");
                break;
            }
        }
        template <class A, class B, class C, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrap(
            R &runner
            , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
                , C
            >> const &toBeWrapped
            , std::string const &channelSpec
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            auto parsed = parseMultiTransportRemoteFacilityChannel(channelSpec);
            if (parsed) {
                wrap<A,B,C,Option>(runner, toBeWrapped, std::get<0>(*parsed), std::get<1>(*parsed), wrapperItemsNamePrefix, hooks);
            } else {
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(onOrderFacilityWithExternalEffects)] unknown channel spec '"+channelSpec+"'");
            }
        }
        template <class A, class B, class C, class D, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrap(
            R &runner
            , std::shared_ptr<typename M::template VIEOnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
                , C
                , D
            >> const &toBeWrapped
            , MultiTransportRemoteFacilityConnectionType rpcConnType
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            switch (rpcConnType) {
            case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        rabbitmq::RabbitMQOnOrderFacility<Env>::template wrapVIEOnOrderFacilityWithoutReply<A,B,C,D>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        rabbitmq::RabbitMQOnOrderFacility<Env>::template wrapVIEOnOrderFacility<A,B,C,D>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(vieOnOrderFacility)] trying to wrap a facility with rabbitmq channel '" << rpcQueueLocator << "', but rabbitmq is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        redis::RedisOnOrderFacility<Env>::template wrapVIEOnOrderFacilityWithoutReply<A,B,C,D>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        redis::RedisOnOrderFacility<Env>::template wrapVIEOnOrderFacility<A,B,C,D>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(vieOnOrderFacility)] trying to wrap a facility with redis channel '" << rpcQueueLocator << "', but redis is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            default:
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(vieOnOrderFacility)] Unknown connection type");
                break;
            }
        }
        template <class A, class B, class C, class D, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrap(
            R &runner
            , std::shared_ptr<typename M::template VIEOnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
                , C
                , D
            >> const &toBeWrapped
            , std::string const &channelSpec
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            auto parsed = parseMultiTransportRemoteFacilityChannel(channelSpec);
            if (parsed) {
                wrap<A,B,C,D,Option>(runner, toBeWrapped, std::get<0>(*parsed), std::get<1>(*parsed), wrapperItemsNamePrefix, hooks);
            } else {
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(vieOnOrderFacility)] unknown channel spec '"+channelSpec+"'");
            }
        }
    };
                
} } } }

#endif