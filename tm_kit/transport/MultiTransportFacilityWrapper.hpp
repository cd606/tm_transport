#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_FACILITY_WRAPPER_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_FACILITY_WRAPPER_HPP_

#include <tm_kit/transport/MultiTransportRemoteFacility.hpp>
#include <tm_kit/transport/grpc_interop/GrpcServerFacility.hpp>
#include <tm_kit/basic/AppRunnerUtils.hpp>

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
        template <class A, class B>
        static auto addIdentity(
            R &runner 
            , typename R::template FacilitioidConnector<
                A
                , B
            > const &toBeWrapped
            , std::string const &prefix
        ) -> typename R::template FacilitioidConnector<
            typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
            , B
        > {
            if constexpr (std::is_same_v<
                typename DetermineServerSideIdentityForRequest<Env, A>::IdentityType 
                , void
            >) {
                return toBeWrapped;
            } else {
                return basic::AppRunnerUtilComponents<R>::template convertIntoTuple2FacilitioidByDiscardingExtraValue<
                    A
                    , B 
                    , typename DetermineServerSideIdentityForRequest<Env, A>::IdentityType
                >(
                    toBeWrapped
                    , prefix
                );
            }
        }
    private:
        template <class A, class B, typename Enable=void>
        class AddIdentityIfNeeded {
        public:
            static auto work(
                R &runner 
                , typename R::template FacilitioidConnector<
                    A
                    , B
                > const &toBeWrapped
                , std::string const &prefix
            ) -> typename R::template FacilitioidConnector<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
            > {
                return MultiTransportFacilityWrapper<R>::template addIdentity<A,B>(runner, toBeWrapped, prefix);
            }
        };
        template <class X, class A, class B>
        class AddIdentityIfNeeded<
            std::tuple<X,A>
            , B
            , std::enable_if_t<
                std::is_same_v<
                    std::tuple<X, A>
                    , typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                >
            >
        > {
        public:
            static auto work(
                R &runner 
                , typename R::template FacilitioidConnector<
                    std::tuple<X,A>
                    , B
                > const &toBeWrapped
                , std::string const &prefix
            ) -> typename R::template FacilitioidConnector<
                std::tuple<X,A>
                , B
            > {
                return toBeWrapped;
            }
        };
    public:
        template <class A, class B>
        static auto addIdentityIfNeeded(
            R &runner 
            , typename R::template FacilitioidConnector<
                A
                , B
            > const &toBeWrapped
            , std::string const &prefix
        ) {
            return AddIdentityIfNeeded<A,B>::work(runner, toBeWrapped, prefix);
        }
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
            case MultiTransportRemoteFacilityConnectionType::SocketRPC:
                if constexpr (std::is_convertible_v<Env *, socket_rpc::SocketRPCComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        socket_rpc::SocketRPCOnOrderFacility<Env>::template wrapOnOrderFacilityWithoutReply<A,B>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        socket_rpc::SocketRPCOnOrderFacility<Env>::template wrapOnOrderFacility<A,B>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(onOrderFacility)] trying to wrap a facility with socket rpc channel '" << rpcQueueLocator << "', but socket rpc is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::GrpcInterop:
                if constexpr (std::is_convertible_v<Env *, grpc_interop::GrpcInteropComponent *>) {
                    if constexpr (DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                        std::ostringstream errOss;
                        errOss << "[MultiTransportFacilityWrapper::wrap(onOrderFacility)] trying to wrap a facility with grpc interop channel '" << rpcQueueLocator << "', but grpc interop does not support facilities with identity on input";
                        throw std::runtime_error(errOss.str());
                    } else {
                        grpc_interop::GrpcServerFacilityWrapper<M>::template wrapOnOrderFacility<A,B>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(onOrderFacility)] trying to wrap a facility with grpc interop channel '" << rpcQueueLocator << "', but grpc interop is unsupported in the environment";
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
            case MultiTransportRemoteFacilityConnectionType::SocketRPC:
                if constexpr (std::is_convertible_v<Env *, socket_rpc::SocketRPCComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        socket_rpc::SocketRPCOnOrderFacility<Env>::template wrapLocalOnOrderFacilityWithoutReply<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        socket_rpc::SocketRPCOnOrderFacility<Env>::template wrapLocalOnOrderFacility<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(localOnOrderFacility)] trying to wrap a facility with socket rpc channel '" << rpcQueueLocator << "', but socket rpc is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::GrpcInterop:
                if constexpr (std::is_convertible_v<Env *, grpc_interop::GrpcInteropComponent *>) {
                    if constexpr (DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                        std::ostringstream errOss;
                        errOss << "[MultiTransportFacilityWrapper::wrap(localOnOrderFacility)] trying to wrap a facility with grpc interop channel '" << rpcQueueLocator << "', but grpc interop does not support facilities with identity on input";
                        throw std::runtime_error(errOss.str());
                    } else {
                        grpc_interop::GrpcServerFacilityWrapper<M>::template wrapLocalOnOrderFacility<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(localOnOrderFacility)] trying to wrap a facility with grpc interop channel '" << rpcQueueLocator << "', but grpc interop is unsupported in the environment";
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
            case MultiTransportRemoteFacilityConnectionType::SocketRPC:
                if constexpr (std::is_convertible_v<Env *, socket_rpc::SocketRPCComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        socket_rpc::SocketRPCOnOrderFacility<Env>::template wrapOnOrderFacilityWithExternalEffectsWithoutReply<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        socket_rpc::SocketRPCOnOrderFacility<Env>::template wrapOnOrderFacilityWithExternalEffects<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(onOrderFacilityWithExternalEffects)] trying to wrap a facility with socket rpc channel '" << rpcQueueLocator << "', but socket rpc is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::GrpcInterop:
                if constexpr (std::is_convertible_v<Env *, grpc_interop::GrpcInteropComponent *>) {
                    if constexpr (DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                        std::ostringstream errOss;
                        errOss << "[MultiTransportFacilityWrapper::wrap(onOrderFacilityWithExternalEffects)] trying to wrap a facility with grpc interop channel '" << rpcQueueLocator << "', but grpc interop does not support facilities with identity on input";
                        throw std::runtime_error(errOss.str());
                    } else {
                        grpc_interop::GrpcServerFacilityWrapper<M>::template wrapOnOrderFacilityWithExternalEffects<A,B,C>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(onOrderFacilityWithExternalEffects)] trying to wrap a facility with grpc interop channel '" << rpcQueueLocator << "', but grpc interop is unsupported in the environment";
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
            case MultiTransportRemoteFacilityConnectionType::SocketRPC:
                if constexpr (std::is_convertible_v<Env *, socket_rpc::SocketRPCComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        socket_rpc::SocketRPCOnOrderFacility<Env>::template wrapVIEOnOrderFacilityWithoutReply<A,B,C,D>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        socket_rpc::SocketRPCOnOrderFacility<Env>::template wrapVIEOnOrderFacility<A,B,C,D>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(vieOnOrderFacility)] trying to wrap a facility with socket rpc channel '" << rpcQueueLocator << "', but socket rpc is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::GrpcInterop:
                if constexpr (std::is_convertible_v<Env *, grpc_interop::GrpcInteropComponent *>) {
                    if constexpr (DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                        std::ostringstream errOss;
                        errOss << "[MultiTransportFacilityWrapper::wrap(vieOrderFacility)] trying to wrap a facility with grpc interop channel '" << rpcQueueLocator << "', but grpc interop does not support facilities with identity on input";
                        throw std::runtime_error(errOss.str());
                    } else {
                        grpc_interop::GrpcServerFacilityWrapper<M>::template wrapVIEOnOrderFacility<A,B,C,D>(
                            runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(vieOnOrderFacility)] trying to wrap a facility with grpc interop channel '" << rpcQueueLocator << "', but grpc interop is unsupported in the environment";
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
        template <class A, class B, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrap(
            R &runner
            , std::string const &registeredNameForFacilitioid
            , typename R::template FacilitioidConnector<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
            > const &toBeWrapped
            , MultiTransportRemoteFacilityConnectionType rpcConnType
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            switch (rpcConnType) {
            case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        rabbitmq::RabbitMQOnOrderFacility<Env>::template wrapFacilitioidConnectorWithoutReply<A,B>(
                            runner, registeredNameForFacilitioid, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        rabbitmq::RabbitMQOnOrderFacility<Env>::template wrapFacilitioidConnector<A,B>(
                            runner, registeredNameForFacilitioid, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(FacilitioidConnector)] trying to wrap a facility with rabbitmq channel '" << rpcQueueLocator << "', but rabbitmq is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        redis::RedisOnOrderFacility<Env>::template wrapFacilitioidConnectorWithoutReply<A,B>(
                            runner, registeredNameForFacilitioid, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        redis::RedisOnOrderFacility<Env>::template wrapFacilitioidConnector<A,B>(
                            runner, registeredNameForFacilitioid, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(FacilitioidConnector)] trying to wrap a facility with redis channel '" << rpcQueueLocator << "', but redis is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::SocketRPC:
                if constexpr (std::is_convertible_v<Env *, socket_rpc::SocketRPCComponent *>) {
                    if constexpr (Option == MultiTransportFacilityWrapperOption::NoReply) {
                        socket_rpc::SocketRPCOnOrderFacility<Env>::template wrapFacilitioidConnectorWithoutReply<A,B>(
                            runner, registeredNameForFacilitioid, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    } else {
                        socket_rpc::SocketRPCOnOrderFacility<Env>::template wrapFacilitioidConnector<A,B>(
                            runner, registeredNameForFacilitioid, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(FacilitioidConnector)] trying to wrap a facility with socket rpc channel '" << rpcQueueLocator << "', but socket rpc is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            case MultiTransportRemoteFacilityConnectionType::GrpcInterop:
                if constexpr (std::is_convertible_v<Env *, grpc_interop::GrpcInteropComponent *>) {
                    if constexpr (DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                        std::ostringstream errOss;
                        errOss << "[MultiTransportFacilityWrapper::wrap(FacilitioidConnector)] trying to wrap a facility with grpc interop channel '" << rpcQueueLocator << "', but grpc interop does not support facilities with identity on input";
                        throw std::runtime_error(errOss.str());
                    } else {
                        grpc_interop::GrpcServerFacilityWrapper<M>::template wrapFacilitioidConnector<A,B>(
                            runner, registeredNameForFacilitioid, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix
                        );
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportFacilityWrapper::wrap(FacilitiodConnector)] trying to wrap a facility with grpc interop channel '" << rpcQueueLocator << "', but grpc interop is unsupported in the environment";
                    throw std::runtime_error(errOss.str());
                }
                break;
            default:
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(FacilitioidConnector)] Unknown connection type");
                break;
            }
        }
        template <class A, class B, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrapFacilitioidWithOptionalSerialization(
            R &runner
            , std::string const &registeredNameForFacilitioid
            , typename R::template FacilitioidConnector<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
            > const &toBeWrapped
            , MultiTransportRemoteFacilityConnectionType rpcConnType
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            wrap<
                typename basic::AppRunnerUtilComponents<R>::template WrapWithCBORIfNecessarySimple<A>::WrappedType
                , typename basic::AppRunnerUtilComponents<R>::template WrapWithCBORIfNecessarySimple<B>::WrappedType
                , Option
            >(
                runner 
                , registeredNameForFacilitioid
                , basic::AppRunnerUtilComponents<R>::template makeServerSideFacilitioidConnectorSerializable
                    <typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                    ,B>
                    (toBeWrapped, wrapperItemsNamePrefix+"/serialization")
                , rpcConnType
                , rpcQueueLocator
                , wrapperItemsNamePrefix
                , hooks
            );
        }
        template <class A, class B, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrap(
            R &runner
            , std::string const &registeredNameForFacilitioid
            , typename R::template FacilitioidConnector<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
            > const &toBeWrapped
            , std::string const &channelSpec
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            auto parsed = parseMultiTransportRemoteFacilityChannel(channelSpec);
            if (parsed) {
                wrap<A,B,Option>(runner, registeredNameForFacilitioid, toBeWrapped, std::get<0>(*parsed), std::get<1>(*parsed), wrapperItemsNamePrefix, hooks);
            } else {
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(FacilitioidConnector)] unknown channel spec '"+channelSpec+"'");
            }
        }
        template <class A, class B, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static void wrapFacilitioidWithOptionalSerialization(
            R &runner
            , std::string const &registeredNameForFacilitioid
            , typename R::template FacilitioidConnector<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
            > const &toBeWrapped
            , std::string const &channelSpec
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            auto parsed = parseMultiTransportRemoteFacilityChannel(channelSpec);
            if (parsed) {
                wrapFacilitioidWithOptionalSerialization<A,B,Option>(runner, registeredNameForFacilitioid, toBeWrapped, std::get<0>(*parsed), std::get<1>(*parsed), wrapperItemsNamePrefix, hooks);
            } else {
                throw std::runtime_error("[MultiTransportFacilityWrapper::wrap(FacilitioidConnector)] unknown channel spec '"+channelSpec+"'");
            }
        }

        template <class A, class B, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static auto facilityWrapper(
            std::string const &channelSpec
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template FacilityWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B> {
            return [channelSpec,wrapperItemsNamePrefix,hooks](
                R &r
                , std::shared_ptr<typename M::template OnOrderFacility<
                    typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                    , B
                >> const &toBeWrapped
            ) {
                wrap<A,B,Option>(r, toBeWrapped, channelSpec, wrapperItemsNamePrefix, hooks);
            };
        }
        template <class A, class B, class C, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static auto localFacilityWrapper(
            std::string const &channelSpec
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template LocalFacilityWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B,C> {
            return [channelSpec,wrapperItemsNamePrefix,hooks](
                R &r
                , std::shared_ptr<typename M::template LocalOnOrderFacility<
                    typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                    , B, C
                >> const &toBeWrapped
            ) {
                wrap<A,B,C,Option>(r, toBeWrapped, channelSpec, wrapperItemsNamePrefix, hooks);
            };
        }
        template <class A, class B, class C, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static auto facilityWithExternalEffectsWrapper(
            std::string const &channelSpec
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template FacilityWithExternalEffectsWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B,C> {
            return [channelSpec,wrapperItemsNamePrefix,hooks](
                R &r
                , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<
                    typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                    , B, C
                >> const &toBeWrapped
            ) {
                wrap<A,B,C,Option>(r, toBeWrapped, channelSpec, wrapperItemsNamePrefix, hooks);
            };
        }
        template <class A, class B, class C, class D, MultiTransportFacilityWrapperOption Option=MultiTransportFacilityWrapperOption::Default>
        static auto vieFacilityWrapper(
            std::string const &channelSpec
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template VIEFacilityWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B,C,D> {
            return [channelSpec,wrapperItemsNamePrefix,hooks](
                R &r
                , std::shared_ptr<typename M::template VIEOnOrderFacility<
                    typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                    , B, C, D
                >> const &toBeWrapped
            ) {
                wrap<A,B,C,D,Option>(r, toBeWrapped, channelSpec, wrapperItemsNamePrefix, hooks);
            };
        }
    };
                
} } } }

#endif