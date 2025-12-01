#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/TraceNodesComponent.hpp>

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/ProtoInterop.hpp>

#include <tm_kit/transport/multicast/MulticastComponent.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQOnOrderFacility.hpp>
#include <tm_kit/transport/redis/RedisComponent.hpp>
#include <tm_kit/transport/redis/RedisOnOrderFacility.hpp>
#include <tm_kit/transport/nats/NATSComponent.hpp>
#include <tm_kit/transport/nats/NATSOnOrderFacility.hpp>
#include <tm_kit/transport/socket_rpc/SocketRPCComponent.hpp>
#include <tm_kit/transport/singlecast/SinglecastComponent.hpp>
#include <tm_kit/transport/socket_rpc/SocketRPCOnOrderFacility.hpp>
#include <tm_kit/transport/grpc_interop/GrpcInteropComponent.hpp>
#include <tm_kit/transport/grpc_interop/GrpcClientFacility.hpp>
#include <tm_kit/transport/json_rest/JsonRESTClientFacility.hpp>
#include <tm_kit/transport/websocket/WebSocketClientFacility.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>
#include <tm_kit/transport/MultiTransportBroadcastListenerManagingUtils.hpp>

#include <type_traits>
#include <regex>

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    enum class MultiTransportRemoteFacilityActionType {
        Register
        , Deregister
    };
    inline const std::array<std::string,2> MULTI_TRANSPORT_REMOTE_FACILITY_ACTION_TYPE_STR = {
        "register"
        , "deregister"
    };
    enum class MultiTransportRemoteFacilityConnectionType {
        RabbitMQ
        , Redis
        , NATS
        , SocketRPC
        , GrpcInterop
        , JsonREST
        , WebSocket
    };
    inline const std::array<std::string,7> MULTI_TRANSPORT_REMOTE_FACILITY_CONNECTION_TYPE_STR = {
        "rabbitmq"
        , "redis"
        , "nats"
        , "socket_rpc"
        , "grpc_interop"
        , "json_rest"
        , "websocket"
    };

    inline auto parseMultiTransportRemoteFacilityChannel(std::string const &s) 
        -> std::optional<std::tuple<MultiTransportRemoteFacilityConnectionType, ConnectionLocator>>
    {
        size_t ii = 0;
        for (auto const &item : MULTI_TRANSPORT_REMOTE_FACILITY_CONNECTION_TYPE_STR) {
            if (boost::starts_with(s, item+"://")) {
                try {
                    auto locator = ConnectionLocator::parse(s.substr(item.length()+3));
                    return std::tuple<MultiTransportRemoteFacilityConnectionType, ConnectionLocator> {
                        static_cast<MultiTransportRemoteFacilityConnectionType>(ii)
                        , locator
                    };
                } catch (ConnectionLocatorParseError const &) {
                    return std::nullopt;
                }
            }
            ++ii;
        }
        return std::nullopt;
    }
    
    struct MultiTransportRemoteFacilityAction {
        MultiTransportRemoteFacilityActionType actionType;
        MultiTransportRemoteFacilityConnectionType connectionType;
        ConnectionLocator connectionLocator;
        std::string description;
    };

    using MultiTransportRemoteFacilityActionResult = 
        std::tuple<MultiTransportRemoteFacilityAction, bool>; //the bool part means whether it is handled

    enum class MultiTransportRemoteFacilityDispatchStrategy {
        Random
        , Designated
    };

    template <class Env, class A, class B
        , class Identity=typename DetermineClientSideIdentityForRequest<Env,A>::IdentityType
        , MultiTransportRemoteFacilityDispatchStrategy DispatchStrategy 
            = MultiTransportRemoteFacilityDispatchStrategy::Random
    >
    class MultiTransportRemoteFacility final :
        public std::conditional_t<
            DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random
            , typename infra::RealTimeApp<Env>::template AbstractIntegratedVIEOnOrderFacility<
                A
                , B 
                , MultiTransportRemoteFacilityAction
                , std::size_t
            >
            , std::conditional_t<
                DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated
                , typename infra::RealTimeApp<Env>::template AbstractIntegratedVIEOnOrderFacility<
                    std::tuple<ConnectionLocator, A>
                    , B 
                    , MultiTransportRemoteFacilityAction
                    , MultiTransportRemoteFacilityActionResult
                >
                , void
            >
        >
    {
    private:
        using M = infra::RealTimeApp<Env>;
    public:
        using Input = std::conditional_t<
            DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random
            , A
            , std::conditional_t<
                DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated
                , std::tuple<ConnectionLocator, A>
                , void
            >
        >;
        using Output = B;
    private:
        using Parent = std::conditional_t<
            DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random
            , typename M::template AbstractIntegratedVIEOnOrderFacility<
                A
                , B 
                , MultiTransportRemoteFacilityAction
                , std::size_t
            >
            , std::conditional_t<
                DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated
                , typename M::template AbstractIntegratedVIEOnOrderFacility<
                    std::tuple<ConnectionLocator, A>
                    , B 
                    , MultiTransportRemoteFacilityAction
                    , MultiTransportRemoteFacilityActionResult
                >
                , void
            >
        >;
        using FacilityParent = typename M::template AbstractOnOrderFacility<
            Input
            , Output
        >;
        using ExporterParent = typename M::template AbstractExporter<
            MultiTransportRemoteFacilityAction
        >;
        using ImporterParent = typename M::template AbstractImporter<
            std::conditional_t<
                DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random
                , std::size_t
                , MultiTransportRemoteFacilityActionResult
            >
        >;
        using RequestSender = std::function<void(std::string const &, A &&)>;

        using SenderMap = std::conditional_t<
            DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated
            , std::unordered_map<ConnectionLocator, RequestSender *>
            , bool
        >;

        std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> hookPairFactory_; 
        std::vector<std::tuple<ConnectionLocator, std::unique_ptr<RequestSender>>> underlyingSenders_;
        SenderMap senderMap_;
        std::unordered_map<ConnectionLocator, uint32_t> clientNumberRecord_;
        std::mutex mutex_;

        std::tuple<bool, std::size_t> registerFacility(Env *env, MultiTransportRemoteFacilityConnectionType connType, ConnectionLocator const &locator, std::string const &description) {
            std::size_t newSize = 0;
            switch (connType) {
            case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    auto *component = static_cast<rabbitmq::RabbitMQComponent *>(env);
                    try {
                        uint32_t clientNumber = 0;
                        auto rawReq = component->rabbitmq_setRPCQueueClient(
                            locator
                            , [this,env](bool isFinal, basic::ByteDataWithID &&data) {
                                if constexpr (std::is_same_v<Identity, void>) {
                                    Output o;
                                    auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, data.content);
                                    if (!result) {
                                        return;
                                    }
                                    this->FacilityParent::publish(
                                        env
                                        , typename M::template Key<Output> {
                                            Env::id_from_string(data.id)
                                            , std::move(o)
                                        }
                                        , isFinal
                                    );
                                } else {
                                    auto processRes = static_cast<typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>(env)->process_incoming_data(
                                        basic::ByteData {std::move(data.content)}
                                    );
                                    if (processRes) {
                                        Output o;
                                        auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, processRes->content);
                                        if (!result) {
                                            return;
                                        }
                                        this->FacilityParent::publish(
                                            env
                                            , typename M::template Key<Output> {
                                                Env::id_from_string(data.id)
                                                , std::move(o)
                                            }
                                            , isFinal
                                        );
                                    }
                                }
                            }
                            , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hookPairFactory_(description, locator))
                            , &clientNumber
                        );
                        RequestSender req;
                        if constexpr (std::is_same_v<Identity,void>) {
                            req = [rawReq](std::string const &id, A &&data) {
                                rawReq(basic::ByteDataWithID {
                                    id 
                                    , basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))
                                });
                            };
                        } else {
                            static_assert(std::is_convertible_v<Env *, typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>
                                        , "the client side identity attacher must be present");
                            auto *attacher = static_cast<typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>(env);
                            req = [attacher,rawReq](std::string const &id, A &&data) {
                                rawReq(basic::ByteDataWithID {
                                    id
                                    , attacher->attach_identity({basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))}).content
                                });
                            };
                        }
                        {
                            std::lock_guard<std::mutex> _(mutex_);
                            underlyingSenders_.push_back({locator, std::make_unique<RequestSender>(std::move(req))});
                            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                                senderMap_.insert({locator, std::get<1>(underlyingSenders_.back()).get()});
                            }
                            newSize = underlyingSenders_.size();
                            clientNumberRecord_[locator] = clientNumber;
                        }
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Registered RabbitMQ facility for "
                            << locator;
                        env->log(infra::LogLevel::Info, oss.str());
                    } catch (rabbitmq::RabbitMQComponentException const &ex) {
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Error registering RabbitMQ facility for "
                            << locator
                            << ": " << ex.what();
                        env->log(infra::LogLevel::Error, oss.str());
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up rabbitmq facility for " << locator << ", but rabbitmq is unsupported in the environment";
                    env->log(infra::LogLevel::Warning, errOss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    auto *component = static_cast<redis::RedisComponent *>(env);
                    try {
                        uint32_t clientNumber = 0;
                        auto rawReq = component->redis_setRPCClient(
                            locator
                            , [env]() {
                                return Env::id_to_string(env->new_id());
                            }
                            , [this,env](bool isFinal, basic::ByteDataWithID &&data) {
                                if constexpr (std::is_same_v<Identity, void>) {
                                    Output o;
                                    auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, data.content);
                                    if (!result) {
                                        return;
                                    }
                                    this->FacilityParent::publish(
                                        env
                                        , typename M::template Key<Output> {
                                            Env::id_from_string(data.id)
                                            , std::move(o)
                                        }
                                        , isFinal
                                    );
                                } else {
                                    auto processRes = static_cast<typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>(env)->process_incoming_data(
                                        basic::ByteData {std::move(data.content)}
                                    );
                                    if (processRes) {
                                        Output o;
                                        auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, processRes->content);
                                        if (!result) {
                                            return;
                                        }
                                        this->FacilityParent::publish(
                                            env
                                            , typename M::template Key<Output> {
                                                Env::id_from_string(data.id)
                                                , std::move(o)
                                            }
                                            , isFinal
                                        );
                                    }
                                }
                            }
                            , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hookPairFactory_(description, locator))
                            , &clientNumber
                        );
                        RequestSender req;
                        if constexpr (std::is_same_v<Identity,void>) {
                            req = [rawReq](std::string const &id, A &&data) {
                                rawReq(basic::ByteDataWithID {
                                    id 
                                    , basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))
                                });
                            };
                        } else {
                            static_assert(std::is_convertible_v<Env *, typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>
                                        , "the client side identity attacher must be present");
                            auto *attacher = static_cast<typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>(env);
                            req = [attacher,rawReq](std::string const &id, A &&data) {
                                rawReq(basic::ByteDataWithID {
                                    id
                                    , attacher->attach_identity({basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))}).content
                                });
                            };
                        }
                        {
                            std::lock_guard<std::mutex> _(mutex_);
                            underlyingSenders_.push_back({locator, std::make_unique<RequestSender>(std::move(req))});
                            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                                senderMap_.insert({locator, std::get<1>(underlyingSenders_.back()).get()});
                            }
                            newSize = underlyingSenders_.size();
                            clientNumberRecord_[locator] = clientNumber;
                        }
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Registered Redis facility for "
                            << locator;
                        env->log(infra::LogLevel::Info, oss.str());
                    } catch (redis::RedisComponentException const &ex) {
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Error registering Redis facility for "
                            << locator
                            << ": " << ex.what();
                        env->log(infra::LogLevel::Error, oss.str());
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up redis facility for " << locator << ", but redis is unsupported in the environment";
                    env->log(infra::LogLevel::Warning, errOss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::NATS:
                if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                    auto *component = static_cast<nats::NATSComponent *>(env);
                    try {
                        uint32_t clientNumber = 0;
                        auto rawReq = component->nats_setRPCClient(
                            locator
                            , [this,env](bool isFinal, basic::ByteDataWithID &&data) {
                                if constexpr (std::is_same_v<Identity, void>) {
                                    Output o;
                                    auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, data.content);
                                    if (!result) {
                                        return;
                                    }
                                    this->FacilityParent::publish(
                                        env
                                        , typename M::template Key<Output> {
                                            Env::id_from_string(data.id)
                                            , std::move(o)
                                        }
                                        , isFinal
                                    );
                                } else {
                                    auto processRes = static_cast<typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>(env)->process_incoming_data(
                                        basic::ByteData {std::move(data.content)}
                                    );
                                    if (processRes) {
                                        Output o;
                                        auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, processRes->content);
                                        if (!result) {
                                            return;
                                        }
                                        this->FacilityParent::publish(
                                            env
                                            , typename M::template Key<Output> {
                                                Env::id_from_string(data.id)
                                                , std::move(o)
                                            }
                                            , isFinal
                                        );
                                    }
                                }
                            }
                            , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hookPairFactory_(description, locator))
                            , &clientNumber
                        );
                        RequestSender req;
                        if constexpr (std::is_same_v<Identity,void>) {
                            req = [rawReq](std::string const &id, A &&data) {
                                rawReq(basic::ByteDataWithID {
                                    id 
                                    , basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))
                                });
                            };
                        } else {
                            static_assert(std::is_convertible_v<Env *, typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>
                                        , "the client side identity attacher must be present");
                            auto *attacher = static_cast<typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>(env);
                            req = [attacher,rawReq](std::string const &id, A &&data) {
                                rawReq(basic::ByteDataWithID {
                                    id
                                    , attacher->attach_identity({basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))}).content
                                });
                            };
                        }
                        {
                            std::lock_guard<std::mutex> _(mutex_);
                            underlyingSenders_.push_back({locator, std::make_unique<RequestSender>(std::move(req))});
                            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                                senderMap_.insert({locator, std::get<1>(underlyingSenders_.back()).get()});
                            }
                            newSize = underlyingSenders_.size();
                            clientNumberRecord_[locator] = clientNumber;
                        }
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Registered NATS facility for "
                            << locator;
                        env->log(infra::LogLevel::Info, oss.str());
                    } catch (nats::NATSComponentException const &ex) {
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Error registering NATS facility for "
                            << locator
                            << ": " << ex.what();
                        env->log(infra::LogLevel::Error, oss.str());
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up nats facility for " << locator << ", but nats is unsupported in the environment";
                    env->log(infra::LogLevel::Warning, errOss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::SocketRPC:
                if constexpr (std::is_convertible_v<Env *, socket_rpc::SocketRPCComponent *>) {
                    auto *component = static_cast<socket_rpc::SocketRPCComponent *>(env);
                    try {
                        auto rawReq = component->socket_rpc_setRPCClient(
                            locator
                            , [this,env](bool isFinal, basic::ByteDataWithID &&data) {
                                if constexpr (std::is_same_v<Identity, void>) {
                                    Output o;
                                    auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, data.content);
                                    if (!result) {
                                        return;
                                    }
                                    this->FacilityParent::publish(
                                        env
                                        , typename M::template Key<Output> {
                                            Env::id_from_string(data.id)
                                            , std::move(o)
                                        }
                                        , isFinal
                                    );
                                } else {
                                    auto processRes = static_cast<typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>(env)->process_incoming_data(
                                        basic::ByteData {std::move(data.content)}
                                    );
                                    if (processRes) {
                                        Output o;
                                        auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, processRes->content);
                                        if (!result) {
                                            return;
                                        }
                                        this->FacilityParent::publish(
                                            env
                                            , typename M::template Key<Output> {
                                                Env::id_from_string(data.id)
                                                , std::move(o)
                                            }
                                            , isFinal
                                        );
                                    }
                                }
                            }
                            , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hookPairFactory_(description, locator))
                        );
                        RequestSender req;
                        if constexpr (std::is_same_v<Identity,void>) {
                            req = [rawReq](std::string const &id, A &&data) {
                                rawReq(basic::ByteDataWithID {
                                    id 
                                    , basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))
                                });
                            };
                        } else {
                            static_assert(std::is_convertible_v<Env *, typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>
                                        , "the client side identity attacher must be present");
                            auto *attacher = static_cast<typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>(env);
                            req = [attacher,rawReq](std::string const &id, A &&data) {
                                rawReq(basic::ByteDataWithID {
                                    id
                                    , attacher->attach_identity({basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))}).content
                                });
                            };
                        }
                        {
                            std::lock_guard<std::mutex> _(mutex_);
                            underlyingSenders_.push_back({locator, std::make_unique<RequestSender>(std::move(req))});
                            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                                senderMap_.insert({locator, std::get<1>(underlyingSenders_.back()).get()});
                            }
                            newSize = underlyingSenders_.size();
                        }
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Registered Socket RPC facility for "
                            << locator;
                        env->log(infra::LogLevel::Info, oss.str());
                    } catch (socket_rpc::SocketRPCComponentException const &ex) {
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Error registering Socket RPC facility for "
                            << locator
                            << ": " << ex.what();
                        env->log(infra::LogLevel::Error, oss.str());
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up socket rpc facility for " << locator << ", but socket rpc is unsupported in the environment";
                    env->log(infra::LogLevel::Warning, errOss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::GrpcInterop:
                if constexpr (std::is_convertible_v<Env *, grpc_interop::GrpcInteropComponent *>) {
                    if constexpr (std::is_same_v<Identity, void> || std::is_same_v<Identity, std::string>) {
                        //in grpc, if identity is std::string, it is treated as if there is no identity
                        //on client side
                        if constexpr (
                            basic::bytedata_utils::ProtobufStyleSerializableChecker<A>::IsProtobufStyleSerializable()
                            &&
                            basic::bytedata_utils::ProtobufStyleSerializableChecker<B>::IsProtobufStyleSerializable()
                        ) {
                            auto *component = static_cast<grpc_interop::GrpcInteropComponent *>(env);
                            try {
                                auto rawReq = component->grpc_interop_setRPCClient(
                                    locator
                                    , [this,env](bool isFinal, std::string const &id, std::optional<std::string> &&data) {
                                        if (data) {
                                            Output o;
                                            auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, *data);
                                            if (!result) {
                                                return;
                                            }
                                            this->FacilityParent::publish(
                                                env
                                                , typename M::template Key<Output> {
                                                    Env::id_from_string(id)
                                                    , std::move(o)
                                                }
                                                , isFinal
                                            );
                                        } else {
                                            if (isFinal) {
                                                this->FacilityParent::markEndHandlingRequest(
                                                    Env::id_from_string(id)
                                                );
                                            }
                                        }
                                    }
                                    , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hookPairFactory_(description, locator))
                                );
                                auto req = [rawReq](std::string const &id, A &&data) {
                                    rawReq(basic::ByteDataWithID {
                                        id 
                                        , basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))
                                    });
                                };
                                {
                                    std::lock_guard<std::mutex> _(mutex_);
                                    underlyingSenders_.push_back({locator, std::make_unique<RequestSender>(std::move(req))});
                                    if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                                        senderMap_.insert({locator, std::get<1>(underlyingSenders_.back()).get()});
                                    }
                                    newSize = underlyingSenders_.size();
                                }
                                std::ostringstream oss;
                                oss << "[MultiTransportRemoteFacility::registerFacility] Registered grpc interop facility for "
                                    << locator;
                                env->log(infra::LogLevel::Info, oss.str());
                            } catch (grpc_interop::GrpcInteropComponentException const &ex) {
                                std::ostringstream oss;
                                oss << "[MultiTransportRemoteFacility::registerFacility] Error registering grpc interop facility for "
                                    << locator
                                    << ": " << ex.what();
                                env->log(infra::LogLevel::Error, oss.str());
                            }
                        } else if constexpr(basic::proto_interop::ProtoWrappable<A>::value && basic::proto_interop::ProtoWrappable<B>::value) {
                            auto *component = static_cast<grpc_interop::GrpcInteropComponent *>(env);
                            try {
                                auto rawReq = component->grpc_interop_setRPCClient(
                                    locator
                                    , [this,env](bool isFinal, std::string const &id, std::optional<std::string> &&data) {
                                        if (data) {
                                            Output o;
                                            auto result = basic::proto_interop::Proto<Output>::runDeserialize(o, *data);
                                            if (!result) {
                                                return;
                                            }
                                            this->FacilityParent::publish(
                                                env
                                                , typename M::template Key<Output> {
                                                    Env::id_from_string(id)
                                                    , std::move(o)
                                                }
                                                , isFinal
                                            );
                                        } else {
                                            if (isFinal) {
                                                this->FacilityParent::markEndHandlingRequest(
                                                    Env::id_from_string(id)
                                                );
                                            }
                                        }
                                    }
                                    , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hookPairFactory_(description, locator))
                                );
                                auto req = [rawReq](std::string const &id, A &&data) {
                                    rawReq(basic::ByteDataWithID {
                                        id 
                                        , basic::proto_interop::Proto<A>::runSerializeIntoValue(data)
                                    });
                                };
                                {
                                    std::lock_guard<std::mutex> _(mutex_);
                                    underlyingSenders_.push_back({locator, std::make_unique<RequestSender>(std::move(req))});
                                    if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                                        senderMap_.insert({locator, std::get<1>(underlyingSenders_.back()).get()});
                                    }
                                    newSize = underlyingSenders_.size();
                                }
                                std::ostringstream oss;
                                oss << "[MultiTransportRemoteFacility::registerFacility] Registered grpc interop facility for "
                                    << locator;
                                env->log(infra::LogLevel::Info, oss.str());
                            } catch (grpc_interop::GrpcInteropComponentException const &ex) {
                                std::ostringstream oss;
                                oss << "[MultiTransportRemoteFacility::registerFacility] Error registering grpc interop facility for "
                                    << locator
                                    << ": " << ex.what();
                                env->log(infra::LogLevel::Error, oss.str());
                            }
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up grpc interop rpc facility for " << locator << ", but grpc interop requires the data structures to be protobuf compatible";
                            env->log(infra::LogLevel::Warning, errOss.str());
                        }

                    } else {
                        std::ostringstream errOss;
                        errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up grpc interop rpc facility for " << locator << ", but grpc interop does not support non-string identities in request";
                        env->log(infra::LogLevel::Warning, errOss.str());
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up grpc interop facility for " << locator << ", but grpc interop is unsupported in the environment";
                    env->log(infra::LogLevel::Warning, errOss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::JsonREST:
                if constexpr (std::is_convertible_v<Env *, json_rest::JsonRESTComponent *>) {
                    if constexpr (std::is_same_v<Identity, void> || std::is_same_v<Identity, std::string>) {
                        //in json rest, if identity is std::string, it is treated as if there is no identity
                        //on client side
                        if constexpr (
                            basic::nlohmann_json_interop::JsonWrappable<A>::value
                            &&
                            basic::nlohmann_json_interop::JsonWrappable<B>::value
                        ) {
                            try {
                                bool useGet = (
                                    (locator.query("use_get", "false") == "true")
                                    && 
                                    basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<A>
                                );
                                bool noRequestResponseWrap = (
                                    locator.query("no_wrap", "false") == "true"
                                );
                                auto req = [this,env,locator,useGet,noRequestResponseWrap](std::string const &id, A &&data) {
                                    nlohmann::json sendData;
                                    std::ostringstream oss;
                                    if constexpr (basic::struct_field_info_utils::IsStructFieldInfoBasedCsvCompatibleStruct<A>) {
                                        if (useGet) {
                                            bool start = true;
                                            basic::struct_field_info_utils::StructFieldInfoBasedSimpleCsvOutput<A, void>
                                                ::outputNameValuePairs(
                                                    data
                                                    , [&start,&oss](std::string const &name, std::string const &value) {
                                                        if (!start) {
                                                            oss << '&';
                                                        }
                                                        json_rest::JsonRESTClientFacilityFactoryUtils::urlEscape(oss, name);
                                                        oss << '=';
                                                        json_rest::JsonRESTClientFacilityFactoryUtils::urlEscape(oss, value);
                                                        start = false;
                                                    }
                                                );
                                        } else {
                                            basic::nlohmann_json_interop::JsonEncoder<A>::write(sendData, (noRequestResponseWrap?std::nullopt:std::optional<std::string> {"request"}), data);
                                        }
                                    } else {
                                        basic::nlohmann_json_interop::JsonEncoder<A>::write(sendData, (noRequestResponseWrap?std::nullopt:std::optional<std::string> {"request"}), data);
                                    }
                                    env->json_rest::JsonRESTComponent::addJsonRESTClient(
                                        (std::is_same_v<B, json_rest::RawStringWithStatus>?locator.addProperty("parse_header","true"):locator)
                                        , (useGet?oss.str():"")
                                        , (useGet?"":sendData.dump())
                                        , [this,env,id,noRequestResponseWrap](unsigned status, std::string &&response, std::unordered_map<std::string,std::string> &&headerFields) mutable {
                                            if constexpr (std::is_same_v<B, json_rest::RawString>) {
                                                this->FacilityParent::publish(
                                                    env
                                                    , typename M::template Key<Output> {
                                                        Env::id_from_string(id)
                                                        , Output {std::move(response)}
                                                    }
                                                    , true
                                                );
                                            } else if constexpr (std::is_same_v<B, json_rest::RawStringWithStatus>) {
                                                this->FacilityParent::publish(
                                                    env
                                                    , typename M::template Key<Output> {
                                                        Env::id_from_string(id)
                                                        , Output {status, std::move(headerFields), std::move(response)}
                                                    }
                                                    , true
                                                );
                                            } else {
                                                nlohmann::json x = nlohmann::json::parse(response);
                                                Output resp;
                                                basic::nlohmann_json_interop::Json<Output *> r(&resp);
                                                if (r.fromNlohmannJson(noRequestResponseWrap?x:x["response"])) {
                                                    this->FacilityParent::publish(
                                                        env
                                                        , typename M::template Key<Output> {
                                                            Env::id_from_string(id)
                                                            , std::move(resp)
                                                        }
                                                        , true
                                                    );
                                                }
                                            }
                                        }
                                    );
                                };
                                {
                                    std::lock_guard<std::mutex> _(mutex_);
                                    underlyingSenders_.push_back({locator, std::make_unique<RequestSender>(std::move(req))});
                                    if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                                        senderMap_.insert({locator, std::get<1>(underlyingSenders_.back()).get()});
                                    }
                                    newSize = underlyingSenders_.size();
                                }
                                std::ostringstream oss;
                                oss << "[MultiTransportRemoteFacility::registerFacility] Registered json rest facility for "
                                    << locator;
                                env->log(infra::LogLevel::Info, oss.str());
                            } catch (json_rest::JsonRESTComponentException const &ex) {
                                std::ostringstream oss;
                                oss << "[MultiTransportRemoteFacility::registerFacility] Error registering json rest facility for "
                                    << locator
                                    << ": " << ex.what();
                                env->log(infra::LogLevel::Error, oss.str());
                            }
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up grpc interop rpc facility for " << locator << ", but grpc interop requires the data structures to be protobuf compatible";
                            env->log(infra::LogLevel::Warning, errOss.str());
                        }
                    } else {
                        std::ostringstream errOss;
                        errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up json rest rpc facility for " << locator << ", but json rest does not support non-string identities in request";
                        env->log(infra::LogLevel::Warning, errOss.str());
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up json rest facility for " << locator << ", but json rest is unsupported in the environment";
                    env->log(infra::LogLevel::Warning, errOss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::WebSocket:
                if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                    auto *component = static_cast<web_socket::WebSocketComponent *>(env);
                    try {
                        uint32_t clientNumber = 0;
                        auto rawReq = component->websocket_setRPCClient(
                            locator
                            , [this,env](bool isFinal, basic::ByteDataWithID &&data) {
                                if constexpr (std::is_same_v<Identity, void>) {
                                    Output o;
                                    auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, data.content);
                                    if (!result) {
                                        return;
                                    }
                                    this->FacilityParent::publish(
                                        env
                                        , typename M::template Key<Output> {
                                            Env::id_from_string(data.id)
                                            , std::move(o)
                                        }
                                        , isFinal
                                    );
                                } else {
                                    auto processRes = static_cast<typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>(env)->process_incoming_data(
                                        basic::ByteData {std::move(data.content)}
                                    );
                                    if (processRes) {
                                        Output o;
                                        auto result = basic::bytedata_utils::RunDeserializer<Output>::applyInPlace(o, processRes->content);
                                        if (!result) {
                                            return;
                                        }
                                        this->FacilityParent::publish(
                                            env
                                            , typename M::template Key<Output> {
                                                Env::id_from_string(data.id)
                                                , std::move(o)
                                            }
                                            , isFinal
                                        );
                                    }
                                }
                            }
                            , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hookPairFactory_(description, locator))
                            , &clientNumber
                        );
                        RequestSender req;
                        if constexpr (std::is_same_v<Identity,void>) {
                            req = [rawReq](std::string const &id, A &&data) {
                                rawReq(basic::ByteDataWithID {
                                    id 
                                    , basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))
                                });
                            };
                        } else {
                            static_assert(std::is_convertible_v<Env *, typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>
                                        , "the client side identity attacher must be present");
                            auto *attacher = static_cast<typename DetermineClientSideIdentityForRequest<Env, A>::ComponentType *>(env);
                            req = [attacher,rawReq](std::string const &id, A &&data) {
                                rawReq(basic::ByteDataWithID {
                                    id
                                    , attacher->attach_identity({basic::SerializationActions<M>::template serializeFunc<A>(std::move(data))}).content
                                });
                            };
                        }
                        {
                            std::lock_guard<std::mutex> _(mutex_);
                            underlyingSenders_.push_back({locator, std::make_unique<RequestSender>(std::move(req))});
                            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                                senderMap_.insert({locator, std::get<1>(underlyingSenders_.back()).get()});
                            }
                            newSize = underlyingSenders_.size();
                            clientNumberRecord_[locator] = clientNumber;
                        }
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Registered web socket facility for "
                            << locator;
                        env->log(infra::LogLevel::Info, oss.str());
                    } catch (web_socket::WebSocketComponentException const &ex) {
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Error registering web socket facility for "
                            << locator
                            << ": " << ex.what();
                        env->log(infra::LogLevel::Error, oss.str());
                    }
                } else {
                    std::ostringstream errOss;
                    errOss << "[MultiTransportRemoteFacility::registerFacility] Trying to set up web socket facility for " << locator << ", but web socket is unsupported in the environment";
                    env->log(infra::LogLevel::Warning, errOss.str());
                }
                return {newSize, true};
                break;
            default:
                return {0, false};
                break;
            }
        }
        std::tuple<std::size_t, bool> deregisterFacility(Env *env, MultiTransportRemoteFacilityConnectionType connType, ConnectionLocator const &locator) {
            std::size_t newSize = 0;
            switch (connType) {
            case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    auto *component = static_cast<rabbitmq::RabbitMQComponent *>(env);
                    uint32_t clientNumber = 0;
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        auto iter = clientNumberRecord_.find(locator);
                        if (iter != clientNumberRecord_.end()) {
                            clientNumber = iter->second;
                            clientNumberRecord_.erase(iter);
                        }
                    }
                    component->rabbitmq_removeRPCQueueClient(locator, clientNumber);
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                            senderMap_.erase(locator);
                        }
                        underlyingSenders_.erase(
                            std::remove_if(
                                underlyingSenders_.begin()
                                , underlyingSenders_.end()
                                , [&locator](auto const &x) {
                                    return (std::get<0>(x) == locator);
                                }
                            )
                            , underlyingSenders_.end()
                        );
                        newSize = underlyingSenders_.size();
                    }
                    std::ostringstream oss;
                    oss << "[MultiTransportRemoteFacility::deregisterFacility] De-registered RabbitMQ facility for "
                        << locator;
                    env->log(infra::LogLevel::Info, oss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    auto *component = static_cast<redis::RedisComponent *>(env);
                    uint32_t clientNumber = 0;
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        auto iter = clientNumberRecord_.find(locator);
                        if (iter != clientNumberRecord_.end()) {
                            clientNumber = iter->second;
                            clientNumberRecord_.erase(iter);
                        }
                    }
                    component->redis_removeRPCClient(locator, clientNumber);
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                            senderMap_.erase(locator);
                        }
                        underlyingSenders_.erase(
                            std::remove_if(
                                underlyingSenders_.begin()
                                , underlyingSenders_.end()
                                , [&locator](auto const &x) {
                                    return (std::get<0>(x) == locator);
                                }
                            )
                            , underlyingSenders_.end()
                        );
                        newSize = underlyingSenders_.size();
                    }
                    std::ostringstream oss;
                    oss << "[MultiTransportRemoteFacility::deregisterFacility] De-registered Redis facility for "
                        << locator;
                    env->log(infra::LogLevel::Info, oss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::NATS:
                if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                    auto *component = static_cast<nats::NATSComponent *>(env);
                    uint32_t clientNumber = 0;
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        auto iter = clientNumberRecord_.find(locator);
                        if (iter != clientNumberRecord_.end()) {
                            clientNumber = iter->second;
                            clientNumberRecord_.erase(iter);
                        }
                    }
                    component->nats_removeRPCClient(locator, clientNumber);
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                            senderMap_.erase(locator);
                        }
                        underlyingSenders_.erase(
                            std::remove_if(
                                underlyingSenders_.begin()
                                , underlyingSenders_.end()
                                , [&locator](auto const &x) {
                                    return (std::get<0>(x) == locator);
                                }
                            )
                            , underlyingSenders_.end()
                        );
                        newSize = underlyingSenders_.size();
                    }
                    std::ostringstream oss;
                    oss << "[MultiTransportRemoteFacility::deregisterFacility] De-registered NATS facility for "
                        << locator;
                    env->log(infra::LogLevel::Info, oss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::SocketRPC:
                if constexpr (std::is_convertible_v<Env *, socket_rpc::SocketRPCComponent *>) {
                    auto *component = static_cast<socket_rpc::SocketRPCComponent *>(env);
                    component->socket_rpc_removeRPCClient(locator);
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                            senderMap_.erase(locator);
                        }
                        underlyingSenders_.erase(
                            std::remove_if(
                                underlyingSenders_.begin()
                                , underlyingSenders_.end()
                                , [&locator](auto const &x) {
                                    return (std::get<0>(x) == locator);
                                }
                            )
                            , underlyingSenders_.end()
                        );
                        newSize = underlyingSenders_.size();
                    }
                    std::ostringstream oss;
                    oss << "[MultiTransportRemoteFacility::deregisterFacility] De-registered Socket RPC facility for "
                        << locator;
                    env->log(infra::LogLevel::Info, oss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::GrpcInterop:
                if constexpr (std::is_convertible_v<Env *, grpc_interop::GrpcInteropComponent *>) {
                    auto *component = static_cast<grpc_interop::GrpcInteropComponent *>(env);
                    component->grpc_interop_removeRPCClient(locator);
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                            senderMap_.erase(locator);
                        }
                        underlyingSenders_.erase(
                            std::remove_if(
                                underlyingSenders_.begin()
                                , underlyingSenders_.end()
                                , [&locator](auto const &x) {
                                    return (std::get<0>(x) == locator);
                                }
                            )
                            , underlyingSenders_.end()
                        );
                        newSize = underlyingSenders_.size();
                    }
                    std::ostringstream oss;
                    oss << "[MultiTransportRemoteFacility::deregisterFacility] De-registered grpc interop facility for "
                        << locator;
                    env->log(infra::LogLevel::Info, oss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::JsonREST:
                if constexpr (std::is_convertible_v<Env *, json_rest::JsonRESTComponent *>) {
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                            senderMap_.erase(locator);
                        }
                        underlyingSenders_.erase(
                            std::remove_if(
                                underlyingSenders_.begin()
                                , underlyingSenders_.end()
                                , [&locator](auto const &x) {
                                    return (std::get<0>(x) == locator);
                                }
                            )
                            , underlyingSenders_.end()
                        );
                        newSize = underlyingSenders_.size();
                    }
                    std::ostringstream oss;
                    oss << "[MultiTransportRemoteFacility::deregisterFacility] De-registered json rest facility for "
                        << locator;
                    env->log(infra::LogLevel::Info, oss.str());
                }
                return {newSize, true};
                break;
            case MultiTransportRemoteFacilityConnectionType::WebSocket:
                if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                    auto *component = static_cast<web_socket::WebSocketComponent *>(env);
                    uint32_t clientNumber = 0;
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        auto iter = clientNumberRecord_.find(locator);
                        if (iter != clientNumberRecord_.end()) {
                            clientNumber = iter->second;
                            clientNumberRecord_.erase(iter);
                        }
                    }
                    component->websocket_removeRPCClient(locator, clientNumber);
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                            senderMap_.erase(locator);
                        }
                        underlyingSenders_.erase(
                            std::remove_if(
                                underlyingSenders_.begin()
                                , underlyingSenders_.end()
                                , [&locator](auto const &x) {
                                    return (std::get<0>(x) == locator);
                                }
                            )
                            , underlyingSenders_.end()
                        );
                        newSize = underlyingSenders_.size();
                    }
                    std::ostringstream oss;
                    oss << "[MultiTransportRemoteFacility::deregisterFacility] De-registered web socket facility for "
                        << locator;
                    env->log(infra::LogLevel::Info, oss.str());
                }
                return {newSize, true};
                break;
            default:
                return {0, false};
                break;
            }
        }

        void actuallyHandleFacilityAction(typename M::template InnerData<MultiTransportRemoteFacilityAction> &&action) {
            TM_INFRA_FACILITY_TRACER_WITH_SUFFIX(action.environment, ":setup");
            std::tuple<std::size_t, bool> handled = {0, false};
            switch (action.timedData.value.actionType) {
            case MultiTransportRemoteFacilityActionType::Register:
                handled = registerFacility(action.environment, action.timedData.value.connectionType, action.timedData.value.connectionLocator, action.timedData.value.description);
                break;
            case MultiTransportRemoteFacilityActionType::Deregister:
                handled = deregisterFacility(action.environment, action.timedData.value.connectionType, action.timedData.value.connectionLocator);
                break;
            default:
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    handled = {underlyingSenders_.size(), false};
                }
                break;
            }
            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random) {
                this->ImporterParent::publish(
                    M::template pureInnerData<std::size_t>(
                        action.environment
                        , std::move(std::get<0>(handled))
                    )
                );
            } else {
                //we ALWAYS publish the action itself as the response
                this->ImporterParent::publish(
                    M::template pureInnerData<MultiTransportRemoteFacilityActionResult>(
                        action.environment
                        , {
                            std::move(action.timedData.value)
                            , std::get<1>(handled)
                        }
                    )
                );
            }
        }
        void actuallyHandleInput(typename M::template InnerData<typename M::template Key<Input>> &&input) {
            TM_INFRA_FACILITY_TRACER_WITH_SUFFIX(input.environment, ":handle");
            std::lock_guard<std::mutex> _(mutex_);
            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random) {
                if (underlyingSenders_.empty()) {
                    return;
                }
                int sz = (int) underlyingSenders_.size();
                int idx = ((sz==1)?0:(std::rand()%sz));
                (*(std::get<1>(underlyingSenders_[idx])))(
                    Env::id_to_string(input.timedData.value.id())
                    , input.timedData.value.key()
                );
            } else if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                auto iter = senderMap_.find(std::get<0>(input.timedData.value.key()));
                if (iter == senderMap_.end()) {
                    return;
                }
                (*(iter->second))(
                    Env::id_to_string(input.timedData.value.id())
                    , std::get<1>(input.timedData.value.key())
                );
            }
        }
    public:
        MultiTransportRemoteFacility(std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> const &hookPairFactory = [](std::string const &, ConnectionLocator const &) {return std::nullopt;})
            : Parent(), hookPairFactory_(hookPairFactory), underlyingSenders_(), senderMap_(), clientNumberRecord_(), mutex_()
        {
            static_assert(
                (
                    DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random
                    ||
                    DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated
                )
                , "Only random and designated dispatch strategies are supported"
            );
            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random) {
                std::srand(std::time(nullptr)); 
            }
        }
        virtual ~MultiTransportRemoteFacility() {}

        void start(Env *) override final {}

        void handle(typename M::template InnerData<MultiTransportRemoteFacilityAction> &&action) override final {
            actuallyHandleFacilityAction(std::move(action));
        }
        void handle(typename M::template InnerData<typename M::template Key<Input>> &&input) override final {
            actuallyHandleInput(std::move(input));
        }
    };

    template <class Env>
    class OneShotMultiTransportRemoteFacilityCall {
    public:
        template <class A, class B>
        static std::future<B> call(
            Env *env
            , MultiTransportRemoteFacilityConnectionType connType
            , ConnectionLocator const &locator
            , A &&request
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            if (connType == MultiTransportRemoteFacilityConnectionType::RabbitMQ) {
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    return rabbitmq::RabbitMQOnOrderFacility<Env>::template typedOneShotRemoteCall<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect, clientNumberOutput
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type RabbitMQ not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::Redis) {
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    return redis::RedisOnOrderFacility<Env>::template typedOneShotRemoteCall<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect, clientNumberOutput
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type Redis not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::NATS) {
                if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                    return nats::NATSOnOrderFacility<Env>::template typedOneShotRemoteCall<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect, clientNumberOutput
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type NATS not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::SocketRPC) {
                if constexpr (std::is_convertible_v<Env *, socket_rpc::SocketRPCComponent *>) {
                    return socket_rpc::SocketRPCOnOrderFacility<Env>::template typedOneShotRemoteCall<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type Socket RPC not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::GrpcInterop) {
                if constexpr (std::is_convertible_v<Env *, grpc_interop::GrpcInteropComponent *>) {
                    if (hooks) {
                        throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type grpc interop does not support hooks");
                    }
                    if constexpr (
                        basic::bytedata_utils::ProtobufStyleSerializableChecker<A>::IsProtobufStyleSerializable()
                        &&
                        basic::bytedata_utils::ProtobufStyleSerializableChecker<B>::IsProtobufStyleSerializable()
                    ) {
                        return grpc_interop::GrpcClientFacilityFactory<infra::RealTimeApp<Env>>::template typedOneShotRemoteCall<A,B>(
                            env, locator, std::move(request)
                        );
                    } else if constexpr (
                        basic::proto_interop::ProtoWrappable<A>::value
                        &&
                        basic::proto_interop::ProtoWrappable<B>::value
                    ) {
                        auto ret = std::make_shared<std::promise<B>>();
                        std::thread th([env,locator,request=std::move(request),ret]() mutable {
                            auto x = grpc_interop::GrpcClientFacilityFactory<infra::RealTimeApp<Env>>::template typedOneShotRemoteCall<basic::proto_interop::Proto<A>,basic::proto_interop::Proto<B>>(
                                env, locator, basic::proto_interop::Proto<A> {std::move(request)}
                            );
                            ret->set_value_at_thread_exit(*(x.get()));
                        });
                        th.detach();
                        return ret->get_future();
                    } else {
                        throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type grpc interop used on non-grpc-compatible types");
                    }
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type grpc interop not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::JsonREST) {
                if constexpr (std::is_convertible_v<Env *, json_rest::JsonRESTComponent *>) {
                    if (hooks) {
                        throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type json rest does not support hooks");
                    }
                    if constexpr (
                        basic::nlohmann_json_interop::JsonWrappable<A>::value
                        &&
                        basic::nlohmann_json_interop::JsonWrappable<B>::value
                    ) {
                        return json_rest::JsonRESTClientFacilityFactory<infra::RealTimeApp<Env>>::template typedOneShotRemoteCall<A,B>(
                            env, locator, std::move(request)
                        );
                    } else {
                        throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type json rest used on non-json-compatible types");
                    }
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type json rest not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::WebSocket) {
                if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                    return web_socket::WebSocketOnOrderFacilityClient<Env>::template typedOneShotRemoteCall<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect, clientNumberOutput
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type web socket not supported in environment");
                }
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: unknown connection type");
            }
        }
        template <template<class...> class ProtocolWrapper, class A, class B>
        static std::future<B> callWithProtocol(
            Env *env
            , MultiTransportRemoteFacilityConnectionType connType
            , ConnectionLocator const &locator
            , A &&request
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            if constexpr (std::is_same_v<
                basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,A>
                , A
            >) {
                return call<A,B>(env, connType, locator, std::move(request), hooks, autoDisconnect, clientNumberOutput);
            } else {
                auto wrappedB = call<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,A>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,B>
                >(
                    env, connType, locator
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                        ::enclose(std::move(request))
                    , hooks, autoDisconnect, clientNumberOutput
                );
                return std::async(std::launch::async, [wrappedB=std::move(wrappedB)]() mutable -> B {
                    return basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                        ::extract(wrappedB.get());
                });
            }
        }
        template <class A, class B>
        static std::future<B> call(
            Env *env
            , std::string const &facilityDescriptor
            , A &&request
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            if (env) {
                std::ostringstream oss;
                oss << "[OneShotMultiTransportRemoteFacilityCall::call] calling " << facilityDescriptor;
                env->log(infra::LogLevel::Info, oss.str());
            }
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                return call<A,B>(env, std::get<0>(*parseRes), std::get<1>(*parseRes), std::move(request), hooks, autoDisconnect, clientNumberOutput);
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: bad facility descriptor '"+facilityDescriptor+"'");
            }
        }
        template <template<class...> class ProtocolWrapper, class A, class B>
        static std::future<B> callWithProtocol(
            Env *env
            , std::string const &facilityDescriptor
            , A &&request
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                return callWithProtocol<ProtocolWrapper,A,B>(env, std::get<0>(*parseRes), std::get<1>(*parseRes), std::move(request), hooks, autoDisconnect, clientNumberOutput);
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: bad facility descriptor '"+facilityDescriptor+"'");
            }
        }
        template <class A, class B>
        static void callNoReply(
            Env *env
            , MultiTransportRemoteFacilityConnectionType connType
            , ConnectionLocator const &locator
            , A &&request
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            if (connType == MultiTransportRemoteFacilityConnectionType::RabbitMQ) {
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    rabbitmq::RabbitMQOnOrderFacility<Env>::template typedOneShotRemoteCallNoReply<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect, clientNumberOutput
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type RabbitMQ not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::Redis) {
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    redis::RedisOnOrderFacility<Env>::template typedOneShotRemoteCallNoReply<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect, clientNumberOutput
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type Redis not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::NATS) {
                if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                    nats::NATSOnOrderFacility<Env>::template typedOneShotRemoteCallNoReply<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect, clientNumberOutput
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type NATS not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::SocketRPC) {
                if constexpr (std::is_convertible_v<Env *, socket_rpc::SocketRPCComponent *>) {
                    socket_rpc::SocketRPCOnOrderFacility<Env>::template typedOneShotRemoteCallNoReply<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type Socket RPC not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::GrpcInterop) {
                if constexpr (std::is_convertible_v<Env *, grpc_interop::GrpcInteropComponent *>) {
                    if (hooks) {
                        throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type grpc interop does not support hooks");
                    }
                    if constexpr (
                        basic::bytedata_utils::ProtobufStyleSerializableChecker<A>::IsProtobufStyleSerializable()
                        &&
                        basic::bytedata_utils::ProtobufStyleSerializableChecker<B>::IsProtobufStyleSerializable()
                    ) {
                        grpc_interop::GrpcClientFacilityFactory<infra::RealTimeApp<Env>>::template typedOneShotRemoteCall<A,B>(
                            env, locator, std::move(request)
                        );
                    } else if constexpr (
                        basic::proto_interop::ProtoWrappable<A>::value
                        &&
                        basic::proto_interop::ProtoWrappable<B>::value
                    ) {
                        grpc_interop::GrpcClientFacilityFactory<infra::RealTimeApp<Env>>::template typedOneShotRemoteCall<basic::proto_interop::Proto<A>,basic::proto_interop::Proto<B>>(
                            env, locator, basic::proto_interop::Proto<A> {std::move(request)}
                        );
                    } else {
                        throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type grpc interop used on non-grpc-compatible types");
                    }
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type grpc interop not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::JsonREST) {
                if constexpr (std::is_convertible_v<Env *, json_rest::JsonRESTComponent *>) {
                    if (hooks) {
                        throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type json rest does not support hooks");
                    }
                    if constexpr (
                        basic::nlohmann_json_interop::JsonWrappable<A>::value
                        &&
                        basic::nlohmann_json_interop::JsonWrappable<B>::value
                    ) {
                        json_rest::JsonRESTClientFacilityFactory<infra::RealTimeApp<Env>>::template typedOneShotRemoteCall<A,B>(
                            env, locator, std::move(request)
                        );
                    } else {
                        throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type json rest used on non-json-compatible types");
                    }
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type json rest not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::WebSocket) {
                if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                    web_socket::WebSocketOnOrderFacilityClient<Env>::template typedOneShotRemoteCallNoReply<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect, clientNumberOutput
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type web socket not supported in environment");
                }
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: unknown connection type");
            }
        }
        template <template <class...> class ProtocolWrapper, class A, class B>
        static void callNoReplyWithProtocol(
            Env *env
            , MultiTransportRemoteFacilityConnectionType connType
            , ConnectionLocator const &locator
            , A &&request
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            callNoReply<
                basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,A>
                , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,B>
            >(
                env, connType, locator 
                , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                    ::enclose(std::move(request))
                , hooks, autoDisconnect, clientNumberOutput
            );
        }
        template <class A, class B>
        static void callNoReply(
            Env *env
            , std::string const &facilityDescriptor
            , A &&request
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                callNoReply<A,B>(env, std::get<0>(*parseRes), std::get<1>(*parseRes), std::move(request), hooks, autoDisconnect, clientNumberOutput);
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: bad facility descriptor '"+facilityDescriptor+"'");
            }
        }
        template <template<class...> class ProtocolWrapper, class A, class B>
        static void callNoReplyWithProtocol(
            Env *env
            , std::string const &facilityDescriptor
            , A &&request
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                callNoReplyWithProtocol<ProtocolWrapper,A,B>(env, std::get<0>(*parseRes), std::get<1>(*parseRes), std::move(request), hooks, autoDisconnect, clientNumberOutput);
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: bad facility descriptor '"+facilityDescriptor+"'");
            }
        }
        static void removeClient(
            Env *env
            , MultiTransportRemoteFacilityConnectionType connType
            , ConnectionLocator const &locator
            , uint32_t clientNumberIfNeeded
        ) {
            if (connType == MultiTransportRemoteFacilityConnectionType::RabbitMQ) {
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    env->rabbitmq_removeRPCQueueClient(locator, clientNumberIfNeeded);
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: connection type RabbitMQ not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::Redis) {
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    env->redis_removeRPCClient(locator, clientNumberIfNeeded);
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: connection type Redis not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::NATS) {
                if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                    env->nats_removeRPCClient(locator, clientNumberIfNeeded);
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: connection type NATS not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::SocketRPC) {
                if constexpr (std::is_convertible_v<Env *, socket_rpc::SocketRPCComponent *>) {
                    env->socket_rpc_removeRPCClient(locator);
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: connection type Socket RPC not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::GrpcInterop) {
                if constexpr (std::is_convertible_v<Env *, grpc_interop::GrpcInteropComponent *>) {
                    env->grpc_interop_removeRPCClient(locator);
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: connection type grpc interop not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::JsonREST) {
                //do nothing, json rest does not need to explicitly remove connection
            } else if (connType == MultiTransportRemoteFacilityConnectionType::WebSocket) {
                if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                    env->websocket_removeRPCClient(locator, clientNumberIfNeeded);
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: connection type web socket not supported in environment");
                }
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: unknown connection type");
            }
        }
        static void removeClient(
            Env *env
            , std::string const &facilityDescriptor
            , uint32_t clientNumberIfNeeded
        ) {
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                return removeClient(env, std::get<0>(*parseRes), std::get<1>(*parseRes), clientNumberIfNeeded);
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: bad facility descriptor '"+facilityDescriptor+"'");
            }
        }
        template <class A, class B>
        static std::optional<B> callWithTimeout(
            Env *env
            , MultiTransportRemoteFacilityConnectionType connType
            , ConnectionLocator const &locator
            , A &&request
            , std::chrono::system_clock::duration const &timeOut
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
        ) {
            uint32_t clientNum = 0;
            auto ret = call<A,B>(env, connType, locator, std::move(request), hooks, autoDisconnect, &clientNum);
            if (ret.wait_for(timeOut) == std::future_status::ready) {
                return {std::move(ret.get())};
            } else {
                removeClient(env, connType, locator, clientNum);
                return std::nullopt;
            }
        }
        template <template<class...> class ProtocolWrapper, class A, class B>
        static std::optional<B> callWithProtocolWithTimeout(
            Env *env
            , MultiTransportRemoteFacilityConnectionType connType
            , ConnectionLocator const &locator
            , A &&request
            , std::chrono::system_clock::duration const &timeOut
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
        ) {
            uint32_t clientNum = 0;
            auto ret = callWithProtocol<ProtocolWrapper,A,B>(env, connType, locator, std::move(request), hooks, autoDisconnect, &clientNum);
            if (ret.wait_for(timeOut) == std::future_status::ready) {
                return {std::move(ret.get())};
            } else {
                removeClient(env, connType, locator, clientNum);
                return std::nullopt;
            }
        }
        template <class A, class B>
        static std::optional<B> callWithTimeout(
            Env *env
            , std::string const &facilityDescriptor
            , A &&request
            , std::chrono::system_clock::duration const &timeOut
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
        ) {
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                return callWithTimeout<A,B>(env, std::get<0>(*parseRes), std::get<1>(*parseRes), std::move(request), timeOut, hooks, autoDisconnect);
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callWithTimeout: bad facility descriptor '"+facilityDescriptor+"'");
            }
        }
        template <template<class...> class ProtocolWrapper, class A, class B>
        static std::optional<B> callWithProtocolWithTimeout(
            Env *env
            , std::string const &facilityDescriptor
            , A &&request
            , std::chrono::system_clock::duration const &timeOut
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
        ) {
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                return callWithProtocolWithTimeout<ProtocolWrapper,A,B>(env, std::get<0>(*parseRes), std::get<1>(*parseRes), std::move(request), timeOut, hooks, autoDisconnect);
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callWithTimeout: bad facility descriptor '"+facilityDescriptor+"'");
            }
        }
        template <class A, class B>
        static std::future<B> callByHeartbeat(
            Env *env
            , std::string const &heartbeatChannelSpec
            , std::string const &heartbeatTopicDescription
            , std::regex const &heartbeatSenderNameRE
            , std::string const &facilityNameInServer
            , A &&request
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            if (env) {
                std::ostringstream oss;
                oss << "[OneShotMultiTransportRemoteFacilityCall::callByHeartbeat] calling by heartbeat, spec='" << heartbeatChannelSpec << "', topic description='" << heartbeatTopicDescription << "'";
                env->log(infra::LogLevel::Info, oss.str());
            }
            auto heartbeatResult = MultiTransportBroadcastFirstUpdateQueryManagingUtils<Env>
                ::template fetchTypedFirstUpdateAndDisconnect<HeartbeatMessage>
                (
                    env
                    , heartbeatChannelSpec
                    , heartbeatTopicDescription
                    , [heartbeatSenderNameRE](HeartbeatMessage const &m) {
                        return std::regex_match(
                            m.senderDescription()
                            , heartbeatSenderNameRE
                        );
                    }
                    , heartbeatHook
                ).get().content;
            if (env) {
                std::ostringstream oss;
                oss << "[OneShotMultiTransportRemoteFacilityCall::callByHeartbeat] received heartbeat result" << heartbeatResult;
                env->log(infra::LogLevel::Info, oss.str());
            }
            auto iter = heartbeatResult.facilityChannels().find(facilityNameInServer);
            if (iter == heartbeatResult.facilityChannels().end()) {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callByHeartbeat: no facility name "+facilityNameInServer+" in heartbeat message");
            } else {
                return call<A,B>(
                    env 
                    , iter->second
                    , std::move(request)
                    , hooks 
                    , autoDisconnect
                    , clientNumberOutput
                );
            }
        }
        template <template<class...> class ProtocolWrapper, class A, class B>
        static std::future<B> callWithProtocolByHeartbeat(
            Env *env
            , std::string const &heartbeatChannelSpec
            , std::string const &heartbeatTopicDescription
            , std::regex const &heartbeatSenderNameRE
            , std::string const &facilityNameInServer
            , A &&request
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            if constexpr (std::is_same_v<
                basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,A>
                , A
            >) {
                return callByHeartbeat<A,B>(
                    env, heartbeatChannelSpec, heartbeatTopicDescription, heartbeatSenderNameRE, facilityNameInServer
                    , std::move(request)
                    , heartbeatHook, hooks, autoDisconnect, clientNumberOutput
                );
            } else {
                auto wrappedB = callByHeartbeat<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,A>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,B>
                >(
                    env, heartbeatChannelSpec, heartbeatTopicDescription, heartbeatSenderNameRE, facilityNameInServer
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                        ::enclose(std::move(request))
                    , heartbeatHook, hooks, autoDisconnect, clientNumberOutput
                );
                return std::async(std::launch::async, [wrappedB=std::move(wrappedB)]() mutable -> B {
                    return basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                        ::extract(wrappedB.get());
                });
            }
        }
        template <class A, class B>
        static void callNoReplyByHeartbeat(
            Env *env
            , std::string const &heartbeatChannelSpec
            , std::string const &heartbeatTopicDescription
            , std::regex const &heartbeatSenderNameRE
            , std::string const &facilityNameInServer
            , A &&request
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            auto heartbeatResult = MultiTransportBroadcastFirstUpdateQueryManagingUtils<Env>
                ::template fetchTypedFirstUpdateAndDisconnect<HeartbeatMessage>
                (
                    env
                    , heartbeatChannelSpec
                    , heartbeatTopicDescription
                    , [heartbeatSenderNameRE](HeartbeatMessage const &m) {
                        return std::regex_match(
                            m.senderDescription()
                            , heartbeatSenderNameRE
                        );
                    }
                    , heartbeatHook
                ).get().content;
            auto iter = heartbeatResult.facilityChannels().find(facilityNameInServer);
            if (iter == heartbeatResult.facilityChannels().end()) {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callByHeartbeat: no facility name "+facilityNameInServer+" in heartbeat message");
            } else {
                callNoReply<A,B>(
                    env 
                    , iter->second
                    , std::move(request)
                    , hooks 
                    , autoDisconnect
                    , clientNumberOutput
                );
            }
        }
        template <template<class...> class ProtocolWrapper, class A, class B>
        static void callNoReplyWithProtocolByHeartbeat(
            Env *env
            , std::string const &heartbeatChannelSpec
            , std::string const &heartbeatTopicDescription
            , std::regex const &heartbeatSenderNameRE
            , std::string const &facilityNameInServer
            , A &&request
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
            , uint32_t *clientNumberOutput = nullptr
        ) {
            callNoReplyByHeartbeat<
                basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,A>
                , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,B>
            >(
                env, heartbeatChannelSpec, heartbeatTopicDescription, heartbeatSenderNameRE, facilityNameInServer
                , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                    ::enclose(std::move(request))
                , heartbeatHook, hooks, autoDisconnect, clientNumberOutput
            );
        }
        template <class A, class B>
        static std::optional<B> callWithTimeoutByHeartbeat(
            Env *env
            , std::string const &heartbeatChannelSpec
            , std::string const &heartbeatTopicDescription
            , std::regex const &heartbeatSenderNameRE
            , std::string const &facilityNameInServer
            , A &&request
            , std::chrono::system_clock::duration const &timeOut
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
        ) {
            auto heartbeatResult = MultiTransportBroadcastFirstUpdateQueryManagingUtils<Env>
                ::template fetchTypedFirstUpdateAndDisconnect<HeartbeatMessage>
                (
                    env
                    , heartbeatChannelSpec
                    , heartbeatTopicDescription
                    , [heartbeatSenderNameRE](HeartbeatMessage const &m) {
                        return std::regex_match(
                            m.senderDescription()
                            , heartbeatSenderNameRE
                        );
                    }
                    , heartbeatHook
                ).get().content;
            auto iter = heartbeatResult.facilityChannels().find(facilityNameInServer);
            if (iter == heartbeatResult.facilityChannels().end()) {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callByHeartbeat: no facility name "+facilityNameInServer+" in heartbeat message");
            } else {
                return callWithTimeout<A,B>(
                    env 
                    , iter->second
                    , std::move(request)
                    , timeOut
                    , hooks 
                    , autoDisconnect
                );
            }
        }
        template <template<class...> class ProtocolWrapper, class A, class B>
        static std::optional<B> callWithProtocolWithTimeoutByHeartbeat(
            Env *env
            , std::string const &heartbeatChannelSpec
            , std::string const &heartbeatTopicDescription
            , std::regex const &heartbeatSenderNameRE
            , std::string const &facilityNameInServer
            , A &&request
            , std::chrono::system_clock::duration const &timeOut
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
        ) {
            auto heartbeatResult = MultiTransportBroadcastFirstUpdateQueryManagingUtils<Env>
                ::template fetchTypedFirstUpdateAndDisconnect<HeartbeatMessage>
                (
                    env
                    , heartbeatChannelSpec
                    , heartbeatTopicDescription
                    , [heartbeatSenderNameRE](HeartbeatMessage const &m) {
                        return std::regex_match(
                            m.senderDescription()
                            , heartbeatSenderNameRE
                        );
                    }
                    , heartbeatHook
                ).get().content;
            auto iter = heartbeatResult.facilityChannels().find(facilityNameInServer);
            if (iter == heartbeatResult.facilityChannels().end()) {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callByHeartbeat: no facility name "+facilityNameInServer+" in heartbeat message");
            } else {
                return callWithProtocolWithTimeout<ProtocolWrapper,A,B>(
                    env 
                    , iter->second
                    , std::move(request)
                    , timeOut
                    , hooks 
                    , autoDisconnect
                );
            }
        }
    };

} } } }

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace bytedata_utils {
    template <>
    struct RunCBORSerializer<transport::MultiTransportRemoteFacilityActionType, void> {
        static std::string apply(transport::MultiTransportRemoteFacilityActionType const &x) {
            return RunCBORSerializer<std::string>::apply(
                transport::MULTI_TRANSPORT_REMOTE_FACILITY_ACTION_TYPE_STR[static_cast<int>(x)]
            );
        }
        static std::size_t apply(transport::MultiTransportRemoteFacilityActionType const &x, char *output) {
            return RunCBORSerializer<std::string>::apply(
                transport::MULTI_TRANSPORT_REMOTE_FACILITY_ACTION_TYPE_STR[static_cast<int>(x)]
                , output
            );
        }
        static std::size_t calculateSize(transport::MultiTransportRemoteFacilityActionType const &x) {
            return RunCBORSerializer<std::string>::calculateSize(
                transport::MULTI_TRANSPORT_REMOTE_FACILITY_ACTION_TYPE_STR[static_cast<int>(x)]
            );
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportRemoteFacilityActionType, void> {
        static std::optional<std::tuple<transport::MultiTransportRemoteFacilityActionType,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<std::string>::apply(data, start);
            if (t) {
                int ii = 0;
                for (auto const &s : transport::MULTI_TRANSPORT_REMOTE_FACILITY_ACTION_TYPE_STR) {
                    if (s == std::get<0>(*t)) {
                        return std::tuple<transport::MultiTransportRemoteFacilityActionType,size_t> {
                            static_cast<transport::MultiTransportRemoteFacilityActionType>(ii)
                            , std::get<1>(*t)
                        };
                    }
                    ++ii;
                }
                return std::nullopt;
            } else {
                return std::nullopt;
            }
        }
        static std::optional<size_t> applyInPlace(transport::MultiTransportRemoteFacilityActionType &output, std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<std::string>::apply(data, start);
            if (t) {
                int ii = 0;
                for (auto const &s : transport::MULTI_TRANSPORT_REMOTE_FACILITY_ACTION_TYPE_STR) {
                    if (s == std::get<0>(*t)) {
                        output = static_cast<transport::MultiTransportRemoteFacilityActionType>(ii);
                        return std::get<1>(*t);
                    }
                    ++ii;
                }
                return std::nullopt;
            } else {
                return std::nullopt;
            }
        }
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportRemoteFacilityConnectionType, void> {
        static std::string apply(transport::MultiTransportRemoteFacilityConnectionType const &x) {
            return RunCBORSerializer<std::string>::apply(
                transport::MULTI_TRANSPORT_REMOTE_FACILITY_CONNECTION_TYPE_STR[static_cast<int>(x)]
            );
        }
        static std::size_t apply(transport::MultiTransportRemoteFacilityConnectionType const &x, char *output) {
            return RunCBORSerializer<std::string>::apply(
                transport::MULTI_TRANSPORT_REMOTE_FACILITY_CONNECTION_TYPE_STR[static_cast<int>(x)]
                , output
            );
        }
        static std::size_t calculateSize(transport::MultiTransportRemoteFacilityConnectionType const &x) {
            return RunCBORSerializer<std::string>::calculateSize(
                transport::MULTI_TRANSPORT_REMOTE_FACILITY_CONNECTION_TYPE_STR[static_cast<int>(x)]
            );
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportRemoteFacilityConnectionType, void> {
        static std::optional<std::tuple<transport::MultiTransportRemoteFacilityConnectionType,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<std::string>::apply(data, start);
            if (t) {
                int ii = 0;
                for (auto const &s : transport::MULTI_TRANSPORT_REMOTE_FACILITY_CONNECTION_TYPE_STR) {
                    if (s == std::get<0>(*t)) {
                        return std::tuple<transport::MultiTransportRemoteFacilityConnectionType,size_t> {
                            static_cast<transport::MultiTransportRemoteFacilityConnectionType>(ii)
                            , std::get<1>(*t)
                        };
                    }
                    ++ii;
                }
                return std::nullopt;
            } else {
                return std::nullopt;
            }
        }
        static std::optional<size_t> applyInPlace(transport::MultiTransportRemoteFacilityConnectionType &output, std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<std::string>::apply(data, start);
            if (t) {
                int ii = 0;
                for (auto const &s : transport::MULTI_TRANSPORT_REMOTE_FACILITY_CONNECTION_TYPE_STR) {
                    if (s == std::get<0>(*t)) {
                        output = static_cast<transport::MultiTransportRemoteFacilityConnectionType>(ii);
                        return std::get<1>(*t);
                    }
                    ++ii;
                }
                return std::nullopt;
            } else {
                return std::nullopt;
            }
        }
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportRemoteFacilityAction, void> {
        static std::string apply(transport::MultiTransportRemoteFacilityAction const &x) {
            std::tuple<
                transport::MultiTransportRemoteFacilityActionType const *
                , transport::MultiTransportRemoteFacilityConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            > t {&x.actionType, &x.connectionType, &x.connectionLocator, &x.description};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportRemoteFacilityActionType const *
                , transport::MultiTransportRemoteFacilityConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            >, 4>
                ::apply(t, {
                    "action_type"
                    , "connection_type"
                    , "connection_locator"
                    , "description"
                });
        }
        static std::size_t apply(transport::MultiTransportRemoteFacilityAction const &x, char *output) {
            std::tuple<
                transport::MultiTransportRemoteFacilityActionType const *
                , transport::MultiTransportRemoteFacilityConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            > t {&x.actionType, &x.connectionType, &x.connectionLocator, &x.description};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportRemoteFacilityActionType const *
                , transport::MultiTransportRemoteFacilityConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            >, 4>
                ::apply(t, {
                    "action_type"
                    , "connection_type"
                    , "connection_locator"
                    , "description"
                }, output);
        }
        static std::size_t calculateSize(transport::MultiTransportRemoteFacilityAction const &x) {
            std::tuple<
                transport::MultiTransportRemoteFacilityActionType const *
                , transport::MultiTransportRemoteFacilityConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            > t {&x.actionType, &x.connectionType, &x.connectionLocator, &x.description};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportRemoteFacilityActionType const *
                , transport::MultiTransportRemoteFacilityConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            >, 4>
                ::calculateSize(t, {
                    "action_type"
                    , "connection_type"
                    , "connection_locator"
                    , "description"
                });
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportRemoteFacilityAction, void> {
        static std::optional<std::tuple<transport::MultiTransportRemoteFacilityAction,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializerWithNameList<std::tuple<
                transport::MultiTransportRemoteFacilityActionType
                , transport::MultiTransportRemoteFacilityConnectionType
                , transport::ConnectionLocator
                , std::string
            >, 4>::apply(data, start, {
                    "action_type"
                    , "connection_type"
                    , "connection_locator"
                    , "description"
                });
            if (t) {
                return std::tuple<transport::MultiTransportRemoteFacilityAction,size_t> {
                    transport::MultiTransportRemoteFacilityAction {
                        std::move(std::get<0>(std::get<0>(*t)))
                        , std::move(std::get<1>(std::get<0>(*t)))
                        , std::move(std::get<2>(std::get<0>(*t)))
                        , std::move(std::get<3>(std::get<0>(*t)))
                    }
                    , std::get<1>(*t)
                };
            } else {
                return std::nullopt;
            }
        }
        static std::optional<size_t> applyInPlace(transport::MultiTransportRemoteFacilityAction &output, std::string_view const &data, size_t start) {
            auto x = std::tuple<
                transport::MultiTransportRemoteFacilityActionType *
                , transport::MultiTransportRemoteFacilityConnectionType *
                , transport::ConnectionLocator *
                , std::string *
            >(&output.actionType, &output.connectionType, &output.connectionLocator, &output.description);
            return RunCBORDeserializerWithNameList<std::tuple<
                transport::MultiTransportRemoteFacilityActionType *
                , transport::MultiTransportRemoteFacilityConnectionType *
                , transport::ConnectionLocator *
                , std::string *
            >, 4>::applyInPlace(x, data, start, {
                    "action_type"
                    , "connection_type"
                    , "connection_locator"
                    , "description"
                });
        }
    };
                
} } } } }

#endif
