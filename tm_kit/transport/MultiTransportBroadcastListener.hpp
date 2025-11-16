#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/TraceNodesComponent.hpp>
#include <tm_kit/infra/Environments.hpp>

#include <tm_kit/basic/ByteData.hpp>

#include <tm_kit/transport/multicast/MulticastComponent.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>
#include <tm_kit/transport/redis/RedisComponent.hpp>
#include <tm_kit/transport/nats/NATSComponent.hpp>
#include <tm_kit/transport/zeromq/ZeroMQComponent.hpp>
#include <tm_kit/transport/nng/NNGComponent.hpp>
#include <tm_kit/transport/socket_rpc/SocketRPCComponent.hpp>
#include <tm_kit/transport/shared_memory_broadcast/SharedMemoryBroadcastComponent.hpp>
#include <tm_kit/transport/AbstractHookFactoryComponent.hpp>
#include <tm_kit/transport/grpc_interop/GrpcInteropComponent.hpp>
#include <tm_kit/transport/json_rest/JsonRESTComponent.hpp>
#include <tm_kit/transport/websocket/WebSocketComponent.hpp>
#include <tm_kit/transport/singlecast/SinglecastComponent.hpp>

#include <type_traits>
#include <regex>

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    using AllNetworkTransportComponents = infra::Environment<
        multicast::MulticastComponent
        , rabbitmq::RabbitMQComponent
        , redis::RedisComponent
        , nats::NATSComponent
        , zeromq::ZeroMQComponent
        , nng::NNGComponent
        , shared_memory_broadcast::SharedMemoryBroadcastComponent
        , socket_rpc::SocketRPCComponent
        , grpc_interop::GrpcInteropComponent
        , json_rest::JsonRESTComponent
        , web_socket::WebSocketComponent
        , singlecast::SinglecastComponent
    >;

    template <class Env>
    inline std::unordered_map<
        std::string,
        std::unordered_map<ConnectionLocator, std::thread::native_handle_type> 
    >
    networkTransportComponents_threadHandles(Env *env) {
        std::unordered_map<
            std::string
            , std::unordered_map<ConnectionLocator, std::thread::native_handle_type> 
        > retVal;
        if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
            retVal["multicast"] = env->multicast_threadHandles();
        }
        if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
            retVal["nng"] = env->nng_threadHandles();
        }
        if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
            retVal["rabbitmq"] = env->rabbitmq_threadHandles();
        }
        if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
            retVal["redis"] = env->redis_threadHandles();
        }
        if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
            retVal["nats"] = env->nats_threadHandles();
        }
        if constexpr (std::is_convertible_v<Env *, shared_memory_broadcast::SharedMemoryBroadcastComponent *>) {
            retVal["shared_memory_broadcast"] = env->shared_memory_broadcast_threadHandles();
        }
        if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
            retVal["zeromq"] = env->zeromq_threadHandles();
        }
        if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
            retVal["websocket"] = env->websocket_threadHandles();
        }
        if constexpr (std::is_convertible_v<Env *, json_rest::JsonRESTComponent *>) {
            retVal["json_rest"] = env->json_rest_threadHandles();
        }
        if constexpr (std::is_convertible_v<Env *, singlecast::SinglecastComponent *>) {
            retVal["singlecast"] = env->singlecast_threadHandles();
        }
        return retVal;
    }

    enum class MultiTransportBroadcastListenerConnectionType {
        Multicast
        , RabbitMQ
        , Redis 
        , NATS
        , ZeroMQ 
        , NNG
        , SharedMemoryBroadcast
        , WebSocket
        , Singlecast
    };
    inline const std::array<std::string,9> MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR = {
        "multicast"
        , "rabbitmq"
        , "redis"
        , "nats"
        , "zeromq"
        , "nng"
        , "shared_memory_broadcast"
        , "websocket"
        , "singlecast"
    };
    inline auto parseMultiTransportBroadcastChannel(std::string const &s) 
        -> std::optional<std::tuple<MultiTransportBroadcastListenerConnectionType, ConnectionLocator>>
    {
        size_t ii = 0;
        for (auto const &item : MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR) {
            if (boost::starts_with(s, item+"://")) {
                try {
                    auto locator = ConnectionLocator::parse(s.substr(item.length()+3));
                    return std::tuple<MultiTransportBroadcastListenerConnectionType, ConnectionLocator> {
                        static_cast<MultiTransportBroadcastListenerConnectionType>(ii)
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
    struct MultiTransportBroadcastListenerAddSubscription {
        MultiTransportBroadcastListenerConnectionType connectionType;
        ConnectionLocator connectionLocator;
        std::string topicDescription; 
        bool operator==(MultiTransportBroadcastListenerAddSubscription const &a) const {
            if (this == &a) {
                return true;
            }
            return (connectionType == a.connectionType && connectionLocator == a.connectionLocator && topicDescription == a.topicDescription);
        }
        bool operator<(MultiTransportBroadcastListenerAddSubscription const &a) const {
            if (this == &a) {
                return false;
            }
            if (connectionType < a.connectionType) {
                return true;
            }
            if (connectionType > a.connectionType) {
                return false;
            }
            if (connectionLocator < a.connectionLocator) {
                return true;
            }
            if (a.connectionLocator < connectionLocator) {
                return false;
            }
            return (topicDescription < a.topicDescription);
        }
    };
    inline std::ostream &operator<<(std::ostream &os, MultiTransportBroadcastListenerAddSubscription const &x) {
        os << "MultiTransportBroadcastListenerAddSubscription{"
            << "connectionType=" << MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR[static_cast<int>(x.connectionType)]
            << ",connectionLocator=" << x.connectionLocator
            << ",topicDescription='" << x.topicDescription << "'"
            << '}';
        return os;
    }
    struct MultiTransportBroadcastListenerRemoveSubscription {
        MultiTransportBroadcastListenerConnectionType connectionType;
        uint32_t subscriptionID;
    };
    inline std::ostream &operator<<(std::ostream &os, MultiTransportBroadcastListenerRemoveSubscription const &x) {
        os << "MultiTransportBroadcastListenerRemoveSubscription{"
            << "connectionType=" << MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR[static_cast<int>(x.connectionType)]
            << ",subscriptionID=" << x.subscriptionID
            << '}';
        return os;
    }
    struct MultiTransportBroadcastListenerRemoveAllSubscriptions {
    };
    inline std::ostream &operator<<(std::ostream &os, MultiTransportBroadcastListenerRemoveAllSubscriptions const &/*x*/) {
        os << "MultiTransportBroadcastListenerRemoveAllSubscriptions{"
            << '}';
        return os;
    }
    struct MultiTransportBroadcastListenerAddSubscriptionResponse {
        uint32_t subscriptionID;
    };
    inline std::ostream &operator<<(std::ostream &os, MultiTransportBroadcastListenerAddSubscriptionResponse const &x) {
        os << "MultiTransportBroadcastListenerAddSubscriptionResponse{"
            << "subscriptionID=" << x.subscriptionID
            << '}';
        return os;
    }
    struct MultiTransportBroadcastListenerRemoveSubscriptionResponse {
    };
    inline std::ostream &operator<<(std::ostream &os, MultiTransportBroadcastListenerRemoveSubscriptionResponse const &/*x*/) {
        os << "MultiTransportBroadcastListenerRemoveSubscriptionResponse{}";
        return os;
    }
    struct MultiTransportBroadcastListenerRemoveAllSubscriptionsResponse {
    };
    inline std::ostream &operator<<(std::ostream &os, MultiTransportBroadcastListenerRemoveAllSubscriptionsResponse const &/*x*/) {
        os << "MultiTransportBroadcastListenerRemoveAllSubscriptionsResponse{}";
        return os;
    }
    using MultiTransportBroadcastListenerInput = basic::CBOR<
        std::variant<
            MultiTransportBroadcastListenerAddSubscription
            , MultiTransportBroadcastListenerRemoveSubscription
            , MultiTransportBroadcastListenerRemoveAllSubscriptions
        >
    >;
    using MultiTransportBroadcastListenerOutput = basic::CBOR<
        std::variant<
            MultiTransportBroadcastListenerAddSubscriptionResponse
            , MultiTransportBroadcastListenerRemoveSubscriptionResponse
            , MultiTransportBroadcastListenerRemoveAllSubscriptionsResponse
        >
    >;

    class MultiTransportBroadcastListenerRedisTopicHelper final {
    public:
        static std::string redisTopicHelper(std::string const &input) {
            std::string ret = input;
            std::replace(ret.begin(), ret.end(), '#', '*');
            return ret;
        }
    };
    class MultiTransportBroadcastListenerNATSTopicHelper final {
    public:
        static std::string natsTopicHelper(std::string const &input) {
            std::string ret = input;
            std::replace(ret.begin(), ret.end(), '#', '*');
            return ret;
        }
    };

    template <class Component>
    class MultiTransportBroadcastListenerTopicHelper final {
    public:
        static std::variant<
            typename Component::NoTopicSelection, std::string, std::regex
        > parseTopic(std::string const &s) {
            if (s == "") {
                if constexpr(std::is_same_v<Component, rabbitmq::RabbitMQComponent>) {
                    return "#";
                } else if constexpr (std::is_same_v<Component, redis::RedisComponent>) {
                    return "*";
                } else if constexpr (std::is_same_v<Component, nats::NATSComponent>) {
                    return ">";
                } else {
                    return typename Component::NoTopicSelection {};
                }
            } else {
                if constexpr(
                    std::is_same_v<Component, rabbitmq::RabbitMQComponent>
                ) {
                    return s;
                } else if constexpr(
                    std::is_same_v<Component, redis::RedisComponent>
                ) {
                    return MultiTransportBroadcastListenerRedisTopicHelper::redisTopicHelper(s);
                } else if constexpr(
                    std::is_same_v<Component, nats::NATSComponent>
                ) {
                    return MultiTransportBroadcastListenerNATSTopicHelper::natsTopicHelper(s);
                } else {
                    if (boost::starts_with(s, "r/") && boost::ends_with(s, "/") && s.length() > 3) {
                        return std::regex {s.substr(2, s.length()-3)};
                    } else {
                        std::ostringstream oss;
                        bool rabbitMQStyle = false;
                        for (char c : s) {
                            if (c == '#') {
                                oss << ".+";
                                rabbitMQStyle = true;
                            } else if (c == '*') {
                                oss << "[^\\.]+";
                                rabbitMQStyle = true;
                            } else if (c == '.') {
                                oss << "\\.";
                            } else {
                                oss << c;
                            }
                        }
                        if (rabbitMQStyle) {
                            return std::regex {oss.str()};
                        } else {
                            return s;
                        }
                    }
                }
            }
        }
    };

    inline constexpr char const * multiTransportListenerDefaultTopic(MultiTransportBroadcastListenerConnectionType connType) {
        switch (connType) {
        case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
            return "#";
            break;
        case MultiTransportBroadcastListenerConnectionType::Redis:
            return "*";
            break;
        default:
            return "";
            break;
        }
    }

    template <class Env, class T>
    class MultiTransportBroadcastListener final :
        public infra::RealTimeApp<Env>::template AbstractIntegratedOnOrderFacilityWithExternalEffects<
            MultiTransportBroadcastListenerInput
            , MultiTransportBroadcastListenerOutput
            , basic::TypedDataWithTopic<T>
        >
        , public infra::IControllableNode<Env>
    {
    private:
        using M = infra::RealTimeApp<Env>;
        using Parent = typename M::template AbstractIntegratedOnOrderFacilityWithExternalEffects<
            MultiTransportBroadcastListenerInput
            , MultiTransportBroadcastListenerOutput
            , basic::TypedDataWithTopic<T>
        >;
        using FacilityParent = typename M::template AbstractOnOrderFacility<
            MultiTransportBroadcastListenerInput
            , MultiTransportBroadcastListenerOutput
        >;
        using ImporterParent = typename M::template AbstractImporter<
            basic::TypedDataWithTopic<T>
        >;
        std::optional<WireToUserHook> wireToUserHook_; 

        using SubscriptionRecKey = std::tuple<MultiTransportBroadcastListenerConnectionType, uint32_t>;
        std::map<SubscriptionRecKey, MultiTransportBroadcastListenerAddSubscription> subscriptions_;
        std::map<MultiTransportBroadcastListenerAddSubscription, SubscriptionRecKey> restartInfo_;
        std::mutex subscriptionsMutex_, controlMutex_;
        std::atomic<bool> stopped_;
  
        void actuallyHandle(typename M::template InnerData<typename M::template Key<MultiTransportBroadcastListenerInput>> &&input) {
            Env *env = input.environment;
            typename Env::IDType id = std::move(input.timedData.value.id());
            TM_INFRA_FACILITY_TRACER_WITH_SUFFIX(env, ":setup");
            std::visit([this,env,&id](auto &&x) {
                using X = std::decay_t<decltype(x)>;
                std::ostringstream oss;
                oss << "[MultiTransportBroadcastListner::actuallyHandle] handling input " << x;
                env->log(infra::LogLevel::Info, oss.str());
                if constexpr (std::is_same_v<X, MultiTransportBroadcastListenerAddSubscription>) {
                    if (stopped_) {
                        std::lock_guard<std::mutex> _(subscriptionsMutex_);
                        restartInfo_.insert({x, {x.connectionType, -1}});
                        return;
                    }
                    switch (x.connectionType) {
                    case MultiTransportBroadcastListenerConnectionType::Multicast:
                        if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                            auto *component = static_cast<multicast::MulticastComponent *>(env);
                            auto actualHook = wireToUserHook_;
                            if (!actualHook) {
                                actualHook = DefaultHookFactory<Env>::template incomingHook<T>(env);
                            }
                            auto res = component->multicast_addSubscriptionClient(
                                x.connectionLocator
                                , MultiTransportBroadcastListenerTopicHelper<multicast::MulticastComponent>::parseTopic(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    TM_INFRA_IMPORTER_TRACER_WITH_SUFFIX(env, ":data");
                                    T t;
                                    auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                                    if (tRes) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(t)}));
                                    }
                                }
                                , actualHook
                            );
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.insert({{x.connectionType, res}, x});
                            }
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportBroadcastListner::actuallyHandle] trying to set up multicast channel " << x.connectionLocator << " but multicast is unsupported in the environment";
                            env->log(infra::LogLevel::Warning, errOss.str());
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                        if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                            auto *component = static_cast<rabbitmq::RabbitMQComponent *>(env);
                            auto actualHook = wireToUserHook_;
                            if (!actualHook) {
                                actualHook = DefaultHookFactory<Env>::template incomingHook<T>(env);
                            }
                            auto res = component->rabbitmq_addExchangeSubscriptionClient(
                                x.connectionLocator
                                , x.topicDescription
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    TM_INFRA_IMPORTER_TRACER_WITH_SUFFIX(env, ":data");
                                    T t;
                                    auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                                    if (tRes) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(t)}));
                                    }
                                }
                                , actualHook
                            );
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.insert({{x.connectionType, res}, x});
                            }
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportBroadcastListner::actuallyHandle] trying to set up rabbitmq channel " << x.connectionLocator << " but rabbitmq is unsupported in the environment";
                            env->log(infra::LogLevel::Warning, errOss.str());
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::Redis:
                        if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                            auto *component = static_cast<redis::RedisComponent *>(env);
                            auto actualHook = wireToUserHook_;
                            if (!actualHook) {
                                actualHook = DefaultHookFactory<Env>::template incomingHook<T>(env);
                            }
                            auto res = component->redis_addSubscriptionClient(
                                x.connectionLocator
                                , MultiTransportBroadcastListenerRedisTopicHelper::redisTopicHelper(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    TM_INFRA_IMPORTER_TRACER_WITH_SUFFIX(env, ":data");
                                    T t;
                                    auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                                    if (tRes) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(t)}));
                                    }
                                }
                                , actualHook
                            );
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.insert({{x.connectionType, res}, x});
                            }
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportBroadcastListner::actuallyHandle] trying to set up redis channel " << x.connectionLocator << " but redis is unsupported in the environment";
                            env->log(infra::LogLevel::Warning, errOss.str());
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::NATS:
                        if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                            auto *component = static_cast<nats::NATSComponent *>(env);
                            auto actualHook = wireToUserHook_;
                            if (!actualHook) {
                                actualHook = DefaultHookFactory<Env>::template incomingHook<T>(env);
                            }
                            auto res = component->nats_addSubscriptionClient(
                                x.connectionLocator
                                , MultiTransportBroadcastListenerNATSTopicHelper::natsTopicHelper(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    TM_INFRA_IMPORTER_TRACER_WITH_SUFFIX(env, ":data");
                                    T t;
                                    auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                                    if (tRes) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(t)}));
                                    }
                                }
                                , actualHook
                            );
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.insert({{x.connectionType, res}, x});
                            }
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportBroadcastListner::actuallyHandle] trying to set up nats channel " << x.connectionLocator << " but nats is unsupported in the environment";
                            env->log(infra::LogLevel::Warning, errOss.str());
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                        if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                            auto *component = static_cast<zeromq::ZeroMQComponent *>(env);
                            auto actualHook = wireToUserHook_;
                            if (!actualHook) {
                                actualHook = DefaultHookFactory<Env>::template incomingHook<T>(env);
                            }
                            auto res = component->zeroMQ_addSubscriptionClient(
                                x.connectionLocator
                                , MultiTransportBroadcastListenerTopicHelper<zeromq::ZeroMQComponent>::parseTopic(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    TM_INFRA_IMPORTER_TRACER_WITH_SUFFIX(env, ":data");
                                    T t;
                                    auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                                    if (tRes) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(t)}));
                                    }
                                }
                                , actualHook
                            );
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.insert({{x.connectionType, res}, x});
                            }
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportBroadcastListner::actuallyHandle] trying to set up zeromq channel " << x.connectionLocator << " but zeromq is unsupported in the environment";
                            env->log(infra::LogLevel::Warning, errOss.str());
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::NNG:
                        if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                            auto *component = static_cast<nng::NNGComponent *>(env);
                            auto actualHook = wireToUserHook_;
                            if (!actualHook) {
                                actualHook = DefaultHookFactory<Env>::template incomingHook<T>(env);
                            }
                            auto res = component->nng_addSubscriptionClient(
                                x.connectionLocator
                                , MultiTransportBroadcastListenerTopicHelper<nng::NNGComponent>::parseTopic(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    TM_INFRA_IMPORTER_TRACER_WITH_SUFFIX(env, ":data");
                                    T t;
                                    auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                                    if (tRes) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(t)}));
                                    }
                                }
                                , actualHook
                            );
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.insert({{x.connectionType, res}, x});
                            }
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportBroadcastListner::actuallyHandle] trying to set up nng channel " << x.connectionLocator << " but nng is unsupported in the environment";
                            env->log(infra::LogLevel::Warning, errOss.str());
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::SharedMemoryBroadcast:
                        if constexpr (std::is_convertible_v<Env *, shared_memory_broadcast::SharedMemoryBroadcastComponent *>) {
                            auto *component = static_cast<shared_memory_broadcast::SharedMemoryBroadcastComponent *>(env);
                            auto actualHook = wireToUserHook_;
                            if (!actualHook) {
                                actualHook = DefaultHookFactory<Env>::template incomingHook<T>(env);
                            }
                            auto res = component->shared_memory_broadcast_addSubscriptionClient(
                                x.connectionLocator
                                , MultiTransportBroadcastListenerTopicHelper<shared_memory_broadcast::SharedMemoryBroadcastComponent>::parseTopic(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    TM_INFRA_IMPORTER_TRACER_WITH_SUFFIX(env, ":data");
                                    T t;
                                    auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                                    if (tRes) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(t)}));
                                    }
                                }
                                , actualHook
                            );
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.insert({{x.connectionType, res}, x});
                            }
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportBroadcastListner::actuallyHandle] trying to set up shared memory broadcast channel " << x.connectionLocator << " but shared memory broadcast is unsupported in the environment";
                            env->log(infra::LogLevel::Warning, errOss.str());
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::WebSocket:
                        if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                            auto *component = static_cast<web_socket::WebSocketComponent *>(env);
                            auto actualHook = wireToUserHook_;
                            if (!actualHook) {
                                actualHook = DefaultHookFactory<Env>::template incomingHook<T>(env);
                            }
                            auto res = component->websocket_addSubscriptionClient(
                                x.connectionLocator
                                , MultiTransportBroadcastListenerTopicHelper<web_socket::WebSocketComponent>::parseTopic(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    TM_INFRA_IMPORTER_TRACER_WITH_SUFFIX(env, ":data");
                                    T t;
                                    auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                                    if (tRes) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(t)}));
                                    }
                                }
                                , actualHook
                            );
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.insert({{x.connectionType, res}, x});
                            }
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportBroadcastListner::actuallyHandle] trying to set up web socket channel " << x.connectionLocator << " but web socket is unsupported in the environment";
                            env->log(infra::LogLevel::Warning, errOss.str());
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::Singlecast:
                        if constexpr (std::is_convertible_v<Env *, singlecast::SinglecastComponent *>) {
                            auto *component = static_cast<singlecast::SinglecastComponent *>(env);
                            auto actualHook = wireToUserHook_;
                            if (!actualHook) {
                                actualHook = DefaultHookFactory<Env>::template incomingHook<T>(env);
                            }
                            auto res = component->singlecast_addSubscriptionClient(
                                x.connectionLocator
                                , MultiTransportBroadcastListenerTopicHelper<singlecast::SinglecastComponent>::parseTopic(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    TM_INFRA_IMPORTER_TRACER_WITH_SUFFIX(env, ":data");
                                    T t;
                                    auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                                    if (tRes) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(t)}));
                                    }
                                }
                                , actualHook
                            );
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.insert({{x.connectionType, res}, x});
                            }
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            std::ostringstream errOss;
                            errOss << "[MultiTransportBroadcastListner::actuallyHandle] trying to set up singlecast channel " << x.connectionLocator << " but singlecast is unsupported in the environment";
                            env->log(infra::LogLevel::Warning, errOss.str());
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                    id
                                    , MultiTransportBroadcastListenerOutput { {
                                        MultiTransportBroadcastListenerAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    default:
                        this->FacilityParent::publish(
                            env
                            , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                                id
                                , MultiTransportBroadcastListenerOutput { {
                                    MultiTransportBroadcastListenerAddSubscriptionResponse {0}
                                } }
                            }
                            , true
                        );
                        break;
                    }
                } else if constexpr (std::is_same_v<X, MultiTransportBroadcastListenerRemoveSubscription>) {
                    if (stopped_) {
                        std::lock_guard<std::mutex> _(subscriptionsMutex_);
                        SubscriptionRecKey k {x.connectionType, x.subscriptionID};
                        for (auto iter = restartInfo_.begin(); iter != restartInfo_.end(); ++iter) {
                            if (iter->second == k) {
                                restartInfo_.erase(iter);
                                break;
                            }
                        }
                        return;
                    }
                    switch (x.connectionType) {
                    case MultiTransportBroadcastListenerConnectionType::Multicast:
                        if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                            static_cast<multicast::MulticastComponent *>(env)
                                ->multicast_removeSubscriptionClient(x.subscriptionID);
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.erase({x.connectionType, x.subscriptionID});
                            }
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                        if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                            static_cast<rabbitmq::RabbitMQComponent *>(env)
                                ->rabbitmq_removeExchangeSubscriptionClient(x.subscriptionID);
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.erase({x.connectionType, x.subscriptionID});
                            }
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::Redis:
                        if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                            static_cast<redis::RedisComponent *>(env)
                                ->redis_removeSubscriptionClient(x.subscriptionID);
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.erase({x.connectionType, x.subscriptionID});
                            }
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::NATS:
                        if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                            static_cast<nats::NATSComponent *>(env)
                                ->nats_removeSubscriptionClient(x.subscriptionID);
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.erase({x.connectionType, x.subscriptionID});
                            }
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                        if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                            static_cast<zeromq::ZeroMQComponent *>(env)
                                ->zeroMQ_removeSubscriptionClient(x.subscriptionID);
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.erase({x.connectionType, x.subscriptionID});
                            }
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::NNG:
                        if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                            static_cast<nng::NNGComponent *>(env)
                                ->nng_removeSubscriptionClient(x.subscriptionID);
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.erase({x.connectionType, x.subscriptionID});
                            }
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::SharedMemoryBroadcast:
                        if constexpr (std::is_convertible_v<Env *, shared_memory_broadcast::SharedMemoryBroadcastComponent *>) {
                            static_cast<shared_memory_broadcast::SharedMemoryBroadcastComponent *>(env)
                                ->shared_memory_broadcast_removeSubscriptionClient(x.subscriptionID);
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.erase({x.connectionType, x.subscriptionID});
                            }
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::WebSocket:
                        if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                            static_cast<web_socket::WebSocketComponent *>(env)
                                ->websocket_removeSubscriptionClient(x.subscriptionID);
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.erase({x.connectionType, x.subscriptionID});
                            }
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::Singlecast:
                        if constexpr (std::is_convertible_v<Env *, singlecast::SinglecastComponent *>) {
                            static_cast<singlecast::SinglecastComponent *>(env)
                                ->singlecast_removeSubscriptionClient(x.subscriptionID);
                            {
                                std::lock_guard<std::mutex> _(subscriptionsMutex_);
                                subscriptions_.erase({x.connectionType, x.subscriptionID});
                            }
                        }
                        break;
                    default:
                        break;
                    }
                    this->FacilityParent::publish(
                        env
                        , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                            id
                            , MultiTransportBroadcastListenerOutput { {
                                MultiTransportBroadcastListenerRemoveSubscriptionResponse {}
                            } }
                        }
                        , true
                    );
                } else if constexpr (std::is_same_v<X, MultiTransportBroadcastListenerRemoveAllSubscriptions>) {
                    std::lock_guard<std::mutex> _(subscriptionsMutex_);
                    for (auto const &x : subscriptions_) {
                        switch (std::get<0>(x.first)) {
                        case MultiTransportBroadcastListenerConnectionType::Multicast:
                            if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                                static_cast<multicast::MulticastComponent *>(env)
                                    ->multicast_removeSubscriptionClient(std::get<1>(x.first));
                            }
                            break;
                        case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                            if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                                static_cast<rabbitmq::RabbitMQComponent *>(env)
                                    ->rabbitmq_removeExchangeSubscriptionClient(std::get<1>(x.first));
                            }
                            break;
                        case MultiTransportBroadcastListenerConnectionType::Redis:
                            if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                                static_cast<redis::RedisComponent *>(env)
                                    ->redis_removeSubscriptionClient(std::get<1>(x.first));
                            }
                            break;
                        case MultiTransportBroadcastListenerConnectionType::NATS:
                            if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                                static_cast<nats::NATSComponent *>(env)
                                    ->nats_removeSubscriptionClient(std::get<1>(x.first));
                            }
                            break;
                        case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                            if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                                static_cast<zeromq::ZeroMQComponent *>(env)
                                    ->zeroMQ_removeSubscriptionClient(std::get<1>(x.first));
                            }
                            break;
                        case MultiTransportBroadcastListenerConnectionType::NNG:
                            if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                                static_cast<nng::NNGComponent *>(env)
                                    ->nng_removeSubscriptionClient(std::get<1>(x.first));
                            }
                            break;
                        case MultiTransportBroadcastListenerConnectionType::SharedMemoryBroadcast:
                            if constexpr (std::is_convertible_v<Env *, shared_memory_broadcast::SharedMemoryBroadcastComponent *>) {
                                static_cast<shared_memory_broadcast::SharedMemoryBroadcastComponent *>(env)
                                    ->shared_memory_broadcast_removeSubscriptionClient(std::get<1>(x.first));
                            }
                            break;
                        case MultiTransportBroadcastListenerConnectionType::WebSocket:
                            if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                                static_cast<web_socket::WebSocketComponent *>(env)
                                    ->websocket_removeSubscriptionClient(std::get<1>(x.first));
                            }
                            break;
                        case MultiTransportBroadcastListenerConnectionType::Singlecast:
                            if constexpr (std::is_convertible_v<Env *, singlecast::SinglecastComponent *>) {
                                static_cast<singlecast::SinglecastComponent *>(env)
                                    ->singlecast_removeSubscriptionClient(std::get<1>(x.first));
                            }
                            break;
                        default:
                            break;
                        }
                    }
                    if (stopped_) {
                        for (auto const &x : subscriptions_) {
                            restartInfo_.insert({x.second, x.first});
                        }
                    }
                    subscriptions_.clear();
                    this->FacilityParent::publish(
                        env
                        , typename M::template Key<MultiTransportBroadcastListenerOutput> {
                            id
                            , MultiTransportBroadcastListenerOutput { {
                                MultiTransportBroadcastListenerRemoveAllSubscriptionsResponse {}
                            } }
                        }
                        , true
                    );
                }
            }, std::move(input.timedData.value.key().value));
        } 
    public:
        MultiTransportBroadcastListener(std::optional<WireToUserHook> wireToUserHook = std::nullopt) 
            : Parent()
            , wireToUserHook_(wireToUserHook) 
            , subscriptions_()
            , restartInfo_()
            , subscriptionsMutex_()
            , controlMutex_()
            , stopped_(false)
        {
        }
        ~MultiTransportBroadcastListener() {
        }
        void start(Env */*env*/) override final {
        }
        void handle(typename M::template InnerData<typename M::template Key<MultiTransportBroadcastListenerInput>> &&input) override final {
            actuallyHandle(std::move(input));
        } 
        void control(Env *env, std::string const &command, std::vector<std::string> const &/*params*/) override final {
            if (command == "stop") {
                std::lock_guard<std::mutex> _(controlMutex_);
                if (!stopped_) {
                    stopped_ = true;
                    actuallyHandle(
                        typename M::template InnerData<typename M::template Key<MultiTransportBroadcastListenerInput>> {
                            env 
                            , {
                                env->now()
                                , M::template keyify<MultiTransportBroadcastListenerInput>(
                                    { MultiTransportBroadcastListenerRemoveAllSubscriptions {} }
                                )
                                , false
                            }
                        }
                    );
                }
            } else if (command == "restart") {
                std::lock_guard<std::mutex> _(controlMutex_);
                if (stopped_) {
                    std::vector<MultiTransportBroadcastListenerAddSubscription> infoCopy;
                    {
                        std::lock_guard<std::mutex> _(subscriptionsMutex_);
                        for (auto const &x : restartInfo_) {
                            infoCopy.push_back(x.first);
                        }
                        restartInfo_.clear();
                    }
                    stopped_ = false;
                    for (auto const &x : infoCopy) {
                        actuallyHandle(
                            typename M::template InnerData<typename M::template Key<MultiTransportBroadcastListenerInput>> {
                                env 
                                , {
                                    env->now()
                                    , M::template keyify<MultiTransportBroadcastListenerInput>(
                                        { x }
                                    )
                                    , false
                                }
                            }
                        );
                    }
                }
            }
        }
    };
} } } }

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace bytedata_utils {
    template <>
    struct RunCBORSerializer<transport::MultiTransportBroadcastListenerConnectionType, void> {
        static std::string apply(transport::MultiTransportBroadcastListenerConnectionType const &x) {
            return RunCBORSerializer<std::string>::apply(
                transport::MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR[static_cast<int>(x)]
            );
        }
        static std::size_t apply(transport::MultiTransportBroadcastListenerConnectionType const &x, char *output) {
            return RunCBORSerializer<std::string>::apply(
                transport::MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR[static_cast<int>(x)]
                , output
            );
        }
        static std::size_t calculateSize(transport::MultiTransportBroadcastListenerConnectionType const &x) {
            return RunCBORSerializer<std::string>::calculateSize(
                transport::MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR[static_cast<int>(x)]
            );
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportBroadcastListenerConnectionType, void> {
        static std::optional<std::tuple<transport::MultiTransportBroadcastListenerConnectionType,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<std::string>::apply(data, start);
            if (t) {
                int ii = 0;
                for (auto const &s : transport::MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR) {
                    if (s == std::get<0>(*t)) {
                        return std::tuple<transport::MultiTransportBroadcastListenerConnectionType,size_t> {
                            static_cast<transport::MultiTransportBroadcastListenerConnectionType>(ii)
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
        static std::optional<size_t> applyInPlace(transport::MultiTransportBroadcastListenerConnectionType &output, std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<std::string>::apply(data, start);
            if (t) {
                int ii = 0;
                for (auto const &s : transport::MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR) {
                    if (s == std::get<0>(*t)) {
                        output = static_cast<transport::MultiTransportBroadcastListenerConnectionType>(ii);
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
    struct RunCBORSerializer<transport::MultiTransportBroadcastListenerAddSubscription, void> {
        static std::string apply(transport::MultiTransportBroadcastListenerAddSubscription const &x) {
            std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            > t {&x.connectionType, &x.connectionLocator, &x.topicDescription};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            >, 3>
                ::apply(t, {
                    "connection_type"
                    , "connection_locator"
                    , "topic_description"
                });
        }
        static std::size_t apply(transport::MultiTransportBroadcastListenerAddSubscription const &x, char *output) {
            std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            > t {&x.connectionType, &x.connectionLocator, &x.topicDescription};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            >, 3>
                ::apply(t, {
                    "connection_type"
                    , "connection_locator"
                    , "topic_description"
                }, output);
        }
        static std::size_t calculateSize(transport::MultiTransportBroadcastListenerAddSubscription const &x) {
            std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            > t {&x.connectionType, &x.connectionLocator, &x.topicDescription};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , transport::ConnectionLocator const *
                , std::string const *
            >, 3>
                ::calculateSize(t, {
                    "connection_type"
                    , "connection_locator"
                    , "topic_description"
                });
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportBroadcastListenerAddSubscription, void> {
        static std::optional<std::tuple<transport::MultiTransportBroadcastListenerAddSubscription,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializerWithNameList<std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType
                , transport::ConnectionLocator
                , std::string
            >, 3>::apply(data, start, {
                    "connection_type"
                    , "connection_locator"
                    , "topic_description"
                });
            if (t) {
                return std::tuple<transport::MultiTransportBroadcastListenerAddSubscription,size_t> {
                    transport::MultiTransportBroadcastListenerAddSubscription {
                        std::move(std::get<0>(std::get<0>(*t)))
                        , std::move(std::get<1>(std::get<0>(*t)))
                        , std::move(std::get<2>(std::get<0>(*t)))
                    }
                    , std::get<1>(*t)
                };
            } else {
                return std::nullopt;
            }
        }
        static std::optional<size_t> applyInPlace(transport::MultiTransportBroadcastListenerAddSubscription &output, std::string_view const &data, size_t start) {
            auto x = std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType *
                , transport::ConnectionLocator *
                , std::string *
            >(&output.connectionType, &output.connectionLocator, &output.topicDescription);
            return RunCBORDeserializerWithNameList<std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType *
                , transport::ConnectionLocator *
                , std::string *
            >, 3>::applyInPlace(x, data, start, {
                    "connection_type"
                    , "connection_locator"
                    , "topic_description"
                });
        }
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportBroadcastListenerRemoveSubscription, void> {
        static std::string apply(transport::MultiTransportBroadcastListenerRemoveSubscription const &x) {
            std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , uint32_t const *
            > t {&x.connectionType, &x.subscriptionID};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , uint32_t const *
            >, 2>
                ::apply(t, {
                    "connection_type"
                    , "subscription_id"
                });
        } 
        static std::size_t apply(transport::MultiTransportBroadcastListenerRemoveSubscription const &x, char *output) {
            std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , uint32_t const *
            > t {&x.connectionType, &x.subscriptionID};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , uint32_t const *
            >, 2>
                ::apply(t, {
                    "connection_type"
                    , "subscription_id"
                }, output);
        }
        static std::size_t calculateSize(transport::MultiTransportBroadcastListenerRemoveSubscription const &x) {
            std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , uint32_t const *
            > t {&x.connectionType, &x.subscriptionID};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType const *
                , uint32_t const *
            >, 2>
                ::calculateSize(t, {
                    "connection_type"
                    , "subscription_id"
                });
        }
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportBroadcastListenerRemoveSubscription, void> {
        static std::optional<std::tuple<transport::MultiTransportBroadcastListenerRemoveSubscription,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializerWithNameList<std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType
                , uint32_t
            >, 2>::apply(data, start, {
                    "connection_type"
                    , "subscription_id"
                });
            if (t) {
                return std::tuple<transport::MultiTransportBroadcastListenerRemoveSubscription,size_t> {
                    transport::MultiTransportBroadcastListenerRemoveSubscription {
                        std::move(std::get<0>(std::get<0>(*t)))
                        , std::move(std::get<1>(std::get<0>(*t)))
                    }
                    , std::get<1>(*t)
                };
            } else {
                return std::nullopt;
            }
        }
        static std::optional<size_t> applyInPlace(transport::MultiTransportBroadcastListenerRemoveSubscription &output, std::string_view const &data, size_t start) {
            auto x = std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType *
                , uint32_t *
            >(&(output.connectionType), &(output.subscriptionID));
            return RunCBORDeserializerWithNameList<std::tuple<
                transport::MultiTransportBroadcastListenerConnectionType *
                , uint32_t *
            >, 2>::applyInPlace(x, data, start, {
                    "connection_type"
                    , "subscription_id"
                });
        }
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportBroadcastListenerAddSubscriptionResponse, void> {
        static std::string apply(transport::MultiTransportBroadcastListenerAddSubscriptionResponse const &x) {
            std::tuple<
                uint32_t const *
            > t {&x.subscriptionID};
            return RunCBORSerializerWithNameList<std::tuple<
                uint32_t const *
            >, 1>
                ::apply(t, {
                    "subscription_id"
                });
        }
        static std::size_t apply(transport::MultiTransportBroadcastListenerAddSubscriptionResponse const &x, char *output) {
            std::tuple<
                uint32_t const *
            > t {&x.subscriptionID};
            return RunCBORSerializerWithNameList<std::tuple<
                uint32_t const *
            >, 1>
                ::apply(t, {
                    "subscription_id"
                }, output);
        }
        static std::size_t calculateSize(transport::MultiTransportBroadcastListenerAddSubscriptionResponse const &x) {
            std::tuple<
                uint32_t const *
            > t {&x.subscriptionID};
            return RunCBORSerializerWithNameList<std::tuple<
                uint32_t const *
            >, 1>
                ::calculateSize(t, {
                    "subscription_id"
                });
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportBroadcastListenerAddSubscriptionResponse, void> {
        static std::optional<std::tuple<transport::MultiTransportBroadcastListenerAddSubscriptionResponse,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializerWithNameList<std::tuple<
                uint32_t
            >, 1>::apply(data, start, {
                    "subscription_id"
                });
            if (t) {
                return std::tuple<transport::MultiTransportBroadcastListenerAddSubscriptionResponse,size_t> {
                    transport::MultiTransportBroadcastListenerAddSubscriptionResponse {
                        std::move(std::get<0>(std::get<0>(*t)))
                    }
                    , std::get<1>(*t)
                };
            } else {
                return std::nullopt;
            }
        }
        static std::optional<size_t> applyInPlace(transport::MultiTransportBroadcastListenerAddSubscriptionResponse &output, std::string_view const &data, size_t start) {
            auto x = std::tuple<
                uint32_t *
            >(&(output.subscriptionID));
            return RunCBORDeserializerWithNameList<std::tuple<
                uint32_t *
            >, 1>::applyInPlace(x, data, start, {
                    "subscription_id"
                });
        }
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse, void> {
        static std::string apply(transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse const &/*x*/) {
            return RunCBORSerializer<VoidStruct>::apply(VoidStruct {});
        }
        static std::size_t apply(transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse const &/*x*/, char *output) {
            return RunCBORSerializer<VoidStruct>::apply(VoidStruct {}, output);
        }
        static std::size_t calculateSize(transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse const &/*x*/) {
            return RunCBORSerializer<VoidStruct>::calculateSize(VoidStruct {});
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse, void> {
        static std::optional<std::tuple<transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<VoidStruct>::apply(data, start);
            if (t) {
                return std::tuple<transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse,size_t> {
                    transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse {}
                    , std::get<1>(*t)
                };
            } else {
                return std::nullopt;
            }
        }
        static std::optional<size_t> applyInPlace(transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse &/*output*/, std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<VoidStruct>::apply(data, start);
            if (t) {
                return std::get<1>(*t);
            } else {
                return std::nullopt;
            }
        }
    };
            
} } } } }

#endif