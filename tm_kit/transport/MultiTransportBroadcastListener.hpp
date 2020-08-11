#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>

#include <tm_kit/basic/ByteData.hpp>

#include <tm_kit/transport/multicast/MulticastComponent.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>
#include <tm_kit/transport/redis/RedisComponent.hpp>
#include <tm_kit/transport/zeromq/ZeroMQComponent.hpp>
#include <tm_kit/transport/nng/NNGComponent.hpp>

#include <type_traits>
#include <regex>

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    struct AllNetworkTransportComponents :
        public multicast::MulticastComponent
        , public rabbitmq::RabbitMQComponent
        , public redis::RedisComponent
        , public zeromq::ZeroMQComponent
        , public nng::NNGComponent
    {};

    enum class MultiTransportBroadcastListenerConnectionType {
        Multicast
        , RabbitMQ
        , Redis 
        , ZeroMQ 
        , NNG
    };
    inline const std::array<std::string,5> MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR = {
        "multicast"
        , "rabbitmq"
        , "redis"
        , "zeromq"
        , "nng"
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
    };
    struct MultiTransportBroadcastListenerRemoveSubscription {
        MultiTransportBroadcastListenerConnectionType connectionType;
        uint32_t subscriptionID;
    };
    struct MultiTransportBroadcastListenerAddSubscriptionResponse {
        uint32_t subscriptionID;
    };
    struct MultiTransportBroadcastListenerRemoveSubscriptionResponse {
    };
    using MultiTransportBroadcastListenerInput = basic::CBOR<
        std::variant<
            MultiTransportBroadcastListenerAddSubscription
            , MultiTransportBroadcastListenerRemoveSubscription
        >
    >;
    using MultiTransportBroadcastListenerOutput = basic::CBOR<
        std::variant<
            MultiTransportBroadcastListenerAddSubscriptionResponse
            , MultiTransportBroadcastListenerRemoveSubscriptionResponse
        >
    >;

    template <class Env, class T>
    class MultiTransportBroadcastListener final :
        public infra::RealTimeApp<Env>::template AbstractIntegratedOnOrderFacilityWithExternalEffects<
            MultiTransportBroadcastListenerInput
            , MultiTransportBroadcastListenerOutput
            , basic::TypedDataWithTopic<T>
        >
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

        template <class Component>
        static std::variant<
            typename Component::NoTopicSelection, std::string, std::regex
        > parseTopic(std::string const &s) {
            if (s == "") {
                return typename Component::NoTopicSelection {};
            } else {
                if (boost::starts_with(s, "r/") && boost::ends_with(s, "/") && s.length() > 3) {
                    return std::regex {s.substr(2, s.length()-3)};
                } else {
                    return s;
                }
            }
        }  
        void actuallyHandle(typename M::template InnerData<typename M::template Key<MultiTransportBroadcastListenerInput>> &&input) {
            Env *env = input.environment;
            typename Env::IDType id = std::move(input.timedData.value.id());
            std::visit([this,env,&id](auto &&x) {
                using X = std::decay_t<decltype(x)>;
                if constexpr (std::is_same_v<X, MultiTransportBroadcastListenerAddSubscription>) {
                    switch (x.connectionType) {
                    case MultiTransportBroadcastListenerConnectionType::Multicast:
                        if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                            auto *component = static_cast<multicast::MulticastComponent *>(env);
                            auto res = component->multicast_addSubscriptionClient(
                                x.connectionLocator
                                , parseTopic<multicast::MulticastComponent>(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    auto t = basic::bytedata_utils::RunDeserializer<T>::apply(d.content);
                                    if (t) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(*t)}));
                                    }
                                }
                                , wireToUserHook_
                            );
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
                            auto res = component->rabbitmq_addExchangeSubscriptionClient(
                                x.connectionLocator
                                , x.topicDescription
                                , [this,env](std::string const &, basic::ByteDataWithTopic &&d) {
                                    auto t = basic::bytedata_utils::RunDeserializer<T>::apply(d.content);
                                    if (t) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(*t)}));
                                    }
                                }
                                , wireToUserHook_
                            );
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
                            auto res = component->redis_addSubscriptionClient(
                                x.connectionLocator
                                , x.topicDescription
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    auto t = basic::bytedata_utils::RunDeserializer<T>::apply(d.content);
                                    if (t) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(*t)}));
                                    }
                                }
                                , wireToUserHook_
                            );
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
                            auto res = component->zeroMQ_addSubscriptionClient(
                                x.connectionLocator
                                , parseTopic<zeromq::ZeroMQComponent>(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    auto t = basic::bytedata_utils::RunDeserializer<T>::apply(d.content);
                                    if (t) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(*t)}));
                                    }
                                }
                                , wireToUserHook_
                            );
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
                            auto res = component->nng_addSubscriptionClient(
                                x.connectionLocator
                                , parseTopic<nng::NNGComponent>(x.topicDescription)
                                , [this,env](basic::ByteDataWithTopic &&d) {
                                    auto t = basic::bytedata_utils::RunDeserializer<T>::apply(d.content);
                                    if (t) {
                                        this->ImporterParent::publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(*t)}));
                                    }
                                }
                                , wireToUserHook_
                            );
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
                    switch (x.connectionType) {
                    case MultiTransportBroadcastListenerConnectionType::Multicast:
                        if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                            static_cast<multicast::MulticastComponent *>(env)
                                ->multicast_removeSubscriptionClient(x.subscriptionID);
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                        if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                            static_cast<rabbitmq::RabbitMQComponent *>(env)
                                ->rabbitmq_removeExchangeSubscriptionClient(x.subscriptionID);
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::Redis:
                        if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                            static_cast<redis::RedisComponent *>(env)
                                ->redis_removeSubscriptionClient(x.subscriptionID);
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                        if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                            static_cast<zeromq::ZeroMQComponent *>(env)
                                ->zeroMQ_removeSubscriptionClient(x.subscriptionID);
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
                }
            }, std::move(input.timedData.value.key().value));
        } 
    public:
        MultiTransportBroadcastListener(std::optional<WireToUserHook> wireToUserHook = std::nullopt) 
            : Parent()
            , wireToUserHook_(wireToUserHook) 
        {
        }
        ~MultiTransportBroadcastListener() {
        }
        void start(Env *env) override final {
        }
        void handle(typename M::template InnerData<typename M::template Key<MultiTransportBroadcastListenerInput>> &&input) override final {
            //These commands are supposed to be sparse, but handling
            //them may involve setting up or closing lower-level communication 
            //channels which would take some time, so we will launch a
            //thread to handle each one
            std::thread th(&MultiTransportBroadcastListener::actuallyHandle, this, std::move(input));
            th.detach();
        } 
    };
} } } }

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace bytedata_utils {
    template <>
    struct RunCBORSerializer<transport::MultiTransportBroadcastListenerConnectionType, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportBroadcastListenerConnectionType const &x) {
            return RunCBORSerializer<std::string>::apply(
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
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportBroadcastListenerAddSubscription, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportBroadcastListenerAddSubscription const &x) {
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
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportBroadcastListenerRemoveSubscription, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportBroadcastListenerRemoveSubscription const &x) {
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
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportBroadcastListenerAddSubscriptionResponse, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportBroadcastListenerAddSubscriptionResponse const &x) {
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
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportBroadcastListenerRemoveSubscriptionResponse const &x) {
            return RunCBORSerializer<VoidStruct>::apply(VoidStruct {});
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
    };
            
} } } } }

#endif