#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_SUBSCRIBER_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_SUBSCRIBER_HPP_

#include <tm_kit/infra/RealTimeMonad.hpp>

#include <tm_kit/basic/ByteData.hpp>

#include <tm_kit/transport/multicast/MulticastComponent.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>
#include <tm_kit/transport/redis/RedisComponent.hpp>
#include <tm_kit/transport/zeromq/ZeroMQComponent.hpp>

#include <type_traits>
#include <regex>

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    enum class MultiTransportSubscriberConnectionType {
        Multicast
        , RabbitMQ
        , Redis 
        , ZeroMQ 
    };
    inline const std::array<std::string,4> MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR = {
        "multicast"
        , "rabbitmq"
        , "redis"
        , "zeromq"
    };
    struct MultiTransportSubscriberAddSubscription {
        MultiTransportSubscriberConnectionType connectionType;
        std::string connectionLocatorDescription;
        std::string topicDescription; 
    };
    struct MultiTransportSubscriberRemoveSubscription {
        MultiTransportSubscriberConnectionType connectionType;
        uint32_t subscriptionID;
    };
    struct MultiTransportSubscriberAddSubscriptionResponse {
        uint32_t subscriptionID;
    };
    struct MultiTransportSubscriberRemoveSubscriptionResponse {
    };
    using MultiTransportSubscriberInput = basic::CBOR<
        std::variant<
            MultiTransportSubscriberAddSubscription
            , MultiTransportSubscriberRemoveSubscription
        >
    >;
    using MultiTransportSubscriberOutput = basic::CBOR<
        std::variant<
            MultiTransportSubscriberAddSubscriptionResponse
            , MultiTransportSubscriberRemoveSubscriptionResponse
        >
    >;

    template <class Env, class T>
    class MultiTransportSubscriber final :
        public infra::RealTimeMonad<Env>::template AbstractIntegratedOnOrderFacilityWithExternalEffects<
            MultiTransportSubscriberInput
            , MultiTransportSubscriberOutput
            , basic::TypedDataWithTopic<T>
        >
    {
    private:
        using M = infra::RealTimeMonad<Env>;
        using Parent = typename M::template AbstractIntegratedOnOrderFacilityWithExternalEffects<
            MultiTransportSubscriberInput
            , MultiTransportSubscriberOutput
            , basic::TypedDataWithTopic<T>
        >;
        using FacilityParent = typename M::template AbstractOnOrderFacility<
            MultiTransportSubscriberInput
            , MultiTransportSubscriberOutput
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
        void actuallyHandle(typename M::template InnerData<typename M::template Key<MultiTransportSubscriberInput>> &&input) {
            Env *env = input.environment;
            typename Env::IDType id = std::move(input.timedData.value.id());
            std::visit([this,env,&id](auto &&x) {
                using X = std::decay_t<decltype(x)>;
                if constexpr (std::is_same_v<X, MultiTransportSubscriberAddSubscription>) {
                    ConnectionLocator locator;
                    try {
                        locator = ConnectionLocator::parse(x.connectionLocatorDescription);
                    } catch (ConnectionLocatorParseError const &) {
                        this->FacilityParent::publish(
                            env
                            , typename M::template Key<MultiTransportSubscriberOutput> {
                                id
                                , MultiTransportSubscriberOutput { {
                                    MultiTransportSubscriberAddSubscriptionResponse {0}
                                } }
                            }
                            , true
                        );
                        return;
                    }
                    switch (x.connectionType) {
                    case MultiTransportSubscriberConnectionType::Multicast:
                        if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                            auto *component = static_cast<multicast::MulticastComponent *>(env);
                            auto res = component->multicast_addSubscriptionClient(
                                locator
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
                                , typename M::template Key<MultiTransportSubscriberOutput> {
                                    id
                                    , MultiTransportSubscriberOutput { {
                                        MultiTransportSubscriberAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportSubscriberOutput> {
                                    id
                                    , MultiTransportSubscriberOutput { {
                                        MultiTransportSubscriberAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportSubscriberConnectionType::RabbitMQ:
                        if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                            auto *component = static_cast<rabbitmq::RabbitMQComponent *>(env);
                            auto res = component->rabbitmq_addExchangeSubscriptionClient(
                                locator
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
                                , typename M::template Key<MultiTransportSubscriberOutput> {
                                    id
                                    , MultiTransportSubscriberOutput { {
                                        MultiTransportSubscriberAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportSubscriberOutput> {
                                    id
                                    , MultiTransportSubscriberOutput { {
                                        MultiTransportSubscriberAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportSubscriberConnectionType::Redis:
                        if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                            auto *component = static_cast<redis::RedisComponent *>(env);
                            auto res = component->redis_addSubscriptionClient(
                                locator
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
                                , typename M::template Key<MultiTransportSubscriberOutput> {
                                    id
                                    , MultiTransportSubscriberOutput { {
                                        MultiTransportSubscriberAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportSubscriberOutput> {
                                    id
                                    , MultiTransportSubscriberOutput { {
                                        MultiTransportSubscriberAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    case MultiTransportSubscriberConnectionType::ZeroMQ:
                        if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                            auto *component = static_cast<zeromq::ZeroMQComponent *>(env);
                            auto res = component->zeroMQ_addSubscriptionClient(
                                locator
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
                                , typename M::template Key<MultiTransportSubscriberOutput> {
                                    id
                                    , MultiTransportSubscriberOutput { {
                                        MultiTransportSubscriberAddSubscriptionResponse {res}
                                    } }
                                }
                                , true
                            );
                        } else {
                            this->FacilityParent::publish(
                                env
                                , typename M::template Key<MultiTransportSubscriberOutput> {
                                    id
                                    , MultiTransportSubscriberOutput { {
                                        MultiTransportSubscriberAddSubscriptionResponse {0}
                                    } }
                                }
                                , true
                            );
                        }
                        break;
                    default:
                        this->FacilityParent::publish(
                            env
                            , typename M::template Key<MultiTransportSubscriberOutput> {
                                id
                                , MultiTransportSubscriberOutput { {
                                    MultiTransportSubscriberAddSubscriptionResponse {0}
                                } }
                            }
                            , true
                        );
                        break;
                    }
                } else if constexpr (std::is_same_v<X, MultiTransportSubscriberRemoveSubscription>) {
                    switch (x.connectionType) {
                    case MultiTransportSubscriberConnectionType::Multicast:
                        if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                            static_cast<multicast::MulticastComponent *>(env)
                                ->multicast_removeSubscriptionClient(x.subscriptionID);
                        }
                        break;
                    case MultiTransportSubscriberConnectionType::RabbitMQ:
                        if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                            static_cast<rabbitmq::RabbitMQComponent *>(env)
                                ->rabbitmq_removeExchangeSubscriptionClient(x.subscriptionID);
                        }
                        break;
                    case MultiTransportSubscriberConnectionType::Redis:
                        if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                            static_cast<redis::RedisComponent *>(env)
                                ->redis_removeSubscriptionClient(x.subscriptionID);
                        }
                        break;
                    case MultiTransportSubscriberConnectionType::ZeroMQ:
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
                        , typename M::template Key<MultiTransportSubscriberOutput> {
                            id
                            , MultiTransportSubscriberOutput { {
                                MultiTransportSubscriberRemoveSubscriptionResponse {}
                            } }
                        }
                        , true
                    );
                }
            }, std::move(input.timedData.value.key().value));
        } 
    public:
        MultiTransportSubscriber(std::optional<WireToUserHook> wireToUserHook = std::nullopt) 
            : Parent()
            , wireToUserHook_(wireToUserHook) 
        {
        }
        ~MultiTransportSubscriber() {
        }
        void start(Env *env) override final {
        }
        void handle(typename M::template InnerData<typename M::template Key<MultiTransportSubscriberInput>> &&input) override final {
            //These commands are supposed to be sparse, but handling
            //them may involve setting up or closing lower-level communication 
            //channels which would take some time, so we will launch a
            //thread to handle each one
            std::thread th(&MultiTransportSubscriber::actuallyHandle, this, std::move(input));
            th.detach();
        } 
    };
} } } }

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace bytedata_utils {
    template <>
    struct RunCBORSerializer<transport::MultiTransportSubscriberConnectionType, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportSubscriberConnectionType const &x) {
            return RunCBORSerializer<std::string>::apply(
                transport::MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR[static_cast<int>(x)]
            );
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportSubscriberConnectionType, void> {
        static std::optional<std::tuple<transport::MultiTransportSubscriberConnectionType,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<std::string>::apply(data, start);
            if (t) {
                int ii = 0;
                for (auto const &s : transport::MULTI_TRANSPORT_SUBSCRIBER_CONNECTION_TYPE_STR) {
                    if (s == std::get<0>(*t)) {
                        return std::tuple<transport::MultiTransportSubscriberConnectionType,size_t> {
                            static_cast<transport::MultiTransportSubscriberConnectionType>(ii)
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
    struct RunCBORSerializer<transport::MultiTransportSubscriberAddSubscription, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportSubscriberAddSubscription const &x) {
            std::tuple<
                transport::MultiTransportSubscriberConnectionType const *
                , std::string const *
                , std::string const *
            > t {&x.connectionType, &x.connectionLocatorDescription, &x.topicDescription};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportSubscriberConnectionType const *
                , std::string const *
                , std::string const *
            >, 3>
                ::apply(t, {
                    "connection_type"
                    , "connection_locator_description"
                    , "topic_description"
                });
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportSubscriberAddSubscription, void> {
        static std::optional<std::tuple<transport::MultiTransportSubscriberAddSubscription,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializerWithNameList<std::tuple<
                transport::MultiTransportSubscriberConnectionType
                , std::string
                , std::string
            >, 3>::apply(data, start, {
                    "connection_type"
                    , "connection_locator_description"
                    , "topic_description"
                });
            if (t) {
                return std::tuple<transport::MultiTransportSubscriberAddSubscription,size_t> {
                    transport::MultiTransportSubscriberAddSubscription {
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
    struct RunCBORSerializer<transport::MultiTransportSubscriberRemoveSubscription, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportSubscriberRemoveSubscription const &x) {
            std::tuple<
                transport::MultiTransportSubscriberConnectionType const *
                , uint32_t const *
            > t {&x.connectionType, &x.subscriptionID};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportSubscriberConnectionType const *
                , uint32_t const *
            >, 2>
                ::apply(t, {
                    "connection_type"
                    , "subscription_id"
                });
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportSubscriberRemoveSubscription, void> {
        static std::optional<std::tuple<transport::MultiTransportSubscriberRemoveSubscription,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializerWithNameList<std::tuple<
                transport::MultiTransportSubscriberConnectionType
                , uint32_t
            >, 2>::apply(data, start, {
                    "connection_type"
                    , "subscription_id"
                });
            if (t) {
                return std::tuple<transport::MultiTransportSubscriberRemoveSubscription,size_t> {
                    transport::MultiTransportSubscriberRemoveSubscription {
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
    struct RunCBORSerializer<transport::MultiTransportSubscriberAddSubscriptionResponse, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportSubscriberAddSubscriptionResponse const &x) {
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
    struct RunCBORDeserializer<transport::MultiTransportSubscriberAddSubscriptionResponse, void> {
        static std::optional<std::tuple<transport::MultiTransportSubscriberAddSubscriptionResponse,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializerWithNameList<std::tuple<
                uint32_t
            >, 1>::apply(data, start, {
                    "subscription_id"
                });
            if (t) {
                return std::tuple<transport::MultiTransportSubscriberAddSubscriptionResponse,size_t> {
                    transport::MultiTransportSubscriberAddSubscriptionResponse {
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
    struct RunCBORSerializer<transport::MultiTransportSubscriberRemoveSubscriptionResponse, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportSubscriberRemoveSubscriptionResponse const &x) {
            return RunCBORSerializer<VoidStruct>::apply(VoidStruct {});
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportSubscriberRemoveSubscriptionResponse, void> {
        static std::optional<std::tuple<transport::MultiTransportSubscriberRemoveSubscriptionResponse,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<VoidStruct>::apply(data, start);
            if (t) {
                return std::tuple<transport::MultiTransportSubscriberRemoveSubscriptionResponse,size_t> {
                    transport::MultiTransportSubscriberRemoveSubscriptionResponse {}
                    , std::get<1>(*t)
                };
            } else {
                return std::nullopt;
            }
        }
    };
            
} } } } }

#endif