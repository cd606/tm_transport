#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>

#include <tm_kit/basic/ByteData.hpp>

#include <tm_kit/transport/multicast/MulticastComponent.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>
#include <tm_kit/transport/redis/RedisComponent.hpp>
#include <tm_kit/transport/zeromq/ZeroMQComponent.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>

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
    };
    inline const std::array<std::string,2> MULTI_TRANSPORT_REMOTE_FACILITY_CONNECTION_TYPE_STR = {
        "rabbitmq"
        , "redis"
    };
    
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

    template <class Env, class A, class B, class Identity=void
        , MultiTransportRemoteFacilityDispatchStrategy DispatchStrategy 
            = MultiTransportRemoteFacilityDispatchStrategy::Random
    >
    class MultiTransportRemoteFacility final :
        public std::conditional_t<
            DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random
            , typename infra::RealTimeApp<Env>::template AbstractIntegratedLocalOnOrderFacility<
                A
                , B 
                , MultiTransportRemoteFacilityAction
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
            , typename M::template AbstractIntegratedLocalOnOrderFacility<
                A
                , B 
                , MultiTransportRemoteFacilityAction
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
            MultiTransportRemoteFacilityActionResult
        >;
        using RequestSender = std::function<void(basic::ByteDataWithID &&)>;

        using SenderMap = std::conditional_t<
            DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated
            , std::unordered_map<ConnectionLocator, RequestSender *>
            , bool
        >;

        std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> hookPairFactory_; 
        std::vector<std::tuple<ConnectionLocator, std::unique_ptr<RequestSender>>> underlyingSenders_;
        SenderMap senderMap_;
        std::mutex mutex_;

        bool registerFacility(Env *env, MultiTransportRemoteFacilityConnectionType connType, ConnectionLocator const &locator, std::string const &description) {
            switch (connType) {
            case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    auto *component = static_cast<rabbitmq::RabbitMQComponent *>(env);
                    try {
                        auto rawReq = component->rabbitmq_setRPCQueueClient(
                            locator
                            , [this,env](std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                                bool isFinal = false;
                                if (contentEncoding == "with_final") {
                                    auto l = data.content.length();
                                    if (l != 0) {
                                        isFinal = (data.content[l-1] != (char) 0);
                                        data.content.resize(l-1);                                
                                    }
                                }
                                auto result = basic::bytedata_utils::RunDeserializer<Output>::apply(data.content);
                                if (!result) {
                                    return;
                                }
                                this->FacilityParent::publish(
                                    env
                                    , typename M::template Key<Output> {
                                        Env::id_from_string(data.id)
                                        , std::move(*result)
                                    }
                                    , isFinal
                                );
                            }
                            , hookPairFactory_(description, locator)
                        );
                        RequestSender req;
                        if constexpr (std::is_same_v<Identity,void>) {
                            req = [rawReq](basic::ByteDataWithID &&data) {
                                rawReq("with_final", std::move(data));
                            };
                        } else {
                            static_assert(std::is_convertible_v<Env *, ClientSideAbstractIdentityAttacherComponent<Identity,A> *>
                                        , "the client side identity attacher must be present");
                            auto *attacher = static_cast<ClientSideAbstractIdentityAttacherComponent<Identity,A> *>(env);
                            req = [attacher,rawReq](basic::ByteDataWithID &&data) {
                                rawReq("with_final", basic::ByteDataWithID {
                                    std::move(data.id)
                                    , attacher->attach_identity(basic::ByteData {std::move(data.content)}).content
                                });
                            };
                        }
                        {
                            std::lock_guard<std::mutex> _(mutex_);
                            underlyingSenders_.push_back({locator, std::make_unique<RequestSender>(std::move(req))});
                            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                                senderMap_.insert({locator, std::get<1>(underlyingSenders_.back()).get()});
                            }
                        }
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Registered RabbitMQ facility for "
                            << locator;
                        env->log(infra::LogLevel::Info, oss.str());
                    } catch (rabbitmq::RabbitMQComponentException const &) {
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Error registering RabbitMQ facility for "
                            << locator;
                        env->log(infra::LogLevel::Error, oss.str());
                    }
                }
                return true;
                break;
            case MultiTransportRemoteFacilityConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    auto *component = static_cast<redis::RedisComponent *>(env);
                    try {
                        auto rawReq = component->redis_setRPCClient(
                            locator
                            , [env]() {
                                return Env::id_to_string(env->new_id());
                            }
                            , [this,env](basic::ByteDataWithID &&data) {
                                bool isFinal = false;
                                auto l = data.content.length();
                                if (l != 0) {
                                    isFinal = (data.content[l-1] != (char) 0);
                                    data.content.resize(l-1);                                
                                }
                                auto result = basic::bytedata_utils::RunDeserializer<Output>::apply(data.content);
                                if (!result) {
                                    return;
                                }
                                this->FacilityParent::publish(
                                    env
                                    , typename M::template Key<Output> {
                                        Env::id_from_string(data.id)
                                        , std::move(*result)
                                    }
                                    , isFinal
                                );
                            }
                            , hookPairFactory_(description, locator)
                        );
                        RequestSender req;
                        if constexpr (std::is_same_v<Identity,void>) {
                            req = rawReq;
                        } else {
                            static_assert(std::is_convertible_v<Env *, ClientSideAbstractIdentityAttacherComponent<Identity,A> *>
                                        , "the client side identity attacher must be present");
                            auto *attacher = static_cast<ClientSideAbstractIdentityAttacherComponent<Identity,A> *>(env);
                            req = [attacher,rawReq](basic::ByteDataWithID &&data) {
                                rawReq(basic::ByteDataWithID {
                                    std::move(data.id)
                                    , attacher->attach_identity(basic::ByteData {std::move(data.content)}).content
                                });
                            };
                        }
                        {
                            std::lock_guard<std::mutex> _(mutex_);
                            underlyingSenders_.push_back({locator, std::make_unique<RequestSender>(std::move(req))});
                            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                                senderMap_.insert({locator, std::get<1>(underlyingSenders_.back()).get()});
                            }
                        }
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Registered Redis facility for "
                            << locator;
                        env->log(infra::LogLevel::Info, oss.str());
                    } catch (redis::RedisComponentException const &) {
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Error registering Redis facility for "
                            << locator;
                        env->log(infra::LogLevel::Error, oss.str());
                    }
                }
                return true;
                break;
            default:
                return false;
                break;
            }
        }
        bool deregisterFacility(Env *env, MultiTransportRemoteFacilityConnectionType connType, ConnectionLocator const &locator) {
            switch (connType) {
            case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    auto *component = static_cast<rabbitmq::RabbitMQComponent *>(env);
                    component->rabbitmq_removeRPCQueueClient(locator);
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
                    }
                    std::ostringstream oss;
                    oss << "[MultiTransportRemoteFacility::deregisterFacility] De-registered RabbitMQ facility for "
                        << locator;
                    env->log(infra::LogLevel::Info, oss.str());
                }
                return true;
                break;
            case MultiTransportRemoteFacilityConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    auto *component = static_cast<redis::RedisComponent *>(env);
                    component->redis_removeRPCClient(locator);
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
                    }
                    std::ostringstream oss;
                    oss << "[MultiTransportRemoteFacility::deregisterFacility] De-registered Redis facility for "
                        << locator;
                    env->log(infra::LogLevel::Info, oss.str());
                }
                return true;
                break;
            default:
                return false;
                break;
            }
        }

        void actuallyHandleFacilityAction(typename M::template InnerData<MultiTransportRemoteFacilityAction> &&action) {
            bool handled = false;
            switch (action.timedData.value.actionType) {
            case MultiTransportRemoteFacilityActionType::Register:
                handled = registerFacility(action.environment, action.timedData.value.connectionType, action.timedData.value.connectionLocator, action.timedData.value.description);
                break;
            case MultiTransportRemoteFacilityActionType::Deregister:
                handled = deregisterFacility(action.environment, action.timedData.value.connectionType, action.timedData.value.connectionLocator);
                break;
            default:
                handled = false;
                break;
            }
            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                //we ALWAYS publish the action itself as the response
                this->ImporterParent::publish(
                    M::template pureInnerData<MultiTransportRemoteFacilityActionResult>(
                        action.environment
                        , {
                            std::move(action.timedData.value)
                            , handled
                        }
                    )
                );
            }
        }
        void actuallyHandleInput(typename M::template InnerData<typename M::template Key<Input>> &&input) {
            std::lock_guard<std::mutex> _(mutex_);
            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random) {
                if (underlyingSenders_.empty()) {
                    return;
                }
                int sz = underlyingSenders_.size();
                int idx = ((sz==1)?0:(std::rand()%sz));
                basic::ByteData s = { 
                    basic::SerializationActions<M>::template serializeFunc<A>(
                        input.timedData.value.key()
                    ) 
                };
                (*(std::get<1>(underlyingSenders_[idx])))(
                    basic::ByteDataWithID {
                        Env::id_to_string(input.timedData.value.id())
                        , std::move(s.content)
                    }
                );
            } else if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                auto iter = senderMap_.find(std::get<0>(input.timedData.value.key()));
                if (iter == senderMap_.end()) {
                    return;
                }
                basic::ByteData s = { 
                    basic::SerializationActions<M>::template serializeFunc<A>(
                        std::get<1>(input.timedData.value.key())
                    ) 
                };
                (*(iter->second))(
                    basic::ByteDataWithID {
                        Env::id_to_string(input.timedData.value.id())
                        , std::move(s.content)
                    }
                );
            }
        }
    public:
        MultiTransportRemoteFacility(std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> const &hookPairFactory = [](std::string const &, ConnectionLocator const &) {return std::nullopt;})
            : Parent(), hookPairFactory_(hookPairFactory), underlyingSenders_(), senderMap_(), mutex_()
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
            std::thread(
                &MultiTransportRemoteFacility::actuallyHandleFacilityAction
                , this
                , std::move(action)
            ).detach();
        }
        void handle(typename M::template InnerData<typename M::template Key<Input>> &&input) override final {
            std::thread(
                &MultiTransportRemoteFacility::actuallyHandleInput
                , this
                , std::move(input)
            ).detach();
        }
    };

} } } }

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace bytedata_utils {
    template <>
    struct RunCBORSerializer<transport::MultiTransportRemoteFacilityActionType, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportRemoteFacilityActionType const &x) {
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
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportRemoteFacilityConnectionType, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportRemoteFacilityConnectionType const &x) {
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
    };
    template <>
    struct RunCBORSerializer<transport::MultiTransportRemoteFacilityAction, void> {
        static std::vector<uint8_t> apply(transport::MultiTransportRemoteFacilityAction const &x) {
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
    };
    template <>
    struct RunSerializer<transport::MultiTransportRemoteFacilityAction, void> {
        static std::string apply(transport::MultiTransportRemoteFacilityAction const &x) {
            return RunSerializer<CBOR<transport::MultiTransportRemoteFacilityAction>>
                    ::apply(CBOR<transport::MultiTransportRemoteFacilityAction> {x});
        }   
    };
    template <>
    struct RunDeserializer<transport::MultiTransportRemoteFacilityAction, void> {
        static std::optional<transport::MultiTransportRemoteFacilityAction> apply(std::string const &data) {
            auto t = RunDeserializer<CBOR<transport::MultiTransportRemoteFacilityAction>>
                        ::apply(data);
            if (!t) {
                return std::nullopt;
            }
            return t->value;
        }
    };
                
} } } } }

#endif