#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_HPP_

#include <tm_kit/infra/RealTimeMonad.hpp>

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
        std::string connectionLocatorDescription;
    };

    using MultiTransportRemoteFacilityActionResult = MultiTransportRemoteFacilityAction;

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
            , typename infra::RealTimeMonad<Env>::template AbstractIntegratedLocalOnOrderFacility<
                A
                , B 
                , MultiTransportRemoteFacilityAction
            >
            , std::conditional_t<
                DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated
                , typename infra::RealTimeMonad<Env>::template AbstractIntegratedVIEOnOrderFacility<
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
        using M = infra::RealTimeMonad<Env>;
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

        std::optional<ByteDataHookPair> hookPair_; 
        std::vector<std::tuple<ConnectionLocator, std::unique_ptr<RequestSender>>> underlyingSenders_;
        SenderMap senderMap_;
        std::mutex mutex_;

        void registerFacility(Env *env, MultiTransportRemoteFacilityConnectionType connType, ConnectionLocator const &locator) {
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
                            , hookPair_
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
                        if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                            this->ImporterParent::publish(
                                M::template pureInnerData<MultiTransportRemoteFacilityActionResult>(
                                    env
                                    , MultiTransportRemoteFacilityActionResult {
                                        MultiTransportRemoteFacilityActionType::Register
                                        , connType
                                        , locator.toSerializationFormat()
                                    }
                                )
                            );
                        }
                    } catch (rabbitmq::RabbitMQComponentException const &) {
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Error registering RabbitMQ facility for "
                            << locator;
                        env->log(infra::LogLevel::Error, oss.str());
                    }
                }
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
                            , hookPair_
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
                        if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                            this->ImporterParent::publish(
                                M::template pureInnerData<MultiTransportRemoteFacilityActionResult>(
                                    env
                                    , MultiTransportRemoteFacilityActionResult {
                                        MultiTransportRemoteFacilityActionType::Register
                                        , connType
                                        , locator.toSerializationFormat()
                                    }
                                )
                            );
                        }
                    } catch (redis::RedisComponentException const &) {
                        std::ostringstream oss;
                        oss << "[MultiTransportRemoteFacility::registerFacility] Error registering Redis facility for "
                            << locator;
                        env->log(infra::LogLevel::Error, oss.str());
                    }
                }
                break;
            default:
                break;
            }
        }
        void deregisterFacility(Env *env, MultiTransportRemoteFacilityConnectionType connType, ConnectionLocator const &locator) {
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
                    if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                        this->ImporterParent::publish(
                            M::template pureInnerData<MultiTransportRemoteFacilityActionResult>(
                                env
                                , MultiTransportRemoteFacilityActionResult {
                                    MultiTransportRemoteFacilityActionType::Deregister
                                    , connType
                                    , locator.toSerializationFormat()
                                }
                            )
                        );
                    }
                }
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
                    if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Designated) {
                        this->ImporterParent::publish(
                            M::template pureInnerData<MultiTransportRemoteFacilityActionResult>(
                                env
                                , MultiTransportRemoteFacilityActionResult {
                                    MultiTransportRemoteFacilityActionType::Deregister
                                    , connType
                                    , locator.toSerializationFormat()
                                }
                            )
                        );
                    }
                }
                break;
            default:
                break;
            }
        }

        void actuallyHandleFacilityAction(typename M::template InnerData<MultiTransportRemoteFacilityAction> &&action) {
            switch (action.timedData.value.actionType) {
            case MultiTransportRemoteFacilityActionType::Register:
                try {
                    registerFacility(action.environment, action.timedData.value.connectionType, ConnectionLocator::parse(action.timedData.value.connectionLocatorDescription));
                } catch (ConnectionLocatorParseError const &) {
                }
                break;
            case MultiTransportRemoteFacilityActionType::Deregister:
                try {
                    deregisterFacility(action.environment, action.timedData.value.connectionType, ConnectionLocator::parse(action.timedData.value.connectionLocatorDescription));
                } catch (ConnectionLocatorParseError const &) {
                }
                break;
            default:
                break;
            }
        }
        void actuallyHandleInput(typename M::template InnerData<typename M::template Key<Input>> &&input) {
            std::lock_guard<std::mutex> _(mutex_);
            if constexpr (DispatchStrategy == MultiTransportRemoteFacilityDispatchStrategy::Random) {
                if (underlyingSenders_.empty()) {
                    return;
                }
                int idx = std::rand()%underlyingSenders_.size();
                basic::ByteData s = { 
                    basic::SerializationActions<M>::template serializeFunc<Input>(
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
                    basic::SerializationActions<M>::template serializeFunc<Input>(
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
        MultiTransportRemoteFacility(std::optional<ByteDataHookPair> const &hookPair)
            : Parent(), hookPair_(hookPair), underlyingSenders_(), senderMap_(), mutex_()
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
                , std::string const *
            > t {&x.actionType, &x.connectionType, &x.connectionLocatorDescription};
            return RunCBORSerializerWithNameList<std::tuple<
                transport::MultiTransportRemoteFacilityActionType const *
                , transport::MultiTransportRemoteFacilityConnectionType const *
                , std::string const *
            >, 3>
                ::apply(t, {
                    "action_type"
                    , "connection_type"
                    , "connection_locator_description"
                });
        }   
    };
    template <>
    struct RunCBORDeserializer<transport::MultiTransportRemoteFacilityAction, void> {
        static std::optional<std::tuple<transport::MultiTransportRemoteFacilityAction,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializerWithNameList<std::tuple<
                transport::MultiTransportRemoteFacilityActionType
                , transport::MultiTransportRemoteFacilityConnectionType
                , std::string
            >, 3>::apply(data, start, {
                    "action_type"
                    , "connection_type"
                    , "connection_locator_description"
                });
            if (t) {
                return std::tuple<transport::MultiTransportRemoteFacilityAction,size_t> {
                    transport::MultiTransportRemoteFacilityAction {
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