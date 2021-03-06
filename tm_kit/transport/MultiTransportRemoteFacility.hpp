#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/TraceNodesComponent.hpp>

#include <tm_kit/basic/ByteData.hpp>

#include <tm_kit/transport/multicast/MulticastComponent.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQOnOrderFacility.hpp>
#include <tm_kit/transport/redis/RedisComponent.hpp>
#include <tm_kit/transport/redis/RedisOnOrderFacility.hpp>
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
    };
    inline const std::array<std::string,2> MULTI_TRANSPORT_REMOTE_FACILITY_CONNECTION_TYPE_STR = {
        "rabbitmq"
        , "redis"
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

        std::tuple<bool, std::size_t> registerFacility(Env *env, MultiTransportRemoteFacilityConnectionType connType, ConnectionLocator const &locator, std::string const &description) {
            std::size_t newSize = 0;
            switch (connType) {
            case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    auto *component = static_cast<rabbitmq::RabbitMQComponent *>(env);
                    try {
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
                                    auto processRes = static_cast<ClientSideAbstractIdentityAttacherComponent<Identity,A> *>(env)->process_incoming_data(
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
                            req = [rawReq](basic::ByteDataWithID &&data) {
                                rawReq(std::move(data));
                            };
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
                            newSize = underlyingSenders_.size();
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
                                    auto processRes = static_cast<ClientSideAbstractIdentityAttacherComponent<Identity,A> *>(env)->process_incoming_data(
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
                            newSize = underlyingSenders_.size();
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
                        newSize = underlyingSenders_.size();
                    }
                    std::ostringstream oss;
                    oss << "[MultiTransportRemoteFacility::deregisterFacility] De-registered Redis facility for "
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
        ) {
            if (connType == MultiTransportRemoteFacilityConnectionType::RabbitMQ) {
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    return rabbitmq::RabbitMQOnOrderFacility<Env>::template typedOneShotRemoteCall<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type RabbitMQ not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::Redis) {
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    return redis::RedisOnOrderFacility<Env>::template typedOneShotRemoteCall<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: connection type Redis not supported in environment");
                }
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::call: unknown connection type");
            }
        }
        template <class A, class B>
        static std::future<B> call(
            Env *env
            , std::string const &facilityDescriptor
            , A &&request
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
        ) {
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                return call<A,B>(env, std::get<0>(*parseRes), std::get<1>(*parseRes), std::move(request), hooks, autoDisconnect);
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
        ) {
            if (connType == MultiTransportRemoteFacilityConnectionType::RabbitMQ) {
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    rabbitmq::RabbitMQOnOrderFacility<Env>::template typedOneShotRemoteCallNoReply<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type RabbitMQ not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::Redis) {
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    return redis::RedisOnOrderFacility<Env>::template typedOneShotRemoteCallNoReply<A,B>(
                        env, locator, std::move(request), hooks, autoDisconnect
                    );
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: connection type Redis not supported in environment");
                }
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: unknown connection type");
            }
        }
        template <class A, class B>
        static void callNoReply(
            Env *env
            , std::string const &facilityDescriptor
            , A &&request
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool autoDisconnect = true
        ) {
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                callNoReply<A,B>(env, std::get<0>(*parseRes), std::get<1>(*parseRes), std::move(request), hooks, autoDisconnect);
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::callNoReply: bad facility descriptor '"+facilityDescriptor+"'");
            }
        }
        static void removeClient(
            Env *env
            , MultiTransportRemoteFacilityConnectionType connType
            , ConnectionLocator const &locator
        ) {
            if (connType == MultiTransportRemoteFacilityConnectionType::RabbitMQ) {
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    env->rabbitmq_removeRPCQueueClient(locator);
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: connection type RabbitMQ not supported in environment");
                }
            } else if (connType == MultiTransportRemoteFacilityConnectionType::Redis) {
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    env->redis_removeRPCClient(locator);
                } else {
                    throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: connection type Redis not supported in environment");
                }
            } else {
                throw std::runtime_error("OneShotMultiTransportRemoteFacilityCall::removeClient: unknown connection type");
            }
        }
        static void removeClient(
            Env *env
            , std::string const &facilityDescriptor
        ) {
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                return removeClient(env, std::get<0>(*parseRes), std::get<1>(*parseRes));
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
        ) {
            auto ret = call<A,B>(env, connType, locator, std::move(request), hooks, true);
            if (ret.wait_for(timeOut) == std::future_status::ready) {
                return {std::move(ret.get())};
            } else {
                removeClient(env, connType, locator);
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
        ) {
            auto parseRes = parseMultiTransportRemoteFacilityChannel(facilityDescriptor);
            if (parseRes) {
                return callWithTimeout<A,B>(env, std::get<0>(*parseRes), std::get<1>(*parseRes), std::move(request), timeOut, hooks);
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
                return call<A,B>(
                    env 
                    , iter->second
                    , std::move(request)
                    , hooks 
                    , autoDisconnect
                );
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
                );
            }
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