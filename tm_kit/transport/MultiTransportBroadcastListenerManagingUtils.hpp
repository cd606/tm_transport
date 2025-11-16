#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_MANAGING_UTILS_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_MANAGING_UTILS_HPP_

#include <tm_kit/transport/MultiTransportBroadcastListener.hpp>
#include <tm_kit/transport/HeartbeatMessage.hpp>
#include <tm_kit/transport/multicast/MulticastImporterExporter.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQImporterExporter.hpp>
#include <tm_kit/transport/redis/RedisImporterExporter.hpp>
#include <tm_kit/transport/nats/NATSImporterExporter.hpp>
#include <tm_kit/transport/zeromq/ZeroMQImporterExporter.hpp>
#include <tm_kit/transport/nng/NNGImporterExporter.hpp>
#include <tm_kit/transport/shared_memory_broadcast/SharedMemoryBroadcastImporterExporter.hpp>
#include <tm_kit/transport/websocket/WebSocketImporterExporter.hpp>
#include <tm_kit/transport/singlecast/SinglecastImporterExporter.hpp>

#include <tm_kit/basic/CommonFlowUtils.hpp>
#include <tm_kit/basic/AppRunnerUtils.hpp>
#include <tm_kit/basic/WrapFacilitioidConnectorForSerialization.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class InputType>
    struct MultiTransportBroadcastListenerSpec {
        std::string name;
        std::string channel;
        std::optional<std::string> topicDescription;
    }; //InputType is put in the signature to force type safety

    template <class R>
    class MultiTransportBroadcastListenerManagingUtils {
    private:
        static std::string getTopic_internal(MultiTransportBroadcastListenerConnectionType connType, std::optional<std::string> const &topic) {
            if (topic) {
                return *topic;
            } else {
                return multiTransportListenerDefaultTopic(connType);
            }
        }
    public:
        using M = typename R::AppType;
        using Env = typename R::EnvironmentType;
        
        template <class ... InputTypes>
        using BroadcastListeners = 
            std::tuple<
                typename R::template Sourceoid<InputTypes>...
            >;
        template <class ... InputTypes>
        using BroadcastListenersWithTopic = 
            std::tuple<
                typename R::template Sourceoid<basic::TypedDataWithTopic<InputTypes>>...
            >;
    private:
        template <bool RemoveTopic, int CurrentIdx, class Input, class Output>
        static void setupBroadcastListeners_internal(
             R &/*r*/
            , Input const &/*specs*/
            , std::string const &/*prefix*/
            , std::function<std::optional<WireToUserHook>(std::string const &)> const &/*hookFactory*/
            , Output &/*output*/
        )
        {}
        template <bool RemoveTopic, class FirstInputType>
        static void setupOneBroadcastListener_internal(
            R &r
            , MultiTransportBroadcastListenerSpec<FirstInputType> const &spec
            , std::string const &prefix
            , std::function<std::optional<WireToUserHook>(std::string const &)> const &hookFactory
            , std::function<void(typename R::template Source<std::conditional_t<
                RemoveTopic
                , FirstInputType
                , basic::TypedDataWithTopic<FirstInputType>
            >> &&)> outputReceiver
        ) 
        {
            if constexpr (!basic::bytedata_utils::DirectlySerializableChecker<FirstInputType>::IsDirectlySerializable()) {
                auto adapter = [outputReceiver,&r,prefix,spec](typename R::template Source<std::conditional_t<
                    RemoveTopic
                    , basic::CBOR<FirstInputType>
                    , basic::TypedDataWithTopic<basic::CBOR<FirstInputType>>
                >> &&s) {
                    auto transform = M::template liftPure<std::conditional_t<
                        RemoveTopic
                        , basic::CBOR<FirstInputType>
                        , basic::TypedDataWithTopic<basic::CBOR<FirstInputType>>
                    >>(
                        [outputReceiver](std::conditional_t<
                            RemoveTopic
                            , basic::CBOR<FirstInputType>
                            , basic::TypedDataWithTopic<basic::CBOR<FirstInputType>>
                        > &&data) -> std::conditional_t<
                            RemoveTopic
                            , FirstInputType
                            , basic::TypedDataWithTopic<FirstInputType>
                        > {
                            if constexpr (RemoveTopic) {
                                return std::move(data.value);
                            } else {
                                return {std::move(data.topic), std::move(data.content.value)};
                            }
                        }
                    );
                    r.registerAction(prefix+"/"+spec.name+":transform", transform);
                    outputReceiver(
                        r.execute(transform, std::move(s))
                    );
                };
                setupOneBroadcastListener_internal<RemoveTopic, basic::CBOR<FirstInputType>>(
                    r
                    , MultiTransportBroadcastListenerSpec<basic::CBOR<FirstInputType>> {
                #ifdef _MSC_VER
                        spec.name 
                        , spec.channel
                        , spec.topicDescription
                #else
                        .name = spec.name 
                        , .channel = spec.channel
                        , .topicDescription = spec.topicDescription
                #endif
                    }
                    , prefix
                    , hookFactory
                    , adapter
                );
            } else {
                auto parsedSpec = parseMultiTransportBroadcastChannel(spec.channel);
                typename R::template ImporterPtr<basic::TypedDataWithTopic<FirstInputType>> sub {};
                if (parsedSpec) {
                    switch (std::get<0>(*parsedSpec)) {
                    case MultiTransportBroadcastListenerConnectionType::Multicast:
                        if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                            sub = multicast::MulticastImporterExporter<Env>::template createTypedImporter<FirstInputType>(
                                std::get<1>(*parsedSpec)
                                , MultiTransportBroadcastListenerTopicHelper<multicast::MulticastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                                , hookFactory(spec.name)
                            );
                            r.registerImporter(prefix+"/"+spec.name, sub);
                        } else {
                            r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create multicast publisher with channel spec '"+spec.channel+"', but multicast is unsupported in the environment");
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                        if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                            sub = rabbitmq::RabbitMQImporterExporter<Env>::template createTypedImporter<FirstInputType>(
                                std::get<1>(*parsedSpec)
                                , getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription)
                                , hookFactory(spec.name)
                            );
                            r.registerImporter(prefix+"/"+spec.name, sub);
                        } else {
                            r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create rabbitmq publisher with channel spec '"+spec.channel+"', but rabbitmq is unsupported in the environment");
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::Redis:
                        if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                            sub = redis::RedisImporterExporter<Env>::template createTypedImporter<FirstInputType>(
                                std::get<1>(*parsedSpec)
                                , MultiTransportBroadcastListenerRedisTopicHelper::redisTopicHelper(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                                , hookFactory(spec.name)
                            );
                            r.registerImporter(prefix+"/"+spec.name, sub);
                        } else {
                            r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create redis publisher with channel spec '"+spec.channel+"', but redis is unsupported in the environment");
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::NATS:
                        if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                            sub = nats::NATSImporterExporter<Env>::template createTypedImporter<FirstInputType>(
                                std::get<1>(*parsedSpec)
                                , MultiTransportBroadcastListenerNATSTopicHelper::natsTopicHelper(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                                , hookFactory(spec.name)
                            );
                            r.registerImporter(prefix+"/"+spec.name, sub);
                        } else {
                            r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create nats publisher with channel spec '"+spec.channel+"', but nats is unsupported in the environment");
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                        if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                            sub = zeromq::ZeroMQImporterExporter<Env>::template createTypedImporter<FirstInputType>(
                                std::get<1>(*parsedSpec)
                                , MultiTransportBroadcastListenerTopicHelper<zeromq::ZeroMQComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                                , hookFactory(spec.name)
                            );
                            r.registerImporter(prefix+"/"+spec.name, sub);
                        } else {
                            r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create zeromq publisher with channel spec '"+spec.channel+"', but zeromq is unsupported in the environment");
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::NNG:
                        if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                            sub = nng::NNGImporterExporter<Env>::template createTypedImporter<FirstInputType>(
                                std::get<1>(*parsedSpec)
                                , MultiTransportBroadcastListenerTopicHelper<nng::NNGComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                                , hookFactory(spec.name)
                            );
                            r.registerImporter(prefix+"/"+spec.name, sub);
                        } else {
                            r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create nng publisher with channel spec '"+spec.channel+"', but nng is unsupported in the environment");
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::SharedMemoryBroadcast:
                        if constexpr (std::is_convertible_v<Env *, shared_memory_broadcast::SharedMemoryBroadcastComponent *>) {
                            sub = shared_memory_broadcast::SharedMemoryBroadcastImporterExporter<Env>::template createTypedImporter<FirstInputType>(
                                std::get<1>(*parsedSpec)
                                , MultiTransportBroadcastListenerTopicHelper<shared_memory_broadcast::SharedMemoryBroadcastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                                , hookFactory(spec.name)
                            );
                            r.registerImporter(prefix+"/"+spec.name, sub);
                        } else {
                            r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create shared memory broadcast publisher with channel spec '"+spec.channel+"', but shared memory broadcast is unsupported in the environment");
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::WebSocket:
                        if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                            sub = web_socket::WebSocketImporterExporter<Env>::template createTypedImporter<FirstInputType>(
                                std::get<1>(*parsedSpec)
                                , MultiTransportBroadcastListenerTopicHelper<web_socket::WebSocketComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                                , hookFactory(spec.name)
                            );
                            r.registerImporter(prefix+"/"+spec.name, sub);
                        } else {
                            r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create web socket publisher with channel spec '"+spec.channel+"', but web socket is unsupported in the environment");
                        }
                        break;
                    case MultiTransportBroadcastListenerConnectionType::Singlecast:
                        if constexpr (std::is_convertible_v<Env *, singlecast::SinglecastComponent *>) {
                            sub = singlecast::SinglecastImporterExporter<Env>::template createTypedImporter<FirstInputType>(
                                std::get<1>(*parsedSpec)
                                , MultiTransportBroadcastListenerTopicHelper<singlecast::SinglecastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                                , hookFactory(spec.name)
                            );
                            r.registerImporter(prefix+"/"+spec.name, sub);
                        } else {
                            r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create singlecast publisher with channel spec '"+spec.channel+"', but singlecast is unsupported in the environment");
                        }
                        break;
                    default:
                        r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create unknown-protocol publisher with channel spec '"+spec.channel+"'");
                        break;
                    }
                    if (sub) {
                        if constexpr (RemoveTopic) {
                            auto removeTopic = M::template liftPure<basic::TypedDataWithTopic<FirstInputType>>(
                                [](basic::TypedDataWithTopic<FirstInputType> &&d) -> FirstInputType {
                                    return std::move(d.content);
                                }
                            );
                            r.registerAction(prefix+"/"+spec.name+".removeTopic", removeTopic);
                            outputReceiver(r.execute(removeTopic, r.importItem(sub)));
                        } else {
                            outputReceiver(r.importItem(sub));
                        }
                    }
                } else {
                    r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Unrecognized spec channel '"+spec.channel+"'");
                }
            }
        }
        template <bool RemoveTopic, int CurrentIdx, class Input, class Output, class FirstInputType, class ... RemainingInputTypes>
        static void setupBroadcastListeners_internal(
             R &r
            , Input const &specs
            , std::string const &prefix
            , std::function<std::optional<WireToUserHook>(std::string const &)> const &hookFactory
            , Output &output
        )
        {
            MultiTransportBroadcastListenerSpec<FirstInputType> const &spec = std::get<CurrentIdx>(specs);
            setupOneBroadcastListener_internal<RemoveTopic, FirstInputType>(
                r, spec, prefix, hookFactory
                , [&output](typename R::template Source<std::conditional_t<
                    RemoveTopic
                    , FirstInputType
                    , basic::TypedDataWithTopic<FirstInputType>
                >> &&src) {
                    std::get<CurrentIdx>(output) = R::sourceAsSourceoid(std::move(src));
                }
            );
            setupBroadcastListeners_internal<
                RemoveTopic, CurrentIdx+1, Input, Output, RemainingInputTypes...
            >(r, specs, prefix, hookFactory, output);
        }
    public:
        //Please note that all the multi-transport listener setup functions
        //are permissive, which means, if the spec is wrong, or unsupported in
        //the environment, it is bypassed with only a logging message. This is
        //because these setup functions may be used to create a program that 
        //listens to outside messages and uses those messages to set up its
        //listeners, if a new outside publisher starts and is available on a
        //transport that this program does not support, this should not cause the
        //program to crash.
        template <class ... InputTypes>
        static auto setupBroadcastListeners(
            R &r
            , std::tuple<MultiTransportBroadcastListenerSpec<InputTypes>...> const &specs
            , std::string const &prefix
            , std::function<std::optional<WireToUserHook>(std::string const &)> const &hookFactory
                = [](std::string const &) {return std::nullopt;}
        ) -> BroadcastListeners<InputTypes...>
        {
            BroadcastListeners<InputTypes...> result;
            setupBroadcastListeners_internal<
                true, 0
                , std::tuple<MultiTransportBroadcastListenerSpec<InputTypes>...>
                , BroadcastListeners<InputTypes...>
                , InputTypes...
            >(r, specs, prefix, hookFactory, result);
            return result;
        }
        template <class ... InputTypes>
        static auto setupBroadcastListenersWithTopic(
            R &r
            , std::tuple<MultiTransportBroadcastListenerSpec<InputTypes>...> const &specs
            , std::string const &prefix
            , std::function<std::optional<WireToUserHook>(std::string const &)> const &hookFactory
                = [](std::string const &) {return std::nullopt;}
        ) -> BroadcastListenersWithTopic<InputTypes...>
        {
            BroadcastListenersWithTopic<InputTypes...> result;
            setupBroadcastListeners_internal<
                false, 0
                , std::tuple<MultiTransportBroadcastListenerSpec<InputTypes>...>
                , BroadcastListenersWithTopic<InputTypes...>
                , InputTypes...
            >(r, specs, prefix, hookFactory, result);
            return result;
        }

        //Since the two "oneBroadcastListener" calls are supposed to be used
        //with quasi-constant channel specification string (i.e. one that does
        //not come from network), it is preferable to have them throw when the
        //specification is unsupported
        template <class InputType>
        static auto oneBroadcastListener(
            R &r
            , std::string const &namePrefix
            , std::string const &channelSpec
            , std::optional<std::string> const &topicDescription = std::nullopt
            , std::optional<WireToUserHook> hook = std::nullopt
        ) -> typename R::template Source<InputType>
        {
            std::optional<typename R::template Source<InputType>> ret = std::nullopt;
            setupOneBroadcastListener_internal<true, InputType>(
                r 
                , { MultiTransportBroadcastListenerSpec<InputType> {
                    namePrefix, channelSpec, topicDescription
                } }
                , namePrefix
                , [hook](std::string const &) -> std::optional<WireToUserHook> {
                    return hook;
                }
                , [&ret](typename R::template Source<InputType> &&src) {
                    ret = {std::move(src)};
                }
            );
            if (!ret) {
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneBroadcastListener] Cannot setup listener for channel spec '"+channelSpec+"'");
            }
            return std::move(*ret);
        }
        template <template<class...> class ProtocolWrapper, class InputType>
        static auto oneBroadcastListenerWithProtocol(
            R &r
            , std::string const &namePrefix
            , std::string const &channelSpec
            , std::optional<std::string> const &topicDescription = std::nullopt
            , std::optional<WireToUserHook> hook = std::nullopt
        ) -> typename R::template Source<InputType> {
            if constexpr (std::is_same_v<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                ProtocolWrapper, InputType
            >, InputType>) {
                return oneBroadcastListener<InputType>(
                    r 
                    , namePrefix 
                    , channelSpec 
                    , topicDescription
                    , hook
                );
            } else {
                auto s = oneBroadcastListener<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                        ProtocolWrapper, InputType
                    >
                >(
                    r 
                    , namePrefix 
                    , channelSpec 
                    , topicDescription
                    , hook
                );
                auto converter = M::template liftPure<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                        ProtocolWrapper, InputType
                    >
                >(
                    &(basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>::template extract<InputType>)
                );
                r.registerAction(namePrefix+"/protocolConverter", converter);
                return r.execute(converter, std::move(s));
            }
        }
        template <class InputType>
        static auto oneBroadcastListenerWithTopic(
            R &r
            , std::string const &namePrefix
            , std::string const &channelSpec
            , std::optional<std::string> const &topicDescription = std::nullopt
            , std::optional<WireToUserHook> hook = std::nullopt
        ) -> typename R::template Source<basic::TypedDataWithTopic<InputType>>
        {
            std::optional<typename R::template Source<basic::TypedDataWithTopic<InputType>>> ret = std::nullopt;
            setupOneBroadcastListener_internal<false, InputType>(
                r 
                , { MultiTransportBroadcastListenerSpec<InputType> {
                    namePrefix, channelSpec, topicDescription
                } }
                , namePrefix
                , [hook](std::string const &) -> std::optional<WireToUserHook> {
                    return hook;
                }
                , [&ret](typename R::template Source<basic::TypedDataWithTopic<InputType>> &&src) {
                    ret = {std::move(src)};
                }
            );
            if (!ret) {
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneBroadcastListenerWithTopic] Cannot setup listener for channel spec '"+channelSpec+"'");
            }
            return std::move(*ret);
        }
        template <template<class...> class ProtocolWrapper, class InputType>
        static auto oneBroadcastListenerWithTopicWithProtocol(
            R &r
            , std::string const &namePrefix
            , std::string const &channelSpec
            , std::optional<std::string> const &topicDescription = std::nullopt
            , std::optional<WireToUserHook> hook = std::nullopt
        ) -> typename R::template Source<basic::TypedDataWithTopic<InputType>>
        {
            if constexpr (std::is_same_v<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                ProtocolWrapper, InputType
            >, InputType>) {
                return oneBroadcastListenerWithTopic<InputType>(
                    r 
                    , namePrefix 
                    , channelSpec 
                    , topicDescription
                    , hook
                );
            } else {
                auto s = oneBroadcastListenerWithTopic<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                        ProtocolWrapper, InputType
                    >
                >(
                    r 
                    , namePrefix 
                    , channelSpec 
                    , topicDescription
                    , hook
                );
                auto converter = M::template liftPure<
                    basic::TypedDataWithTopic<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                        ProtocolWrapper, InputType
                    >>
                >(
                    [](basic::TypedDataWithTopic<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                        ProtocolWrapper, InputType
                    >> &&x) -> basic::TypedDataWithTopic<InputType> {
                        return {
                            std::move(x.topic)
                            , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>::template extract<InputType>(
                                std::move(x.content)
                            )
                        };
                    }
                );
                r.registerAction(namePrefix+"/protocolConverter", converter);
                return r.execute(converter, std::move(s));
            }
        }

        static auto oneByteDataBroadcastListener(
            R &r
            , std::string const &name
            , std::string const &channelSpec
            , std::optional<std::string> const &topicDescription = std::nullopt
            , std::optional<WireToUserHook> hook = std::nullopt
        ) -> typename R::template Source<basic::ByteDataWithTopic>
        {
            auto parsed = parseMultiTransportBroadcastChannel(channelSpec);
            if (!parsed) {
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Unknown channel spec '"+channelSpec+"'");
            }
            switch (std::get<0>(*parsed)) {
            case MultiTransportBroadcastListenerConnectionType::Multicast:
                if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                    auto sub = multicast::MulticastImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<multicast::MulticastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    r.registerImporter(name, sub);
                    return r.importItem(sub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create multicast publisher with channel spec '"+channelSpec+"', but multicast is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    auto sub = rabbitmq::RabbitMQImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , getTopic_internal(std::get<0>(*parsed), topicDescription)
                        , hook
                    );
                    r.registerImporter(name, sub);
                    return r.importItem(sub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create rabbitmq publisher with channel spec '"+channelSpec+"', but rabbitmq is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    auto sub = redis::RedisImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerRedisTopicHelper::redisTopicHelper(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    r.registerImporter(name, sub);
                    return r.importItem(sub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create redis publisher with channel spec '"+channelSpec+"', but redis is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::NATS:
                if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                    auto sub = nats::NATSImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerNATSTopicHelper::natsTopicHelper(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    r.registerImporter(name, sub);
                    return r.importItem(sub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create nats publisher with channel spec '"+channelSpec+"', but nats is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                    auto sub = zeromq::ZeroMQImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<zeromq::ZeroMQComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    r.registerImporter(name, sub);
                    return r.importItem(sub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create zeromq publisher with channel spec '"+channelSpec+"', but zeromq is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::NNG:
                if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                    auto sub = nng::NNGImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<nng::NNGComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    r.registerImporter(name, sub);
                    return r.importItem(sub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create nng publisher with channel spec '"+channelSpec+"', but nng is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::SharedMemoryBroadcast:
                if constexpr (std::is_convertible_v<Env *, shared_memory_broadcast::SharedMemoryBroadcastComponent *>) {
                    auto sub = shared_memory_broadcast::SharedMemoryBroadcastImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<shared_memory_broadcast::SharedMemoryBroadcastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    r.registerImporter(name, sub);
                    return r.importItem(sub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create shared memory broadcast publisher with channel spec '"+channelSpec+"', but shared memory broadcast is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::WebSocket:
                if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                    auto sub = web_socket::WebSocketImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<web_socket::WebSocketComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    r.registerImporter(name, sub);
                    return r.importItem(sub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create web socket publisher with channel spec '"+channelSpec+"', but web socket is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::Singlecast:
                if constexpr (std::is_convertible_v<Env *, singlecast::SinglecastComponent *>) {
                    auto sub = singlecast::SinglecastImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<singlecast::SinglecastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    r.registerImporter(name, sub);
                    return r.importItem(sub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create singlecast publisher with channel spec '"+channelSpec+"', but singlecast is unsupported in the environment");
                }
                break;
            default:
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create unknown-protocol publisher with channel spec '"+channelSpec+"'");
                break;
            }
        }
    private:
        class AddSubscriptionCreator {
        private:
            std::unordered_set<std::string> seenLocators_;
            std::mutex mutex_;
            std::regex serverNameRE_;
            std::string lookupName_;
            std::string topic_;
        public:
            AddSubscriptionCreator(std::regex const &serverNameRE, std::string const &lookupName, std::string const &topic) : seenLocators_(), mutex_(), serverNameRE_(serverNameRE), lookupName_(lookupName), topic_(topic) {}
            AddSubscriptionCreator(AddSubscriptionCreator &&c)
                : seenLocators_(std::move(c.seenLocators_)), mutex_(), serverNameRE_(std::move(c.serverNameRE_)), lookupName_(std::move(c.lookupName_)), topic_(std::move(c.topic_)) {}
        private:
            std::vector<MultiTransportBroadcastListenerInput> handleHeartbeat(HeartbeatMessage const &msg) {
                std::vector<MultiTransportBroadcastListenerInput> ret;
                if (!std::regex_match(msg.senderDescription(), serverNameRE_)) {
                    return ret;
                }
                std::lock_guard<std::mutex> _(mutex_);
                auto iter = msg.broadcastChannels().find(lookupName_);
                if (iter != msg.broadcastChannels().end() && !iter->second.empty()) {
                    for (auto const &c : iter->second) {
                        if (seenLocators_.find(c) == seenLocators_.end()) {
                            seenLocators_.insert(c);
                            auto parsed = parseMultiTransportBroadcastChannel(c);
                            if (parsed) {
                                ret.push_back(MultiTransportBroadcastListenerInput {
                                    MultiTransportBroadcastListenerAddSubscription {
                                        std::get<0>(*parsed)
                                        , std::get<1>(*parsed)
                                        , topic_
                                    }
                                });
                            }
                        }
                    }
                }
                return ret;
            }
        public:
            std::vector<MultiTransportBroadcastListenerInput> operator()(HeartbeatMessage &&msg) {
                return handleHeartbeat(msg);
            }
            std::vector<MultiTransportBroadcastListenerInput> operator()(std::shared_ptr<HeartbeatMessage const> &&msg) {
                return handleHeartbeat(*msg);
            }
        };
    public:
        //we assume that all the broadcasts under one source lookup name are
        //the same, with the same hook
        template <class InputType>
        static auto setupBroadcastListenerWithTopicThroughHeartbeat(
            R &r
            , std::variant<
                typename R::template ConvertibleToSourceoid<HeartbeatMessage>
                , typename R::template ConvertibleToSourceoid<std::shared_ptr<HeartbeatMessage const>>
            > &&heartbeatSource
            , std::regex const &serverNameRE
            , std::string const &broadcastSourceLookupName
            , std::string const &broadcastTopic
            , std::string const &prefix
            , std::optional<WireToUserHook> const &hook = std::nullopt
            , std::function<bool(basic::TypedDataWithTopic<InputType> const &)> stopCriteria = {}
        ) 
            ->  typename R::template Source<basic::TypedDataWithTopic<InputType>>
        {
            auto keyify = M::template kleisli<MultiTransportBroadcastListenerInput>(
                basic::CommonFlowUtilComponents<M>::template keyify<MultiTransportBroadcastListenerInput>()
            );
            r.registerAction(prefix+"/keyify", keyify);

            std::visit([&r,&broadcastSourceLookupName,&broadcastTopic,&prefix,&serverNameRE,&keyify](auto &&x) {
                using T = std::decay_t<decltype(x)>;
                if constexpr (std::is_same_v<T, typename R::template ConvertibleToSourceoid<HeartbeatMessage>>) {
                    auto addSubscriptionCreator = M::template liftMulti<HeartbeatMessage>(
                        AddSubscriptionCreator(serverNameRE, broadcastSourceLookupName, broadcastTopic)
                    );
                    r.registerAction(prefix+"/addSubscriptionCreator", addSubscriptionCreator);
                    (R::convertToSourceoid(std::move(x)))(r, r.actionAsSink(addSubscriptionCreator));
                    r.execute(keyify, r.actionAsSource(addSubscriptionCreator));
                } else {
                    auto addSubscriptionCreator = M::template liftMulti<std::shared_ptr<HeartbeatMessage const>>(
                        AddSubscriptionCreator(serverNameRE, broadcastSourceLookupName, broadcastTopic)
                    );
                    r.registerAction(prefix+"/addSubscriptionCreator", addSubscriptionCreator);
                    (R::convertToSourceoid(std::move(x)))(r, r.actionAsSink(addSubscriptionCreator));
                    r.execute(keyify, r.actionAsSource(addSubscriptionCreator));
                }
            }, std::move(heartbeatSource));

            auto listener = M::onOrderFacilityWithExternalEffects(
                new MultiTransportBroadcastListener<
                    typename M::EnvironmentType
                    , InputType
                >(hook)
            );
            r.registerOnOrderFacilityWithExternalEffects(prefix+"/listener", listener);
            r.placeOrderWithFacilityWithExternalEffectsAndForget(r.actionAsSource(keyify), listener);
            if (stopCriteria) {
                auto stopChecker = infra::GenericLift<M>::lift(
                    [stopCriteria](basic::TypedDataWithTopic<InputType> &&data) 
                    -> std::optional<typename M::template Key<MultiTransportBroadcastListenerInput>> {
                        if (stopCriteria(data)) {
                            return typename M::template Key<MultiTransportBroadcastListenerInput> {
                                MultiTransportBroadcastListenerInput {
                                    MultiTransportBroadcastListenerRemoveAllSubscriptions {}
                                }
                            };
                        } else {
                            return std::nullopt;
                        }
                    }
                );
                r.registerAction(prefix+"/stopChecker", stopChecker);
                r.placeOrderWithFacilityWithExternalEffectsAndForget(
                    r.execute(stopChecker, r.facilityWithExternalEffectsAsSource(listener))
                    , listener
                );
            }
            return r.facilityWithExternalEffectsAsSource(listener);
        }

        template <class InputType>
        static auto setupBroadcastListenerThroughHeartbeat(
            R &r
            , std::variant<
                typename R::template ConvertibleToSourceoid<HeartbeatMessage>
                , typename R::template ConvertibleToSourceoid<std::shared_ptr<HeartbeatMessage const>>
            > &&heartbeatSource
            , std::regex const &serverNameRE
            , std::string const &broadcastSourceLookupName
            , std::string const &broadcastTopic
            , std::string const &prefix
            , std::optional<WireToUserHook> const &hook = std::nullopt
            , std::function<bool(basic::TypedDataWithTopic<InputType> const &)> stopCriteria = {}
        ) 
            ->  typename R::template Source<InputType>
        {
            auto s = setupBroadcastListenerWithTopicThroughHeartbeat<InputType>(
                r, std::move(heartbeatSource), serverNameRE, broadcastSourceLookupName, broadcastTopic, prefix, hook, stopCriteria
            );
            auto removeTopic = M::template liftPure<basic::TypedDataWithTopic<InputType>>(
                [](basic::TypedDataWithTopic<InputType> &&d) -> InputType {
                    return std::move(d.content);
                }
            );
            r.registerAction(prefix+"/removeTopic", removeTopic);
            return r.execute(removeTopic, std::move(s));
        }
    };

    template <class Env>
    class MultiTransportBroadcastFirstUpdateQueryManagingUtils {
    private:
        static std::string getTopic_internal(MultiTransportBroadcastListenerConnectionType connType, std::optional<std::string> const &topic) {
            if (topic) {
                return *topic;
            } else {
                return multiTransportListenerDefaultTopic(connType);
            }
        }
    public:
        static std::future<basic::ByteDataWithTopic> fetchFirstUpdateAndDisconnect(
            Env *env
            , std::string const &channelSpec
            , std::optional<std::string> const &topicDescription = std::nullopt
            , std::optional<WireToUserHook> hook = std::nullopt
        )
        {
            auto parsed = parseMultiTransportBroadcastChannel(channelSpec);
            if (!parsed) {
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Unknown channel spec '"+channelSpec+"'");
            }
            switch (std::get<0>(*parsed)) {
            case MultiTransportBroadcastListenerConnectionType::Multicast:
                if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                    return multicast::MulticastImporterExporter<Env>::fetchFirstUpdateAndDisconnect(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<multicast::MulticastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Trying to create multicast publisher with channel spec '"+channelSpec+"', but multicast is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    return rabbitmq::RabbitMQImporterExporter<Env>::fetchFirstUpdateAndDisconnect(
                        env
                        , std::get<1>(*parsed)
                        , getTopic_internal(std::get<0>(*parsed), topicDescription)
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Trying to create rabbitmq publisher with channel spec '"+channelSpec+"', but rabbitmq is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    return redis::RedisImporterExporter<Env>::fetchFirstUpdateAndDisconnect(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerRedisTopicHelper::redisTopicHelper(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Trying to create redis publisher with channel spec '"+channelSpec+"', but redis is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::NATS:
                if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                    return nats::NATSImporterExporter<Env>::fetchFirstUpdateAndDisconnect(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerNATSTopicHelper::natsTopicHelper(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Trying to create nats publisher with channel spec '"+channelSpec+"', but nats is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                    return zeromq::ZeroMQImporterExporter<Env>::fetchFirstUpdateAndDisconnect(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<zeromq::ZeroMQComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Trying to create zeromq publisher with channel spec '"+channelSpec+"', but zeromq is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::NNG:
                if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                    return nng::NNGImporterExporter<Env>::fetchFirstUpdateAndDisconnect(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<nng::NNGComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Trying to create nng publisher with channel spec '"+channelSpec+"', but nng is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::SharedMemoryBroadcast:
                if constexpr (std::is_convertible_v<Env *, shared_memory_broadcast::SharedMemoryBroadcastComponent *>) {
                    return shared_memory_broadcast::SharedMemoryBroadcastImporterExporter<Env>::fetchFirstUpdateAndDisconnect(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<shared_memory_broadcast::SharedMemoryBroadcastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Trying to create shared memory broadcast publisher with channel spec '"+channelSpec+"', but shared memory broadcast is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::WebSocket:
                if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                    return web_socket::WebSocketImporterExporter<Env>::fetchFirstUpdateAndDisconnect(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<web_socket::WebSocketComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Trying to create web socket publisher with channel spec '"+channelSpec+"', but web socket is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::Singlecast:
                if constexpr (std::is_convertible_v<Env *, singlecast::SinglecastComponent *>) {
                    return singlecast::SinglecastImporterExporter<Env>::fetchFirstUpdateAndDisconnect(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<singlecast::SinglecastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Trying to create singlecast publisher with channel spec '"+channelSpec+"', but singlecast is unsupported in the environment");
                }
                break;
            default:
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchFirstUpdateAndDisconnect] Trying to create unknown-protocol publisher with channel spec '"+channelSpec+"'");
                break;
            }
        }

        template <class T>
        static std::future<basic::TypedDataWithTopic<T>> fetchTypedFirstUpdateAndDisconnect(
            Env *env
            , std::string const &channelSpec
            , std::optional<std::string> const &topicDescription = std::nullopt
            , std::function<bool(T const &)> predicate = std::function<bool(T const &)>()
            , std::optional<WireToUserHook> hook = std::nullopt
        )
        {
            auto parsed = parseMultiTransportBroadcastChannel(channelSpec);
            if (!parsed) {
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Unknown channel spec '"+channelSpec+"'");
            }
            switch (std::get<0>(*parsed)) {
            case MultiTransportBroadcastListenerConnectionType::Multicast:
                if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                    return multicast::MulticastImporterExporter<Env>::template fetchTypedFirstUpdateAndDisconnect<T>(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<multicast::MulticastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , predicate
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Trying to create multicast publisher with channel spec '"+channelSpec+"', but multicast is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    return rabbitmq::RabbitMQImporterExporter<Env>::template fetchTypedFirstUpdateAndDisconnect<T>(
                        env
                        , std::get<1>(*parsed)
                        , getTopic_internal(std::get<0>(*parsed), topicDescription)
                        , predicate
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Trying to create rabbitmq publisher with channel spec '"+channelSpec+"', but rabbitmq is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    return redis::RedisImporterExporter<Env>::template fetchTypedFirstUpdateAndDisconnect<T>(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerRedisTopicHelper::redisTopicHelper(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , predicate
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Trying to create redis publisher with channel spec '"+channelSpec+"', but redis is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::NATS:
                if constexpr (std::is_convertible_v<Env *, nats::NATSComponent *>) {
                    return nats::NATSImporterExporter<Env>::template fetchTypedFirstUpdateAndDisconnect<T>(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerNATSTopicHelper::natsTopicHelper(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , predicate
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Trying to create nats publisher with channel spec '"+channelSpec+"', but nats is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                    return zeromq::ZeroMQImporterExporter<Env>::template fetchTypedFirstUpdateAndDisconnect<T>(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<zeromq::ZeroMQComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , predicate
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Trying to create zeromq publisher with channel spec '"+channelSpec+"', but zeromq is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::NNG:
                if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                    return nng::NNGImporterExporter<Env>::template fetchTypedFirstUpdateAndDisconnect<T>(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<nng::NNGComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , predicate
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Trying to create nng publisher with channel spec '"+channelSpec+"', but nng is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::SharedMemoryBroadcast:
                if constexpr (std::is_convertible_v<Env *, shared_memory_broadcast::SharedMemoryBroadcastComponent *>) {
                    return shared_memory_broadcast::SharedMemoryBroadcastImporterExporter<Env>::template fetchTypedFirstUpdateAndDisconnect<T>(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<shared_memory_broadcast::SharedMemoryBroadcastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , predicate
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Trying to create shared memory broadcast publisher with channel spec '"+channelSpec+"', but shared memory broadcast is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::WebSocket:
                if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                    return web_socket::WebSocketImporterExporter<Env>::template fetchTypedFirstUpdateAndDisconnect<T>(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<web_socket::WebSocketComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , predicate
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Trying to create web socket publisher with channel spec '"+channelSpec+"', but web socket is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::Singlecast:
                if constexpr (std::is_convertible_v<Env *, singlecast::SinglecastComponent *>) {
                    return singlecast::SinglecastImporterExporter<Env>::template fetchTypedFirstUpdateAndDisconnect<T>(
                        env
                        , std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<singlecast::SinglecastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , predicate
                        , hook
                    );
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Trying to create singlecast publisher with channel spec '"+channelSpec+"', but singlecast is unsupported in the environment");
                }
                break;
            default:
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::fetchTypedFirstUpdateAndDisconnect] Trying to create unknown-protocol publisher with channel spec '"+channelSpec+"'");
                break;
            }
        }

        static HeartbeatMessage fetchFirstMatchingHeartbeat(
            Env *env
            , std::string const &heartbeatChannelSpec
            , std::string const &heartbeatTopicDescription
            , std::regex const &heartbeatSenderNameRE
            , std::function<bool(HeartbeatMessage const &)> furtherPredicates = {}
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
        ) {
            return fetchTypedFirstUpdateAndDisconnect<HeartbeatMessage>
                (
                    env
                    , heartbeatChannelSpec
                    , heartbeatTopicDescription
                    , [heartbeatSenderNameRE,furtherPredicates](HeartbeatMessage const &m) {
                        if (furtherPredicates) {
                            return std::regex_match(
                                m.senderDescription()
                                , heartbeatSenderNameRE
                            ) && furtherPredicates(m);
                        } else {
                            return std::regex_match(
                                m.senderDescription()
                                , heartbeatSenderNameRE
                            );
                        }
                    }
                    , heartbeatHook
                ).get().content;
        }
        static std::optional<HeartbeatMessage> fetchFirstMatchingHeartbeatWithTimeOut(
            Env *env
            , std::string const &heartbeatChannelSpec
            , std::string const &heartbeatTopicDescription
            , std::regex const &heartbeatSenderNameRE
            , std::chrono::system_clock::duration const &timeOut
            , std::function<bool(HeartbeatMessage const &)> furtherPredicates = {}
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
        ) {
            auto f = fetchTypedFirstUpdateAndDisconnect<HeartbeatMessage>
                (
                    env
                    , heartbeatChannelSpec
                    , heartbeatTopicDescription
                    , [heartbeatSenderNameRE,furtherPredicates](HeartbeatMessage const &m) {
                        if (furtherPredicates) {
                            return std::regex_match(
                                m.senderDescription()
                                , heartbeatSenderNameRE
                            ) && furtherPredicates(m);
                        } else {
                            return std::regex_match(
                                m.senderDescription()
                                , heartbeatSenderNameRE
                            );
                        }
                    }
                    , heartbeatHook
                );
            if (f.wait_for(timeOut) == std::future_status::ready) {
                return {std::move(f.get().content)};
            } else {
                return std::nullopt;
            }
        }

        static std::optional<std::string> getFacilityConnectionLocatorViaHeartbeat(
            Env *env
            , std::string const &heartbeatChannelSpec
            , std::string const &heartbeatTopicDescription
            , std::regex const &heartbeatSenderNameRE
            , std::string const &facilityNameInServer
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
        ) {
            auto h = fetchFirstMatchingHeartbeat(
                env
                , heartbeatChannelSpec
                , heartbeatTopicDescription
                , heartbeatSenderNameRE
                , [facilityNameInServer](HeartbeatMessage const &m) {
                    auto const &f = m.facilityChannels();
                    return (f.find(facilityNameInServer) != f.end());
                }
                , heartbeatHook
            );
            auto const &f = h.facilityChannels();
            auto iter = f.find(facilityNameInServer);
            if (iter == f.end()) {
                return std::nullopt;
            }
            return iter->second;
        }

        static std::vector<std::string> getBroadcastConnectionLocatorsViaHeartbeat(
            Env *env
            , std::string const &heartbeatChannelSpec
            , std::string const &heartbeatTopicDescription
            , std::regex const &heartbeatSenderNameRE
            , std::string const &broadcastSourceLookupName
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
        ) {
            auto h = fetchFirstMatchingHeartbeat(
                env
                , heartbeatChannelSpec
                , heartbeatTopicDescription
                , heartbeatSenderNameRE
                , [broadcastSourceLookupName](HeartbeatMessage const &m) {
                    auto const &b = m.broadcastChannels();
                    auto iter = b.find(broadcastSourceLookupName);
                    return (iter != b.end() && !iter->second.empty());
                }
                , heartbeatHook
            );
            auto const &b = h.broadcastChannels();
            auto iter = b.find(broadcastSourceLookupName);
            if (iter == b.end()) {
                return {};
            }
            return iter->second;
        }
    };
} } } }

#endif