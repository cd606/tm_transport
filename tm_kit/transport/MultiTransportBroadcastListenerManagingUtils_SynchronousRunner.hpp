#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_MANAGING_UTILS_SYNCHRONOUS_RUNNER_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_MANAGING_UTILS_SYNCHRONOUS_RUNNER_HPP_

#include <tm_kit/infra/SynchronousRunner.hpp>
#include <tm_kit/transport/MultiTransportBroadcastListenerManagingUtils.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class M>
    class MultiTransportBroadcastListenerManagingUtils<
        infra::SynchronousRunner<M>
    > {
    private:
        static std::string getTopic_internal(MultiTransportBroadcastListenerConnectionType connType, std::optional<std::string> const &topic) {
            if (topic) {
                return *topic;
            } else {
                return multiTransportListenerDefaultTopic(connType);
            }
        }
    public:
        using R = infra::SynchronousRunner<M>;
        using Env = typename M::EnvironmentType;
        
        template <class InputType, bool DontWrap = false>
        static auto oneBroadcastListenerWithTopic(
            infra::SynchronousRunner<M> &r
            , MultiTransportBroadcastListenerSpec<InputType> const &spec
            , std::function<std::optional<WireToUserHook>(std::string const &)> const &hookFactory
        ) -> typename R::template ImporterPtr<basic::TypedDataWithTopic<
                std::conditional_t<
                    (DontWrap || basic::bytedata_utils::DirectlySerializableChecker<InputType>::IsDirectlySerializable())
                    , InputType
                    , basic::CBOR<InputType>
                >
            >>
        {
            using DataType = std::conditional_t<
                basic::bytedata_utils::DirectlySerializableChecker<InputType>::IsDirectlySerializable()
                , InputType
                , basic::CBOR<InputType>
            >;
            auto parsedSpec = parseMultiTransportBroadcastChannel(spec.channel);
            typename R::template ImporterPtr<basic::TypedDataWithTopic<DataType>> sub {};
            if (parsedSpec) {
                switch (std::get<0>(*parsedSpec)) {
                case MultiTransportBroadcastListenerConnectionType::Multicast:
                    if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                        sub = multicast::MulticastImporterExporter<Env>::template createTypedImporter<DataType>(
                            std::get<1>(*parsedSpec)
                            , MultiTransportBroadcastListenerTopicHelper<multicast::MulticastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                            , hookFactory(spec.name)
                        );
                    } else {
                        r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneBroadcastListenerWithTopic] Trying to create multicast publisher with channel spec '"+spec.channel+"', but multicast is unsupported in the environment");
                    }
                    break;
                case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                    if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                        sub = rabbitmq::RabbitMQImporterExporter<Env>::template createTypedImporter<DataType>(
                            std::get<1>(*parsedSpec)
                            , getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription)
                            , hookFactory(spec.name)
                        );
                    } else {
                        r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneBroadcastListenerWithTopic] Trying to create rabbitmq publisher with channel spec '"+spec.channel+"', but rabbitmq is unsupported in the environment");
                    }
                    break;
                case MultiTransportBroadcastListenerConnectionType::Redis:
                    if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                        sub = redis::RedisImporterExporter<Env>::template createTypedImporter<DataType>(
                            std::get<1>(*parsedSpec)
                            , getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription)
                            , hookFactory(spec.name)
                        );
                    } else {
                        r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneBroadcastListenerWithTopic] Trying to create redis publisher with channel spec '"+spec.channel+"', but redis is unsupported in the environment");
                    }
                    break;
                case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                    if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                        sub = zeromq::ZeroMQImporterExporter<Env>::template createTypedImporter<DataType>(
                            std::get<1>(*parsedSpec)
                            , MultiTransportBroadcastListenerTopicHelper<zeromq::ZeroMQComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                            , hookFactory(spec.name)
                        );
                    } else {
                        r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneBroadcastListenerWithTopic] Trying to create zeromq publisher with channel spec '"+spec.channel+"', but zeromq is unsupported in the environment");
                    }
                    break;
                case MultiTransportBroadcastListenerConnectionType::NNG:
                    if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                        sub = nng::NNGImporterExporter<Env>::template createTypedImporter<DataType>(
                            std::get<1>(*parsedSpec)
                            , MultiTransportBroadcastListenerTopicHelper<nng::NNGComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                            , hookFactory(spec.name)
                        );
                    } else {
                        r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneBroadcastListenerWithTopic] Trying to create nng publisher with channel spec '"+spec.channel+"', but nng is unsupported in the environment");
                    }
                    break;
                case MultiTransportBroadcastListenerConnectionType::SharedMemoryBroadcast:
                    if constexpr (std::is_convertible_v<Env *, shared_memory_broadcast::SharedMemoryBroadcastComponent *>) {
                        sub = shared_memory_broadcast::SharedMemoryBroadcastImporterExporter<Env>::template createTypedImporter<DataType>(
                            std::get<1>(*parsedSpec)
                            , MultiTransportBroadcastListenerTopicHelper<shared_memory_broadcast::SharedMemoryBroadcastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                            , hookFactory(spec.name)
                        );
                    } else {
                        r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneBroadcastListenerWithTopic] Trying to create shared memory broadcast publisher with channel spec '"+spec.channel+"', but shared memory broadcast is unsupported in the environment");
                    }
                    break;
                case MultiTransportBroadcastListenerConnectionType::WebSocket:
                    if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                        sub = web_socket::WebSocketImporterExporter<Env>::template createTypedImporter<DataType>(
                            std::get<1>(*parsedSpec)
                            , MultiTransportBroadcastListenerTopicHelper<web_socket::WebSocketComponent>::parseTopic(getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription))
                            , hookFactory(spec.name)
                        );
                    } else {
                        r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneBroadcastListenerWithTopic] Trying to create websocket publisher with channel spec '"+spec.channel+"', but websocket is unsupported in the environment");
                    }
                    break;
                default:
                    r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneBroadcastListenerWithTopic] Trying to create unknown-protocol publisher with channel spec '"+spec.channel+"'");
                    break;
                }
            } else {
                r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneBroadcastListenerWithTopic] Unrecognized spec channel '"+spec.channel+"'");
            }
            if (!sub) {
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneBroadcastListenerWithTopic] cannot create the importer");
            }
            return sub;
        }

        //Since the two "oneBroadcastListener" calls are supposed to be used
        //with quasi-constant channel specification string (i.e. one that does
        //not come from network), it is preferable to have them throw when the
        //specification is unsupported
        template <class InputType, bool DontWrap = false>
        static auto oneBroadcastListenerWithTopic(
            infra::SynchronousRunner<M> &r
            , std::string const &channelSpec
            , std::optional<std::string> const &topicDescription = std::nullopt
            , std::optional<WireToUserHook> hook = std::nullopt
        ) -> typename R::template ImporterPtr<basic::TypedDataWithTopic<
                std::conditional_t<
                    (DontWrap || basic::bytedata_utils::DirectlySerializableChecker<InputType>::IsDirectlySerializable())
                    , InputType
                    , basic::CBOR<InputType>
                >
            >>
        {
            return oneBroadcastListenerWithTopic<InputType, DontWrap>(
                r 
                , { MultiTransportBroadcastListenerSpec<InputType> {
                    "", channelSpec, topicDescription
                } }
                , [hook](std::string const &) -> std::optional<WireToUserHook> {
                    return hook;
                }
            );
        }

        template <template<class...> class ProtocolWrapper, class InputType>
        static auto oneBroadcastListenerWithTopicWithProtocol(
            infra::SynchronousRunner<M> &r
            , std::string const &channelSpec
            , std::optional<std::string> const &topicDescription = std::nullopt
            , std::optional<WireToUserHook> hook = std::nullopt
        ) -> typename R::template ImporterPtr<basic::TypedDataWithTopic<InputType>>
        {
            if constexpr (std::is_same_v<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                ProtocolWrapper, InputType
            >, InputType>) {
                return oneBroadcastListenerWithTopic<InputType, true>(
                    r 
                    , channelSpec 
                    , topicDescription
                    , hook
                );
            } else {
                auto baseImporter = oneBroadcastListenerWithTopic<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                        ProtocolWrapper, InputType
                    >, true
                >(
                    r 
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
                return M::template composeImporter<
                    basic::TypedDataWithTopic<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                        ProtocolWrapper, InputType
                    >>
                    , basic::TypedDataWithTopic<InputType>
                >(
                    std::move(*baseImporter)
                    , std::move(*converter)
                );
            }
        }
        
        static auto oneByteDataBroadcastListener(
            infra::SynchronousRunner<M> &r
            , std::string const &channelSpec
            , std::optional<std::string> const &topicDescription = std::nullopt
            , std::optional<WireToUserHook> hook = std::nullopt
        ) -> typename R::template ImporterPtr<basic::ByteDataWithTopic>
        {
            auto parsed = parseMultiTransportBroadcastChannel(channelSpec);
            if (!parsed) {
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneByteDataBroadcastListener] Unknown channel spec '"+channelSpec+"'");
            }
            switch (std::get<0>(*parsed)) {
            case MultiTransportBroadcastListenerConnectionType::Multicast:
                if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                    auto sub = multicast::MulticastImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<multicast::MulticastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    return sub;
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneByteDataBroadcastListener] Trying to create multicast publisher with channel spec '"+channelSpec+"', but multicast is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    auto sub = rabbitmq::RabbitMQImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , getTopic_internal(std::get<0>(*parsed), topicDescription)
                        , hook
                    );
                    return sub;
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneByteDataBroadcastListener] Trying to create rabbitmq publisher with channel spec '"+channelSpec+"', but rabbitmq is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    auto sub = redis::RedisImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , getTopic_internal(std::get<0>(*parsed), topicDescription)
                        , hook
                    );
                    return sub;
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneByteDataBroadcastListener] Trying to create redis publisher with channel spec '"+channelSpec+"', but redis is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                    auto sub = zeromq::ZeroMQImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<zeromq::ZeroMQComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    return sub;
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneByteDataBroadcastListener] Trying to create zeromq publisher with channel spec '"+channelSpec+"', but zeromq is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::NNG:
                if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                    auto sub = nng::NNGImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<nng::NNGComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    return sub;
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneByteDataBroadcastListener] Trying to create nng publisher with channel spec '"+channelSpec+"', but nng is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::SharedMemoryBroadcast:
                if constexpr (std::is_convertible_v<Env *, shared_memory_broadcast::SharedMemoryBroadcastComponent *>) {
                    auto sub = shared_memory_broadcast::SharedMemoryBroadcastImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<shared_memory_broadcast::SharedMemoryBroadcastComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    return sub;
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneByteDataBroadcastListener] Trying to create shared memory broadcast publisher with channel spec '"+channelSpec+"', but shared memory broadcast is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::WebSocket:
                if constexpr (std::is_convertible_v<Env *, web_socket::WebSocketComponent *>) {
                    auto sub = web_socket::WebSocketImporterExporter<Env>::createImporter(
                        std::get<1>(*parsed)
                        , MultiTransportBroadcastListenerTopicHelper<web_socket::WebSocketComponent>::parseTopic(getTopic_internal(std::get<0>(*parsed), topicDescription))
                        , hook
                    );
                    return sub;
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneByteDataBroadcastListener] Trying to create web socket publisher with channel spec '"+channelSpec+"', but web socket is unsupported in the environment");
                }
                break;
            default:
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::oneByteDataBroadcastListener] Trying to create unknown-protocol publisher with channel spec '"+channelSpec+"'");
                break;
            }
        }

        //we assume that all the broadcasts under one source lookup name are
        //the same, with the same hook
        template <class InputType>
        static auto setupBroadcastListenerWithTopicThroughHeartbeat(
            infra::SynchronousRunner<M> &r
            , std::string const &heartbeatChannelSpec
            , std::optional<std::string> const &heartbeatChannelTopicDescription
            , std::regex const &serverNameRE
            , std::string const &broadcastSourceLookupName
            , std::string const &broadcastTopic
            , std::optional<WireToUserHook> const &dataHook = std::nullopt
            , std::optional<WireToUserHook> const &heartbeatHook = std::nullopt
        ) 
            ->  typename R::template ImporterPtr<basic::TypedDataWithTopic<
                std::conditional_t<
                    basic::bytedata_utils::DirectlySerializableChecker<InputType>::IsDirectlySerializable()
                    , InputType
                    , basic::CBOR<InputType>
                >
            >>
        {
            auto importer = oneBroadcastListenerWithTopic<HeartbeatMessage>(
                r, heartbeatChannelSpec, heartbeatChannelTopicDescription, heartbeatHook
            );
            auto heartbeatMsg = r.importItemUntil(
                importer 
                , [serverNameRE,broadcastSourceLookupName](typename M::template InnerData<basic::TypedDataWithTopic<HeartbeatMessage>> const &h) {
                    if (!std::regex_match(h.timedData.value.content.senderDescription(), serverNameRE)) {
                        return false;
                    }
                    auto iter = h.timedData.value.content.broadcastChannels().find(broadcastSourceLookupName);
                    return (iter != h.timedData.value.content.broadcastChannels().end() && !iter->second.empty());
                }
            )->back();
            if (!std::regex_match(heartbeatMsg.timedData.value.content.senderDescription(), serverNameRE)) {
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::setupBroadcastListenerWithTopicThroughHeartbeat] Cannot find heartbeat for server that matches the RE");
            }
            auto iter = heartbeatMsg.timedData.value.content.broadcastChannels().find(broadcastSourceLookupName);
            if (iter == heartbeatMsg.timedData.value.content.broadcastChannels().end() || iter->second.empty()) {
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils (Synchronous Runner)::setupBroadcastListenerWithTopicThroughHeartbeat] Cannot find heartbeat entry for source '"+broadcastSourceLookupName+"'");
            }
            auto spec = *(iter->second.begin());
            return oneBroadcastListenerWithTopic<InputType>(
                r, spec, broadcastTopic, dataHook
            );
        }
    };

} } } }

#endif