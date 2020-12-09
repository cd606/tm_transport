#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_MANAGING_UTILS_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_MANAGING_UTILS_HPP_

#include <tm_kit/transport/MultiTransportBroadcastListener.hpp>
#include <tm_kit/transport/HeartbeatMessage.hpp>
#include <tm_kit/transport/multicast/MulticastImporterExporter.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQImporterExporter.hpp>
#include <tm_kit/transport/redis/RedisImporterExporter.hpp>
#include <tm_kit/transport/zeromq/ZeroMQImporterExporter.hpp>
#include <tm_kit/transport/nng/NNGImporterExporter.hpp>

#include <tm_kit/basic/CommonFlowUtils.hpp>
#include <tm_kit/basic/AppRunnerUtils.hpp>

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
             R &r
            , Input const &specs
            , std::string const &prefix
            , std::function<std::optional<WireToUserHook>(std::string const &)> const &hookFactory
            , Output &output
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
                                return {std::move(data.content.value), std::move(data.topic)};
                            }
                        }
                    );
                    r.registerAction(prefix+"/"+spec.name+":transform", transform);
                    outputReceiver(
                        r.execute(transform, std::move(s))
                    );
                };
                setupOneBroadcastListener_internal<RemoveTopic, basic::CBOR<FirstInputType>>(
                    r, spec, prefix, hookFactory, adapter
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
                                , getTopic_internal(std::get<0>(*parsedSpec), spec.topicDescription)
                                , hookFactory(spec.name)
                            );
                            r.registerImporter(prefix+"/"+spec.name, sub);
                        } else {
                            r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Trying to create redis publisher with channel spec '"+spec.channel+"', but redis is unsupported in the environment");
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
                        , getTopic_internal(std::get<0>(*parsed), topicDescription)
                        , hook
                    );
                    r.registerImporter(name, sub);
                    return r.importItem(sub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create redis publisher with channel spec '"+channelSpec+"', but redis is unsupported in the environment");
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
            default:
                throw std::runtime_error("[MultiTransportBroadcastListenerManagingUtils::oneByteDataBroadcastListener] Trying to create unknown-protocol publisher with channel spec '"+channelSpec+"'");
                break;
            }
        }

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
        ) 
            ->  typename R::template Source<basic::TypedDataWithTopic<InputType>>
        {
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
        ) 
            ->  typename R::template Source<InputType>
        {
            auto s = setupBroadcastListenerWithTopicThroughHeartbeat<InputType>(
                r, std::move(heartbeatSource), serverNameRE, broadcastSourceLookupName, broadcastTopic, prefix, hook
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

} } } }

#endif