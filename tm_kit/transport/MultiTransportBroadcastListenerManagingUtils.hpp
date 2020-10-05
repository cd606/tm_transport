#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_MANAGING_UTILS_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_MANAGING_UTILS_HPP_

#include <tm_kit/transport/MultiTransportBroadcastListener.hpp>
#include <tm_kit/transport/HeartbeatMessage.hpp>

#include <tm_kit/basic/CommonFlowUtils.hpp>
#include <tm_kit/basic/AppRunnerUtils.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class InputType>
    struct MultiTransportBroadcastListenerSpec {
        std::string name;
        std::string channel;
        std::string topicDescription;
    }; //InputType is put in the signature to force type safety

    template <class R>
    class MultiTransportBroadcastListenerManagingUtils {
    public:
        using M = typename R::AppType;
        
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
            auto parsedSpec = parseMultiTransportBroadcastChannel(spec.channel);
            if (parsedSpec) {
                auto thisSource = M::template constFirstPushKeyImporter<
                    MultiTransportBroadcastListenerInput
                >(MultiTransportBroadcastListenerInput {
                    MultiTransportBroadcastListenerAddSubscription {
                        std::get<0>(*parsedSpec)
                        , std::get<1>(*parsedSpec)
                        , spec.topicDescription
                    }
                });
                auto listener = M::onOrderFacilityWithExternalEffects(
                    new MultiTransportBroadcastListener<
                        typename M::EnvironmentType
                        , FirstInputType
                    >(
                        hookFactory(spec.name)
                    )
                );
                r.registerImporter(prefix+"/"+spec.name+".key", thisSource);
                r.registerOnOrderFacilityWithExternalEffects(prefix+"/"+spec.name, listener);
                r.placeOrderWithFacilityWithExternalEffectsAndForget(r.importItem(thisSource), listener);
                if constexpr (RemoveTopic) {
                    auto removeTopic = M::template liftPure<basic::TypedDataWithTopic<FirstInputType>>(
                        [](basic::TypedDataWithTopic<FirstInputType> &&d) -> FirstInputType {
                            return std::move(d.content);
                        }
                    );
                    r.registerAction(prefix+"/"+spec.name+".removeTopic", removeTopic);
                    outputReceiver(r.execute(removeTopic, r.facilityWithExternalEffectsAsSource(listener)));
                } else {
                    outputReceiver(r.facilityWithExternalEffectsAsSource(listener));
                }
            } else {
                r.environment()->log(infra::LogLevel::Warning, "[MultiTransportBroadcastListenerManagingUtils::setupBroadcastListeners_internal] Unrecognized spec channel '"+spec.channel+"'");
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

        template <class InputType>
        static auto oneBroadcastListener(
            R &r
            , std::string const &namePrefix
            , std::string const &channelSpec
            , std::string const &topicDescription
            , std::optional<WireToUserHook> hook = std::nullopt
        ) -> std::optional<typename R::template Source<InputType>>
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
            return ret;
        }
        template <class InputType>
        static auto oneBroadcastListenerWithTopic(
            R &r
            , std::string const &namePrefix
            , std::string const &channelSpec
            , std::string const &topicDescription
            , std::optional<WireToUserHook> hook = std::nullopt
        ) -> std::optional<typename R::template Source<basic::TypedDataWithTopic<InputType>>>
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
            return ret;
        }

        //we assume that all the broadcasts under one source lookup name are
        //the same, with the same hook
        template <class InputType>
        static auto setupBroadcastListenerWithTopicThroughHeartbeat(
            R &r
            , typename R::template ConvertibleToSourceoid<HeartbeatMessage> &&heartbeatSource
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
                std::vector<MultiTransportBroadcastListenerInput> operator()(HeartbeatMessage &&msg) {
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
            };
            auto addSubscriptionCreator = M::template liftMulti<HeartbeatMessage>(
                AddSubscriptionCreator(serverNameRE, broadcastSourceLookupName, broadcastTopic)
            );
            r.registerAction(prefix+"/addSubscriptionCreator", addSubscriptionCreator);
            (R::convertToSourceoid(std::move(heartbeatSource)))(r, r.actionAsSink(addSubscriptionCreator));

            auto keyify = M::template kleisli<MultiTransportBroadcastListenerInput>(
                basic::CommonFlowUtilComponents<M>::template keyify<MultiTransportBroadcastListenerInput>()
            );
            r.registerAction(prefix+"/keyify", keyify);
            r.execute(keyify, r.actionAsSource(addSubscriptionCreator));

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
            , typename R::template ConvertibleToSourceoid<HeartbeatMessage> &&heartbeatSource
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