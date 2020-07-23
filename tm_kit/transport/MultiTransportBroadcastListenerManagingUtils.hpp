#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_MANAGING_UTILS_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_LISTENER_MANAGING_UTILS_HPP_

#include <tm_kit/transport/MultiTransportBroadcastListener.hpp>

#include <tm_kit/basic/CommonFlowUtils.hpp>
#include <tm_kit/basic/MonadRunnerUtils.hpp>

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
        using M = typename R::MonadType;
        
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
                    std::get<CurrentIdx>(output) = r.sourceAsSourceoid(r.execute(removeTopic, r.facilityWithExternalEffectsAsSource(listener)));
                } else {
                    std::get<CurrentIdx>(output) = r.sourceAsSourceoid(r.facilityWithExternalEffectsAsSource(listener));
                }
            }
            setupBroadcastListeners_internal<
                RemoveTopic, CurrentIdx+1, Input, Output, RemainingInputTypes...
            >(r, specs, prefix, hookFactory, output);
        }
    public:
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
    };

} } } }

#endif