#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_MANAGING_UTILS_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_MANAGING_UTILS_HPP_

#include <tm_kit/transport/MultiTransportRemoteFacility.hpp>
#include <tm_kit/transport/MultiTransportBroadcastListener.hpp>
#include <tm_kit/transport/HeartbeatMessageToRemoteFacilityCommand.hpp>

#include <tm_kit/basic/real_time_clock/ClockComponent.hpp>
#include <tm_kit/basic/real_time_clock/ClockImporter.hpp>
#include <tm_kit/basic/CommonFlowUtils.hpp>
#include <tm_kit/basic/AppRunnerUtils.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class R>
    class MultiTransportRemoteFacilityManagingUtils {
    public:
        template <class ID, class In, class Out>
        class IdentityAndInputAndOutputHolder {
        };
    private:
        template <class Spec>
        class SpecReader {
        };
        template <class ID, class In, class Out>
        class SpecReader<IdentityAndInputAndOutputHolder<ID,In,Out>> {
        public:
            using Identity = ID;
            using Input = In;
            using Output = Out;
        };
        template <class ID, class In, class Out>
        class SpecReader<std::tuple<ID,In,Out>> {
        public:
            using Identity = ID;
            using Input = In;
            using Output = Out;
        };
    public:
        using M = typename R::AppType;

        template <class ... IdentitiesAndInputsAndOutputs>
        using NonDistinguishedRemoteFacilities =
            std::tuple<
                typename R::template FacilitioidConnector<
                    typename SpecReader<IdentitiesAndInputsAndOutputs>::Input
                    , typename SpecReader<IdentitiesAndInputsAndOutputs>::Output
                >...
            >;

    private:
        template <
            int CurrentIdx
            , class Output
        >
        static void setupNonDistinguishedRemoteFacilitiesInternal(
            R &r
            , typename R::template Source<
                std::array<MultiTransportRemoteFacilityAction, CurrentIdx>
            > &&actionSource
            , std::array<std::string, CurrentIdx> const &names
            , std::string const &prefix
            , std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> const &hookPairFactory
            , Output &output
        ) 
        {}

        template <
            int CurrentIdx
            , class Output
            , class FirstRemainingIdentityAndInputAndOutput
            , class ... RemainingIdentitiesAndInputsAndOutputs
        >
        static void setupNonDistinguishedRemoteFacilitiesInternal(
            R &r
            , typename R::template Source<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(RemainingIdentitiesAndInputsAndOutputs)+CurrentIdx+1>
            > &&actionSource
            , std::array<std::string, sizeof...(RemainingIdentitiesAndInputsAndOutputs)+CurrentIdx+1> const &names
            , std::string const &prefix
            , std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> const &hookPairFactory
            , Output &output
        ) {
            auto facility = M::localOnOrderFacility(
                new MultiTransportRemoteFacility<
                    typename M::EnvironmentType
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Input
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Output
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Identity
                    , transport::MultiTransportRemoteFacilityDispatchStrategy::Random
                >(hookPairFactory)
            );
            r.registerLocalOnOrderFacility(
                prefix+"/"+names[CurrentIdx]
                , facility
            );
            auto getOneAction = M::template liftPure<std::array<MultiTransportRemoteFacilityAction, sizeof...(RemainingIdentitiesAndInputsAndOutputs)+CurrentIdx+1>>(
                [](std::array<MultiTransportRemoteFacilityAction, sizeof...(RemainingIdentitiesAndInputsAndOutputs)+CurrentIdx+1> &&x) -> MultiTransportRemoteFacilityAction {
                    return x[CurrentIdx];
                }
            );
            r.registerAction(prefix+"/get_"+std::to_string(CurrentIdx), getOneAction);
            r.connect(
                r.execute(getOneAction, actionSource.clone())
                , r.localFacilityAsSink(facility)
            );
            std::get<CurrentIdx>(output) = R::localFacilityConnector(facility);
            setupNonDistinguishedRemoteFacilitiesInternal<
                (CurrentIdx+1)
                , Output
                , RemainingIdentitiesAndInputsAndOutputs...
            >(
                r 
                , actionSource.clone()
                , names
                , prefix
                , hookPairFactory
                , output
            );
        }

        template <class ... IdentitiesAndInputsAndOutputs>
        static auto setupNonDistinguishedRemoteFacilities(
            R &r
            , typename R::template Source<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(IdentitiesAndInputsAndOutputs)>
            > &&actionSource
            , std::array<std::string, sizeof...(IdentitiesAndInputsAndOutputs)> const &names
            , std::string const &prefix
            , std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> const &hookPairFactory
        ) -> NonDistinguishedRemoteFacilities<IdentitiesAndInputsAndOutputs...>
        {
            if constexpr (sizeof...(IdentitiesAndInputsAndOutputs) < 1) {
                return {};
            } else {
                using Output = NonDistinguishedRemoteFacilities<IdentitiesAndInputsAndOutputs...>;
                Output output;
                setupNonDistinguishedRemoteFacilitiesInternal<
                    0
                    , Output
                    , IdentitiesAndInputsAndOutputs...
                >(
                    r 
                    , std::move(actionSource)
                    , names
                    , prefix
                    , hookPairFactory
                    , output
                );
                return output;
            }
        }

    public:
        template <class Input, class Output>
        struct DistinguishedRemoteFacility {
            typename R::template Sinkoid<
                typename M::template Key<std::tuple<ConnectionLocator, Input>>
            > orderReceiver;
            typename R::template Sourceoid<
                typename M::template KeyedData<std::tuple<ConnectionLocator, Input>, Output> 
            > feedOrderResults;
            typename R::template Sourceoid<
                std::tuple<ConnectionLocator, bool>
            > feedConnectionChanges;
        };

        template <class ... IdentitiesAndInputsAndOutputs>
        using DistinguishedRemoteFacilities =
            std::tuple<
                DistinguishedRemoteFacility<
                    typename SpecReader<IdentitiesAndInputsAndOutputs>::Input
                    , typename SpecReader<IdentitiesAndInputsAndOutputs>::Output
                >...
            >;

        template <class ... IdentitiesAndInputsAndOutputs>
        using DistinguishedRemoteFacilityInitiators =
            std::tuple<
                std::function<
                    typename SpecReader<IdentitiesAndInputsAndOutputs>::Input()
                >...
            >;

        template <class Input, class Output>
        using DistinguishedRemoteFacilityInitialCallbackPicker =
            std::function<bool(Input const &, Output const &)>;

        template <class ... IdentitiesAndInputsAndOutputs>
        using DistinguishedRemoteFacilityInitialCallbackPickers =
            std::tuple<
                DistinguishedRemoteFacilityInitialCallbackPicker<
                    typename SpecReader<IdentitiesAndInputsAndOutputs>::Input
                    , typename SpecReader<IdentitiesAndInputsAndOutputs>::Output
                >...
            >;

    private:
        template <
            int CurrentIdx
            , class Initiators
            , class InitialCallbackPickers
            , class Output
        >
        static void setupDistinguishedRemoteFacilitiesInternal(
            R &r
            , typename R::template Source<
                std::array<MultiTransportRemoteFacilityAction, CurrentIdx>
            > &&actionSource
            , Initiators const &initiators
            , InitialCallbackPickers const &initialCallbackPickers
            , std::array<std::string, CurrentIdx> const &names
            , std::string const &prefix
            , std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> const &hookPairFactory
            , bool lastOneNeedsToBeTiedUp
            , Output &output
            , typename R::template Source<
                std::array<MultiTransportRemoteFacilityAction, CurrentIdx>
            > *nextSourceOutput
        ) 
        {
            if (nextSourceOutput) {
                *nextSourceOutput = std::move(actionSource);
            }
        }

        template <
            int CurrentIdx
            , class Initiators
            , class InitialCallbackPickers
            , class Output
            , class FirstRemainingIdentityAndInputAndOutput
            , class ... RemainingIdentitiesAndInputsAndOutputs
        >
        static void setupDistinguishedRemoteFacilitiesInternal(
            R &r
            , typename R::template Source<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(RemainingIdentitiesAndInputsAndOutputs)+CurrentIdx+1>
            > &&actionSource
            , Initiators const &initiators
            , InitialCallbackPickers const &initialCallbackPickers
            , std::array<std::string, sizeof...(RemainingIdentitiesAndInputsAndOutputs)+CurrentIdx+1> const &names
            , std::string const &prefix
            , std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> const &hookPairFactory
            , bool lastOneNeedsToBeTiedUp
            , Output &output
            , typename R::template Source<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(RemainingIdentitiesAndInputsAndOutputs)+CurrentIdx+1>
            > *nextSourceOutput
        ) {
            using InputArray = std::array<MultiTransportRemoteFacilityAction, sizeof...(RemainingIdentitiesAndInputsAndOutputs)+CurrentIdx+1>;
            
            auto vieInputInject = [](InputArray &&x) ->  MultiTransportRemoteFacilityAction {
                return std::move(x[CurrentIdx]);
            };
            auto vieExtraOutputClassifier = 
                [](transport::MultiTransportRemoteFacilityActionResult const &res) -> bool {
                    return (std::get<0>(res).actionType == transport::MultiTransportRemoteFacilityActionType::Register);
                };
            
            auto initiator = std::get<CurrentIdx>(initiators);
            auto vieSelfLoopCreator = [initiator](MultiTransportRemoteFacilityActionResult &&x) 
                -> std::tuple<
                    ConnectionLocator
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Input
                >
            {
                return {std::get<0>(x).connectionLocator, initiator()};
            };

            auto initialCallbackPicker = std::get<CurrentIdx>(initialCallbackPickers);

            auto vieInitialCallbackFilter =
                [initialCallbackPicker](typename M::template KeyedData<
                    std::tuple<
                        ConnectionLocator
                        , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Input
                    >
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Output
                > const &x) -> bool {
                    return initialCallbackPicker(std::get<1>(x.key.key()), x.data);
                };

            auto facility = M::vieOnOrderFacility(
                new MultiTransportRemoteFacility<
                    typename M::EnvironmentType
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Input
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Output
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Identity
                    , transport::MultiTransportRemoteFacilityDispatchStrategy::Designated
                >(hookPairFactory)
            );

            auto facilityLoopOutput = basic::AppRunnerUtilComponents<R>::setupVIEFacilitySelfLoopAndWait(
                r
                , actionSource.clone()
                , std::move(vieInputInject)
                , std::move(vieExtraOutputClassifier)
                , std::move(vieSelfLoopCreator)
                , facility
                , std::move(vieInitialCallbackFilter)
                , prefix+"/"+names[CurrentIdx]+"/loop"
                , names[CurrentIdx]
            );

            auto extraOutputConverter = M::template liftPure<transport::MultiTransportRemoteFacilityActionResult>(
                [](transport::MultiTransportRemoteFacilityActionResult &&x) 
                    -> std::tuple<ConnectionLocator, bool>
                {
                    return {
                        std::get<0>(x).connectionLocator
                        , (std::get<0>(x).actionType == MultiTransportRemoteFacilityActionType::Register)
                    };
                }
            );

            r.registerAction(prefix+"/"+names[CurrentIdx]+"/loop/extraOutputConverter", extraOutputConverter);

            auto extraOutput = r.execute(extraOutputConverter, facilityLoopOutput.facilityExtraOutput.clone());

            auto extraOutputCatcher = M::template trivialExporter<std::tuple<ConnectionLocator, bool>>();
            r.registerExporter(prefix+"/"+names[CurrentIdx]+"/loop/extraOutputCatcher", extraOutputCatcher);
            r.exportItem(extraOutputCatcher, extraOutput.clone());

            std::get<CurrentIdx>(output) = 
                DistinguishedRemoteFacility<
                    typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Input
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Output
                > {
                    facilityLoopOutput.callIntoFacility
                    , r.sourceAsSourceoid(facilityLoopOutput.facilityOutput.clone())
                    , r.sourceAsSourceoid(extraOutput.clone())
                };

            if constexpr (sizeof...(RemainingIdentitiesAndInputsAndOutputs) == 0) {
                if (lastOneNeedsToBeTiedUp) {
                    auto emptyExporter = M::template trivialExporter<InputArray>();
                    r.exportItem(prefix+"/"+names[CurrentIdx]+"/loop/emptyExporter", emptyExporter, facilityLoopOutput.nextTriggeringSource.clone());
                }
                if (nextSourceOutput) {
                    *nextSourceOutput = facilityLoopOutput.nextTriggeringSource;
                }
            } else {
                setupDistinguishedRemoteFacilitiesInternal<
                    (CurrentIdx+1)
                    , Initiators
                    , Output
                    , RemainingIdentitiesAndInputsAndOutputs...
                >(
                    r 
                    , std::move(facilityLoopOutput.nextTriggeringSource)
                    , initiators
                    , names
                    , prefix
                    , hookPairFactory
                    , lastOneNeedsToBeTiedUp
                    , output
                    , nextSourceOutput
                );
            }
        }

        template <class ... IdentitiesAndInputsAndOutputs>
        static auto setupDistinguishedRemoteFacilities(
            R &r
            , typename R::template Source<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(IdentitiesAndInputsAndOutputs)>
            > &&actionSource
            , DistinguishedRemoteFacilityInitiators<IdentitiesAndInputsAndOutputs...> const &initiators
            , DistinguishedRemoteFacilityInitialCallbackPickers<IdentitiesAndInputsAndOutputs...> const &initialCallbackPickers
            , std::array<std::string, sizeof...(IdentitiesAndInputsAndOutputs)> const &names
            , std::string const &prefix
            , std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> const &hookPairFactory
            , bool lastOneNeedsToBeTiedUp
            , typename R::template Source<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(IdentitiesAndInputsAndOutputs)>
            > *nextTriggeringSource
        ) -> DistinguishedRemoteFacilities<IdentitiesAndInputsAndOutputs...>
        {
            if constexpr (sizeof...(IdentitiesAndInputsAndOutputs) < 1) {
                return {};
            } else {
                using Initiators = DistinguishedRemoteFacilityInitiators<IdentitiesAndInputsAndOutputs...>;
                using InitialCallbackPickers = DistinguishedRemoteFacilityInitialCallbackPickers<IdentitiesAndInputsAndOutputs...>;
                using Output = DistinguishedRemoteFacilities<IdentitiesAndInputsAndOutputs...>;
                Output output;
                setupDistinguishedRemoteFacilitiesInternal<
                    0
                    , Initiators
                    , InitialCallbackPickers
                    , Output
                    , IdentitiesAndInputsAndOutputs...
                >(
                    r 
                    , std::move(actionSource)
                    , initiators
                    , initialCallbackPickers
                    , names
                    , prefix
                    , hookPairFactory
                    , lastOneNeedsToBeTiedUp
                    , output
                    , nextTriggeringSource
                );
                return output;
            }
        }
    
    public:
        template <class ... Specs>
        class SetupRemoteFacilities {};

        template <class ... DistinguishedRemoteFacilitiesSpec, class ... NonDistinguishedRemoteFacilitiesSpec>
        class SetupRemoteFacilities<
            std::tuple<DistinguishedRemoteFacilitiesSpec...>
            , std::tuple<NonDistinguishedRemoteFacilitiesSpec...>
        > {
        public:
            static auto run(
                R &r
                , std::variant<
                    MultiTransportBroadcastListenerAddSubscription
                    , typename R::template Sourceoid<HeartbeatMessage>
                > const &heartbeatSource
                , std::regex const &serverNameRE
                , std::array<std::string, sizeof...(DistinguishedRemoteFacilitiesSpec)+sizeof...(NonDistinguishedRemoteFacilitiesSpec)> const &channelNames
                , std::chrono::system_clock::duration ttl
                , std::chrono::system_clock::duration checkPeriod
                , DistinguishedRemoteFacilityInitiators<DistinguishedRemoteFacilitiesSpec...> const &initiators
                , DistinguishedRemoteFacilityInitialCallbackPickers<DistinguishedRemoteFacilitiesSpec...> const &initialCallbackPickers
                , std::array<std::string, sizeof...(DistinguishedRemoteFacilitiesSpec)+sizeof...(NonDistinguishedRemoteFacilitiesSpec)> const &facilityRegistrationNames
                , std::string const &prefix
                , std::optional<WireToUserHook> wireToUserHookForHeartbeat = std::nullopt
                , std::function<std::optional<ByteDataHookPair>(std::string const &, ConnectionLocator const &)> const &hookPairFactory
                    = [](std::string const &, ConnectionLocator const &) {return std::nullopt;}
            ) 
            -> std::tuple<
                DistinguishedRemoteFacilities<DistinguishedRemoteFacilitiesSpec...>
                , NonDistinguishedRemoteFacilities<NonDistinguishedRemoteFacilitiesSpec...>
            >
            {
                std::array<std::string, sizeof...(DistinguishedRemoteFacilitiesSpec)> distinguishedChannelNames;
                std::array<std::string, sizeof...(NonDistinguishedRemoteFacilitiesSpec)> nonDistinguishedChannelNames;
                for (int ii=0; ii<sizeof...(DistinguishedRemoteFacilitiesSpec); ++ii) {
                    distinguishedChannelNames[ii] = channelNames[ii];
                }
                for (int ii=0; ii<sizeof...(NonDistinguishedRemoteFacilitiesSpec); ++ii) {
                    nonDistinguishedChannelNames[ii] = channelNames[ii+sizeof...(DistinguishedRemoteFacilitiesSpec)];
                }
                std::array<std::string, sizeof...(DistinguishedRemoteFacilitiesSpec)> distinguishedRegNames;
                std::array<std::string, sizeof...(NonDistinguishedRemoteFacilitiesSpec)> nonDistinguishedRegNames;
                for (int ii=0; ii<sizeof...(DistinguishedRemoteFacilitiesSpec); ++ii) {
                    distinguishedRegNames[ii] = facilityRegistrationNames[ii];
                }
                for (int ii=0; ii<sizeof...(NonDistinguishedRemoteFacilitiesSpec); ++ii) {
                    nonDistinguishedRegNames[ii] = facilityRegistrationNames[ii+sizeof...(DistinguishedRemoteFacilitiesSpec)];
                }

                typename R::template Sourceoid<HeartbeatMessage> hsMsgSource = std::visit([&r,&prefix,wireToUserHookForHeartbeat](auto const &hs) -> typename R::template Sourceoid<HeartbeatMessage> {
                    using T = std::decay_t<decltype(hs)>;
                    if constexpr (std::is_same_v<T, MultiTransportBroadcastListenerAddSubscription>) {
                        auto createHeartbeatListenKey = M::constFirstPushKeyImporter(
                            transport::MultiTransportBroadcastListenerInput { {
                                hs
                            } }
                        );
                        r.registerImporter(prefix+"/createHeartbeatListenKey", createHeartbeatListenKey);

                        auto discardTopicFromHeartbeat = M::template liftPure<basic::TypedDataWithTopic<HeartbeatMessage>>(
                            [](basic::TypedDataWithTopic<HeartbeatMessage> &&d) {
                                return std::move(d.content);
                            }
                        );
                        r.registerAction(prefix+"/discardTopicFromHeartbeat", discardTopicFromHeartbeat);

                        auto realHeartbeatSource = basic::AppRunnerUtilComponents<R>::importWithTrigger(
                            r
                            , r.importItem(createHeartbeatListenKey)
                            , M::onOrderFacilityWithExternalEffects(
                                new transport::MultiTransportBroadcastListener<typename M::EnvironmentType, HeartbeatMessage>(wireToUserHookForHeartbeat)
                            )
                            , std::nullopt //the trigger response is not needed
                            , prefix+"/heartbeatListener"
                        );
                        return R::sourceAsSourceoid(r.execute(
                            discardTopicFromHeartbeat
                            , std::move(realHeartbeatSource)
                        ));
                    } else if constexpr (std::is_same_v<T, typename R::template Sourceoid<HeartbeatMessage>>) {
                        return hs;
                    } else {
                        return typename R::template Sourceoid<HeartbeatMessage> {};
                    }
                }, heartbeatSource);

                auto facilityCheckTimer = basic::real_time_clock::ClockImporter<typename M::EnvironmentType>
                    ::template createRecurringClockImporter<basic::VoidStruct>(
                        std::chrono::system_clock::now()
                        , std::chrono::system_clock::now()+std::chrono::hours(24)
                        , checkPeriod
                        , [](typename M::EnvironmentType::TimePointType const &tp) {
                            return basic::VoidStruct {};
                        }
                    );
                auto timerInput = r.importItem(prefix+"/checkTimer", facilityCheckTimer);

                auto heartbeatParser = M::template enhancedMulti2<
                    HeartbeatMessage, basic::VoidStruct
                >(
                    HeartbeatMessageToRemoteFacilityCommandConverter<
                        std::tuple<
                            DistinguishedRemoteFacilitiesSpec...
                        >
                        , std::tuple<
                            NonDistinguishedRemoteFacilitiesSpec...
                        >
                    >(
                        serverNameRE
                        , distinguishedChannelNames
                        , nonDistinguishedChannelNames
                        , ttl
                    )
                );
                r.registerAction(prefix+"/heartbeatParser", heartbeatParser);

                auto distinguishedSpecInputBrancher = M::template liftPure<
                    std::tuple<
                        std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedRemoteFacilitiesSpec)>
                        , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedRemoteFacilitiesSpec)>
                    >
                >(
                    [](
                        std::tuple<
                            std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedRemoteFacilitiesSpec)>
                            , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedRemoteFacilitiesSpec)>
                        > &&x
                    ) -> std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedRemoteFacilitiesSpec)> {
                        return std::get<0>(x);
                    }
                );

                r.registerAction(prefix+"/distinguishedBranch", distinguishedSpecInputBrancher);
                
                hsMsgSource(r, r.actionAsSink_2_0(heartbeatParser));
                auto commands = r.actionAsSource(heartbeatParser);
                r.execute(heartbeatParser, std::move(timerInput));
                auto distinguishedCommands
                    = r.execute(
                        distinguishedSpecInputBrancher
                        , commands.clone()
                    );

                auto nextTriggeringSource = distinguishedCommands.clone();
                auto distinguishedFacilities = 
                    setupDistinguishedRemoteFacilities<DistinguishedRemoteFacilitiesSpec...>
                    (
                        r 
                        , distinguishedCommands.clone()
                        , initiators
                        , initialCallbackPickers
                        , distinguishedRegNames
                        , prefix+"/facilities"
                        , hookPairFactory
                        , (sizeof...(NonDistinguishedRemoteFacilitiesSpec) == 0)
                        , &nextTriggeringSource
                    );

                if constexpr (sizeof...(NonDistinguishedRemoteFacilitiesSpec) > 0) {
                    auto nonDistinguishedSpecInputBrancher = M::template liftPure<
                        std::tuple<
                            std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedRemoteFacilitiesSpec)>
                            , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedRemoteFacilitiesSpec)>
                        >
                    >(
                        [](
                            std::tuple<
                                std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedRemoteFacilitiesSpec)>
                                , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedRemoteFacilitiesSpec)>
                            > &&x
                        ) -> std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedRemoteFacilitiesSpec)> {
                            return std::get<1>(x);
                        }
                    );

                    r.registerAction(prefix+"/nonDistinguishedBranch", nonDistinguishedSpecInputBrancher);
                    
                    auto nonDistinguishedCommands
                        = r.execute(
                            nonDistinguishedSpecInputBrancher
                            , commands.clone()
                        );

                    auto synchronizerInput = M::template liftPure<
                        std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedRemoteFacilitiesSpec)>
                    >(
                        [](std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedRemoteFacilitiesSpec)> &&) 
                            -> basic::VoidStruct 
                        {
                            return basic::VoidStruct {};
                        }
                    );
                    r.registerAction(prefix+"/synchronizerInput", synchronizerInput);

                    auto synchronizer = basic::CommonFlowUtilComponents<M>::template synchronizer2<
                        basic::VoidStruct
                        , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedRemoteFacilitiesSpec)>
                    >
                    (
                        [](
                            basic::VoidStruct
                            , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedRemoteFacilitiesSpec)> &&x
                        ) -> std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedRemoteFacilitiesSpec)>
                        {
                            return std::move(x);
                        }
                    );
                    r.registerAction(prefix+"/synchronizer", synchronizer);

                    auto synchronizedNonDistinguishedCommands =
                        r.execute(
                            synchronizer
                            , r.execute(
                                synchronizerInput
                                , nextTriggeringSource.clone()
                            )
                        );
                    r.execute(synchronizer, nonDistinguishedCommands.clone());

                    auto nonDistinguishedFacilities = 
                        setupNonDistinguishedRemoteFacilities<NonDistinguishedRemoteFacilitiesSpec...>
                        (
                            r 
                            , std::move(synchronizedNonDistinguishedCommands)
                            , nonDistinguishedRegNames
                            , prefix+"/facilities"
                            , hookPairFactory
                        );

                    return {
                        std::move(distinguishedFacilities)
                        , std::move(nonDistinguishedFacilities)
                    };
                } else {
                    return {
                        std::move(distinguishedFacilities)
                        , {}
                    };
                }               
            }
        };
        template <class Request, class Result>
        static auto setupOneNonDistinguishedRemoteFacility(
            R &r 
            , typename R::template ConvertibleToSourceoid<HeartbeatMessage> &&heartbeatSource
            , std::regex const &serverNameRE
            , std::string const &facilityRegistrationName 
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , std::chrono::system_clock::duration ttl = std::chrono::seconds(3)
            , std::chrono::system_clock::duration checkPeriod = std::chrono::seconds(5)
        ) -> typename R::template FacilitioidConnector<Request, Result>
        {
            return std::get<0>(std::get<1>(SetupRemoteFacilities<
                std::tuple<>
                , std::tuple<
                    IdentityAndInputAndOutputHolder<
                        typename DetermineClientSideIdentityForRequest<typename R::EnvironmentType, Request>::IdentityType
                        , Request
                        , Result
                    >
                >
            >::run(
                r 
                , R::convertToSourceoid(std::move(heartbeatSource))
                , serverNameRE
                , {facilityRegistrationName}
                , ttl 
                , checkPeriod
                , {}
                , {}
                , {"proxy:"+facilityRegistrationName}
                , "group:"+facilityRegistrationName
                , std::nullopt 
                , [hooks](std::string const &, ConnectionLocator const &) {
                    return hooks;
                }
            )));
        }
        template <class Request, class Result>
        static auto setupOneDistinguishedRemoteFacility(
            R &r 
            , typename R::template ConvertibleToSourceoid<HeartbeatMessage> &&heartbeatSource
            , std::regex const &serverNameRE
            , std::string const &facilityRegistrationName
            , std::function<Request()> requestGenerator
            , std::function<bool(Request const &, Result const &)> resultChecker
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , std::chrono::system_clock::duration ttl = std::chrono::seconds(3)
            , std::chrono::system_clock::duration checkPeriod = std::chrono::seconds(5)
        ) -> DistinguishedRemoteFacility<Request, Result>
        {
            return std::get<0>(std::get<0>(SetupRemoteFacilities<
                std::tuple<
                    IdentityAndInputAndOutputHolder<
                        typename DetermineClientSideIdentityForRequest<typename R::EnvironmentType, Request>::IdentityType
                        , Request
                        , Result
                    >
                >
                , std::tuple<
                >
            >::run(
                r 
                , R::convertToSourceoid(std::move(heartbeatSource))
                , serverNameRE
                , {facilityRegistrationName}
                , ttl 
                , checkPeriod
                , {requestGenerator}
                , {resultChecker}
                , {"proxy:"+facilityRegistrationName}
                , ("group:"+facilityRegistrationName)
                , std::nullopt 
                , [hooks](std::string const &, ConnectionLocator const &) {
                    return hooks;
                }
            )));
        }
        template <class FirstRequest, class FirstResult, class SecondRequest, class SecondResult>
        static auto setupTwoStepRemoteFacility(
            R &r 
            , typename R::template ConvertibleToSourceoid<HeartbeatMessage> &&heartbeatSource
            , std::regex const &serverNameRE
            , std::tuple<std::string, std::string> const &facilityRegistrationNames
            , std::function<FirstRequest()> firstRequestGenerator
            , std::function<bool(FirstRequest const &, FirstResult const &)> firstResultChecker
            , std::tuple<
                std::optional<ByteDataHookPair>
                , std::optional<ByteDataHookPair>
            > hooks = {std::nullopt, std::nullopt}
            , std::chrono::system_clock::duration ttl = std::chrono::seconds(3)
            , std::chrono::system_clock::duration checkPeriod = std::chrono::seconds(5)
        ) -> std::tuple<
            DistinguishedRemoteFacility<FirstRequest, FirstResult>
            , typename R::template FacilitioidConnector<SecondRequest, SecondResult>
        >
        {
            auto res = SetupRemoteFacilities<
                std::tuple<
                    IdentityAndInputAndOutputHolder<
                        typename DetermineClientSideIdentityForRequest<typename R::EnvironmentType, FirstRequest>::IdentityType
                        , FirstRequest
                        , FirstResult
                    >
                >
                , std::tuple<
                    IdentityAndInputAndOutputHolder<
                        typename DetermineClientSideIdentityForRequest<typename R::EnvironmentType, SecondRequest>::IdentityType
                        , SecondRequest
                        , SecondResult
                    >
                >
            >::run(
                r 
                , R::convertToSourceoid(std::move(heartbeatSource))
                , serverNameRE
                , {std::get<0>(facilityRegistrationNames), std::get<1>(facilityRegistrationNames)}
                , ttl 
                , checkPeriod
                , {firstRequestGenerator}
                , {firstResultChecker}
                , {"proxy:"+std::get<0>(facilityRegistrationNames), "proxy:"+std::get<1>(facilityRegistrationNames)}
                , "group:"+std::get<0>(facilityRegistrationNames)+","+std::get<1>(facilityRegistrationNames)
                , std::nullopt 
                , [hooks,facilityRegistrationNames](std::string const &s, ConnectionLocator const &) -> std::optional<ByteDataHookPair> {
                    if (s == std::get<0>(facilityRegistrationNames)) {
                        return std::get<0>(hooks);
                    } else if (s == std::get<1>(facilityRegistrationNames)) {
                        return std::get<1>(hooks);
                    } else {
                        return std::nullopt;
                    }
                }
            );

            return {
                std::get<0>(std::get<0>(res))
                , std::get<0>(std::get<1>(res))
            };
        }
        template <class Request, class Result>
        static auto setupSimpleRemoteFacility(
            R &r 
            , std::string const &channelSpec
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) ->  typename R::template OnOrderFacilityPtr<Request, Result>
        {
            auto parsed = parseMultiTransportRemoteFacilityChannel(channelSpec);
            if (parsed) {
                switch (std::get<0>(*parsed)) {
                case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
                    if constexpr(std::is_convertible_v<typename R::EnvironmentType *, rabbitmq::RabbitMQComponent *>) {
                        return rabbitmq::RabbitMQOnOrderFacility<typename R::EnvironmentType>::template createTypedRPCOnOrderFacility<Request,Result>(std::get<1>(*parsed), hooks);
                    } else {
                        throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up rabbitmq facility for channel spec '"+channelSpec+"', but rabbitmq is unsupported in the environment");
                    }
                    break;
                case MultiTransportRemoteFacilityConnectionType::Redis:
                    if constexpr(std::is_convertible_v<typename R::EnvironmentType *, redis::RedisComponent *>) {
                        return redis::RedisOnOrderFacility<typename R::EnvironmentType>::template createTypedRPCOnOrderFacility<Request,Result>(std::get<1>(*parsed), hooks);
                    } else {
                        throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up redis facility for channel spec '"+channelSpec+"', but redis is unsupported in the environment");
                    }
                    break;
                default:
                    throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up unsupported facility for channel spec '"+channelSpec+"'");
                    break;
                }
            } else {
                throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] unknown channel spec '"+channelSpec+"'");
            }
        }
    };
    
} } } }

#endif