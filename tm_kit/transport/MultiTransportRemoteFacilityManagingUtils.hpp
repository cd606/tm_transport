#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_MANAGING_UTILS_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_MANAGING_UTILS_HPP_

#include <tm_kit/transport/MultiTransportRemoteFacility.hpp>
#include <tm_kit/transport/MultiTransportBroadcastListener.hpp>
#include <tm_kit/transport/MultiTransportBroadcastListenerManagingUtils.hpp>
#include <tm_kit/transport/HeartbeatMessageToRemoteFacilityCommand.hpp>

#include <tm_kit/basic/real_time_clock/ClockComponent.hpp>
#include <tm_kit/basic/real_time_clock/ClockImporter.hpp>
#include <tm_kit/basic/CommonFlowUtils.hpp>
#include <tm_kit/basic/AppRunnerUtils.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    struct SimpleRemoteFacilitySpecByHeartbeat {
        std::string heartbeatSpec;
        std::string heartbeatTopic;
        std::regex facilityServerHeartbeatIdentityRE;
        std::string facilityRegistrationName; 
        std::optional<WireToUserHook> heartbeatHook = std::nullopt;
        std::chrono::system_clock::duration ttl = std::chrono::seconds(3);
        std::chrono::system_clock::duration checkPeriod = std::chrono::seconds(5);
    };
    using SimpleRemoteFacilitySpec = std::variant<
        std::string 
        , SimpleRemoteFacilitySpecByHeartbeat
    >;

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

        template <class Input, class Output>
        struct NonDistinguishedRemoteFacility {
            typename R::template FacilitioidConnector<
                Input
                , Output
            > facility;
            typename R::template Sourceoid<std::size_t> feedUnderlyingCount;
        };

        template <class ... IdentitiesAndInputsAndOutputs>
        using NonDistinguishedRemoteFacilities =
            std::tuple<
                NonDistinguishedRemoteFacility<
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
            auto facility = M::vieOnOrderFacility(
                new MultiTransportRemoteFacility<
                    typename M::EnvironmentType
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Input
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Output
                    , typename SpecReader<FirstRemainingIdentityAndInputAndOutput>::Identity
                    , transport::MultiTransportRemoteFacilityDispatchStrategy::Random
                >(hookPairFactory)
            );
            r.registerVIEOnOrderFacility(
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
                , r.vieFacilityAsSink(facility)
            );
            //this is added so that the user does not have to handle the
            //count. If the user wants to handle the count, the source is
            //still there in the return value.
            auto countSource = r.vieFacilityAsSource(facility);
            auto discardCount = M::template trivialExporter<std::size_t>();
            r.registerExporter(prefix+"/discard_count_"+std::to_string(CurrentIdx), discardCount);
            r.connect(countSource.clone(), r.exporterAsSink(discardCount));
            std::get<CurrentIdx>(output) = {
                R::vieFacilityConnector(facility)
                , r.sourceAsSourceoid(countSource.clone())
            };
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
        private:
            static auto buildFacilityActionSource(
                R &r 
                , std::variant<
                    MultiTransportBroadcastListenerAddSubscription
                    , typename R::template Sourceoid<HeartbeatMessage>
                    , typename R::template Sourceoid<std::shared_ptr<HeartbeatMessage const>>
                > const &heartbeatSource
                , std::regex const &serverNameRE
                , std::array<std::string, sizeof...(DistinguishedRemoteFacilitiesSpec)+sizeof...(NonDistinguishedRemoteFacilitiesSpec)> const &channelNames
                , std::chrono::system_clock::duration ttl
                , std::chrono::system_clock::duration checkPeriod
                , std::string const &prefix
                , std::optional<WireToUserHook> wireToUserHookForHeartbeat = std::nullopt
            )
            -> typename R::template Source<std::tuple<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedRemoteFacilitiesSpec)>
                , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedRemoteFacilitiesSpec)>
            >>
            {
                std::array<std::string, sizeof...(DistinguishedRemoteFacilitiesSpec)> distinguishedChannelNames;
                std::array<std::string, sizeof...(NonDistinguishedRemoteFacilitiesSpec)> nonDistinguishedChannelNames;
                for (int ii=0; ii<sizeof...(DistinguishedRemoteFacilitiesSpec); ++ii) {
                    distinguishedChannelNames[ii] = channelNames[ii];
                }
                for (int ii=0; ii<sizeof...(NonDistinguishedRemoteFacilitiesSpec); ++ii) {
                    nonDistinguishedChannelNames[ii] = channelNames[ii+sizeof...(DistinguishedRemoteFacilitiesSpec)];
                }

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

                return std::visit([&r,&prefix,wireToUserHookForHeartbeat,&serverNameRE,ttl,&distinguishedChannelNames,&nonDistinguishedChannelNames,&timerInput](auto const &hs) -> typename R::template Source<std::tuple<
                    std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedRemoteFacilitiesSpec)>
                    , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedRemoteFacilitiesSpec)>
                >> {
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

                        r.execute(heartbeatParser, r.execute(
                            discardTopicFromHeartbeat
                            , std::move(realHeartbeatSource)
                        ));
                        r.execute(heartbeatParser, std::move(timerInput));

                        return r.actionAsSource(heartbeatParser);
                    } else if constexpr (std::is_same_v<T, typename R::template Sourceoid<HeartbeatMessage>>) {
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

                        hs(r, r.actionAsSink_2_0(heartbeatParser));
                        r.execute(heartbeatParser, std::move(timerInput));

                        return r.actionAsSource(heartbeatParser);
                    } else {
                        auto heartbeatParser = M::template enhancedMulti2<
                            std::shared_ptr<HeartbeatMessage const>, basic::VoidStruct
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

                        hs(r, r.actionAsSink_2_0(heartbeatParser));
                        r.execute(heartbeatParser, std::move(timerInput));

                        return r.actionAsSource(heartbeatParser);
                    }
                }, heartbeatSource);
            }
        public:
            static auto run(
                R &r
                , std::variant<
                    MultiTransportBroadcastListenerAddSubscription
                    , typename R::template Sourceoid<HeartbeatMessage>
                    , typename R::template Sourceoid<std::shared_ptr<HeartbeatMessage const>>
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
                std::array<std::string, sizeof...(DistinguishedRemoteFacilitiesSpec)> distinguishedRegNames;
                std::array<std::string, sizeof...(NonDistinguishedRemoteFacilitiesSpec)> nonDistinguishedRegNames;
                for (int ii=0; ii<sizeof...(DistinguishedRemoteFacilitiesSpec); ++ii) {
                    distinguishedRegNames[ii] = facilityRegistrationNames[ii];
                }
                for (int ii=0; ii<sizeof...(NonDistinguishedRemoteFacilitiesSpec); ++ii) {
                    nonDistinguishedRegNames[ii] = facilityRegistrationNames[ii+sizeof...(DistinguishedRemoteFacilitiesSpec)];
                }

                auto commands = buildFacilityActionSource(
                    r 
                    , heartbeatSource
                    , serverNameRE
                    , channelNames
                    , ttl
                    , checkPeriod
                    , prefix
                    , wireToUserHookForHeartbeat
                );

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
    private:
        static auto convertToHeartbeatInput(
            std::variant<
                typename R::template ConvertibleToSourceoid<HeartbeatMessage>
                , typename R::template ConvertibleToSourceoid<std::shared_ptr<HeartbeatMessage const>>
            > &&heartbeatSource
        ) -> std::variant<
            MultiTransportBroadcastListenerAddSubscription
            , typename R::template Sourceoid<HeartbeatMessage>
            , typename R::template Sourceoid<std::shared_ptr<HeartbeatMessage const>>
        > {
            return std::visit([](auto &&h) -> std::variant<
                MultiTransportBroadcastListenerAddSubscription
                , typename R::template Sourceoid<HeartbeatMessage>
                , typename R::template Sourceoid<std::shared_ptr<HeartbeatMessage const>>
            > {
                using T = std::decay_t<decltype(h)>;
                if constexpr (std::is_same_v<T, typename R::template ConvertibleToSourceoid<HeartbeatMessage>>) {
                    return R::template convertToSourceoid<HeartbeatMessage>(std::move(h));
                } else {
                    return R::template convertToSourceoid<std::shared_ptr<HeartbeatMessage const>>(std::move(h));
                }
            }, std::move(heartbeatSource));
        }
    private:
        template <template <class...> class ProtocolWrapper, class Request, class Result>
        static auto adaptNonDistinguishedRemoteFacility(
            NonDistinguishedRemoteFacility<
                basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request>
                , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Result>
            > const &f
            , std::string const &facilityRegistrationName
        ) -> NonDistinguishedRemoteFacility<Request, Result>
        {
            return {
                basic::WrapFacilitioidConnectorForSerialization<R>
                    ::template wrapClientSideWithProtocol<ProtocolWrapper,Request,Result>(
                        f.facility
                        , "protocol:"+facilityRegistrationName
                    )
                , f.feedUnderlyingCount
            };
        }
        template <template <class...> class ProtocolWrapper, class Request, class Result>
        static auto adaptDistinguishedRemoteFacility(
            R &r
            , DistinguishedRemoteFacility<
                basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request>
                , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Result>
            > const &f
            , std::string const &facilityRegistrationName
        ) -> DistinguishedRemoteFacility<Request, Result>
        {
            auto receiver = M::template liftPure<typename M::template Key<std::tuple<ConnectionLocator, Request>>>(
                [](typename M::template Key<std::tuple<ConnectionLocator, Request>> &&x) 
                    -> typename M::template Key<std::tuple<ConnectionLocator, basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request>>>
                {
                    return {
                        x.id()
                        , {
                            std::move(std::get<0>(x.key()))
                            , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                                ::template enclose<Request>(std::get<1>(x.key()))
                        }
                    };
                }
            );
            auto feedResults = M::template liftPure<
                typename M::template KeyedData<
                    std::tuple<ConnectionLocator, basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request>>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Result>
                >
            >(
                [](typename M::template KeyedData<
                        std::tuple<ConnectionLocator, basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request>>
                        , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Result>
                    > &&x
                ) -> typename M::template KeyedData<
                    std::tuple<ConnectionLocator, Request>
                    , Result
                >
                {
                    return {
                        {
                            x.key.id()
                            , {
                                std::get<0>(x.key.key())
                                , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                                    ::template extract<Request>(std::get<1>(x.key.key()))
                            }
                        }
                        , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                            ::template extract<Result>(std::move(x.data))
                    };
                }
            );
            r.registerAction("protocol:"+facilityRegistrationName+"/receiver", receiver);
            r.registerAction("protocol:"+facilityRegistrationName+"/feedResults", feedResults);
            f.orderReceiver(r, r.actionAsSource(receiver));
            f.feedOrderResults(r, r.actionAsSink(feedResults));
            auto receiverPlug = M::template vacuousImporter<typename M::template Key<std::tuple<ConnectionLocator, Request>>>();
            r.registerImporter("protocol:"+facilityRegistrationName+"/receiverPlug", receiverPlug);
            auto feedOrderResultsPlug = M::template trivialExporter<typename M::template KeyedData<
                    std::tuple<ConnectionLocator, Request>
                    , Result
                >>();
            r.registerExporter("protocol:"+facilityRegistrationName+"/feedOrderResultsPlug", feedOrderResultsPlug);
            r.execute(receiver, r.importItem(receiverPlug));
            r.exportItem(feedOrderResultsPlug, r.actionAsSource(feedResults));
            return {
                r.sinkAsSinkoid(r.actionAsSink(receiver))
                , r.sourceAsSourceoid(r.actionAsSource(feedResults))
                , f.feedConnectionChanges
            };
        }
    public:
        template <class Request, class Result>
        static auto setupOneNonDistinguishedRemoteFacility(
            R &r 
            , std::variant<
                typename R::template ConvertibleToSourceoid<HeartbeatMessage>
                , typename R::template ConvertibleToSourceoid<std::shared_ptr<HeartbeatMessage const>>
            > &&heartbeatSource
            , std::regex const &serverNameRE
            , std::string const &facilityRegistrationName 
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , std::chrono::system_clock::duration ttl = std::chrono::seconds(3)
            , std::chrono::system_clock::duration checkPeriod = std::chrono::seconds(5)
        ) -> NonDistinguishedRemoteFacility<Request, Result>
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
                , convertToHeartbeatInput(std::move(heartbeatSource))
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
        template <template <class...> class ProtocolWrapper, class Request, class Result>
        static auto setupOneNonDistinguishedRemoteFacilityWithProtocol(
            R &r 
            , std::variant<
                typename R::template ConvertibleToSourceoid<HeartbeatMessage>
                , typename R::template ConvertibleToSourceoid<std::shared_ptr<HeartbeatMessage const>>
            > &&heartbeatSource
            , std::regex const &serverNameRE
            , std::string const &facilityRegistrationName 
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , std::chrono::system_clock::duration ttl = std::chrono::seconds(3)
            , std::chrono::system_clock::duration checkPeriod = std::chrono::seconds(5)
        ) -> NonDistinguishedRemoteFacility<Request, Result>
        {
            if constexpr (std::is_same_v<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                ProtocolWrapper, Request
            >, Request>) {
                return setupOneNonDistinguishedRemoteFacility<Request,Result>(
                    r, std::move(heartbeatSource), serverNameRE
                    , facilityRegistrationName, hooks, ttl, checkPeriod
                );
            } else {
                auto x = setupOneNonDistinguishedRemoteFacility<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Result>
                >(
                    r, std::move(heartbeatSource), serverNameRE
                    , facilityRegistrationName, hooks, ttl, checkPeriod
                );
                return adaptNonDistinguishedRemoteFacility<
                    ProtocolWrapper, Request, Result
                >(x, facilityRegistrationName);
            }
        }
        template <class Request, class Result>
        static auto setupOneDistinguishedRemoteFacility(
            R &r 
            , std::variant<
                typename R::template ConvertibleToSourceoid<HeartbeatMessage>
                , typename R::template ConvertibleToSourceoid<std::shared_ptr<HeartbeatMessage const>>
            > &&heartbeatSource
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
                , convertToHeartbeatInput(std::move(heartbeatSource))
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
        template <template <class...> class ProtocolWrapper, class Request, class Result>
        static auto setupOneDistinguishedRemoteFacilityWithProtocol(
            R &r 
            , std::variant<
                typename R::template ConvertibleToSourceoid<HeartbeatMessage>
                , typename R::template ConvertibleToSourceoid<std::shared_ptr<HeartbeatMessage const>>
            > &&heartbeatSource
            , std::regex const &serverNameRE
            , std::string const &facilityRegistrationName
            , std::function<Request()> requestGenerator
            , std::function<bool(Request const &, Result const &)> resultChecker
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , std::chrono::system_clock::duration ttl = std::chrono::seconds(3)
            , std::chrono::system_clock::duration checkPeriod = std::chrono::seconds(5)
        ) -> DistinguishedRemoteFacility<Request, Result>
        {
            if constexpr (std::is_same_v<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                ProtocolWrapper, Request
            >, Request>) {
                return setupOneDistinguishedRemoteFacility<Request,Result>(
                    r, std::move(heartbeatSource), serverNameRE
                    , facilityRegistrationName, requestGenerator, resultChecker
                    , hooks, ttl, checkPeriod
                );
            } else {
                auto gen = [requestGenerator]() 
                    -> basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request>
                {
                    return basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                        ::template enclose<Request>(requestGenerator());
                };
                auto checker = [resultChecker](
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request> const &req 
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Result> const &res
                ) -> bool {
                    return resultChecker(
                        basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                            ::template extractConst<Request>(req)
                        , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<ProtocolWrapper>
                            ::template extractConst<Result>(res)
                    );
                };
                auto x = setupOneDistinguishedRemoteFacility<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Result>
                >(
                    r, std::move(heartbeatSource), serverNameRE
                    , facilityRegistrationName, gen, checker
                    , hooks, ttl, checkPeriod
                );
                return adaptDistinguishedRemoteFacility<
                    ProtocolWrapper, Request, Result
                >(r, x, facilityRegistrationName);   
            }
        }
        template <class FirstRequest, class FirstResult, class SecondRequest, class SecondResult>
        static auto setupTwoStepRemoteFacility(
            R &r 
            , std::variant<
                typename R::template ConvertibleToSourceoid<HeartbeatMessage>
                , typename R::template ConvertibleToSourceoid<std::shared_ptr<HeartbeatMessage const>>
            > &&heartbeatSource
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
            , NonDistinguishedRemoteFacility<SecondRequest, SecondResult>
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
                , convertToHeartbeatInput(std::move(heartbeatSource))
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
        template <
            template <class...> class FirstProtocolWrapper, class FirstRequest, class FirstResult
            , template <class...> class SecondProtocolWrapper, class SecondRequest, class SecondResult
        >
        static auto setupTwoStepRemoteFacilityWithProtocol(
            R &r 
            , std::variant<
                typename R::template ConvertibleToSourceoid<HeartbeatMessage>
                , typename R::template ConvertibleToSourceoid<std::shared_ptr<HeartbeatMessage const>>
            > &&heartbeatSource
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
            , NonDistinguishedRemoteFacility<SecondRequest, SecondResult>
        >
        {
            if constexpr (std::is_same_v<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                FirstProtocolWrapper, FirstRequest
            >, FirstRequest>) {
                auto x = setupTwoStepRemoteFacility<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<FirstProtocolWrapper, FirstRequest>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<FirstProtocolWrapper, FirstResult>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<SecondProtocolWrapper, SecondRequest>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<SecondProtocolWrapper, SecondResult>
                >(
                    r, std::move(heartbeatSource), serverNameRE
                    , facilityRegistrationNames, firstRequestGenerator, firstResultChecker
                    , hooks, ttl, checkPeriod
                );
                return {
                    std::get<0>(x)
                    , adaptNonDistinguishedRemoteFacility<SecondProtocolWrapper, SecondRequest, SecondResult>(
                        std::get<1>(x), std::get<1>(facilityRegistrationNames)
                    )
                };
            } else {
                auto gen = [firstRequestGenerator]() 
                    -> basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<FirstProtocolWrapper,FirstRequest>
                {
                    return basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<FirstProtocolWrapper>
                        ::template enclose<FirstRequest>(firstRequestGenerator());
                };
                auto checker = [firstResultChecker](
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<FirstProtocolWrapper,FirstRequest> const &req 
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<FirstProtocolWrapper,FirstResult> const &res
                ) -> bool {
                    return firstResultChecker(
                        basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<FirstProtocolWrapper>
                            ::template extractConst<FirstRequest>(req)
                        , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<FirstProtocolWrapper>
                            ::template extractConst<FirstResult>(res)
                    );
                };
                auto x = setupTwoStepRemoteFacility<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<FirstProtocolWrapper, FirstRequest>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<FirstProtocolWrapper, FirstResult>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<SecondProtocolWrapper, SecondRequest>
                    , basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<SecondProtocolWrapper, SecondResult>
                >(
                    r, std::move(heartbeatSource), serverNameRE
                    , facilityRegistrationNames, gen, checker
                    , hooks, ttl, checkPeriod
                );
                return {
                    adaptDistinguishedRemoteFacility<FirstProtocolWrapper, FirstRequest, FirstResult>(
                        r, std::get<0>(x), std::get<0>(facilityRegistrationNames)
                    )
                    , adaptNonDistinguishedRemoteFacility<SecondProtocolWrapper, SecondRequest, SecondResult>(
                        std::get<1>(x), std::get<1>(facilityRegistrationNames)
                    )
                };
            }
        }
        template <class Request, class Result>
        static auto setupSimpleRemoteFacility(
            std::string const &channelSpec
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
                case MultiTransportRemoteFacilityConnectionType::SocketRPC:
                    if constexpr(std::is_convertible_v<typename R::EnvironmentType *, socket_rpc::SocketRPCComponent *>) {
                        return socket_rpc::SocketRPCOnOrderFacility<typename R::EnvironmentType>::template createTypedRPCOnOrderFacility<Request,Result>(std::get<1>(*parsed), hooks);
                    } else {
                        throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up socket rpc facility for channel spec '"+channelSpec+"', but socket rpc is unsupported in the environment");
                    }
                    break;
                case MultiTransportRemoteFacilityConnectionType::GrpcInterop:
                    if constexpr(std::is_convertible_v<typename R::EnvironmentType *, grpc_interop::GrpcInteropComponent *>) {
                        if constexpr(DetermineClientSideIdentityForRequest<typename R::EnvironmentType, Request>::HasIdentity) {
                            if constexpr (std::is_same_v<typename DetermineClientSideIdentityForRequest<typename R::EnvironmentType, Request>::IdentityType, std::string>) {
                                if (hooks) {
                                    throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up grpc interop facility for channel spec '"+channelSpec+"', but grpc interop does not support requests with hooks");
                                } else {
                                    if constexpr (
                                        basic::bytedata_utils::ProtobufStyleSerializableChecker<Request>::IsProtobufStyleSerializable()
                                        &&
                                        basic::bytedata_utils::ProtobufStyleSerializableChecker<Result>::IsProtobufStyleSerializable()
                                    ) {
                                        return grpc_interop::GrpcClientFacilityFactory<typename R::AppType>::template createClientFacility<Request, Result>(std::get<1>(*parsed));
                                    } else if constexpr (
                                        basic::proto_interop::ProtoWrappable<Request>::value
                                        &&
                                        basic::proto_interop::ProtoWrappable<Result>::value
                                    ) {
                                        auto underlyingFacility = grpc_interop::GrpcClientFacilityFactory<typename R::AppType>::template createClientFacility<basic::proto_interop::Proto<Request>, basic::proto_interop::Proto<Result>>(std::get<1>(*parsed));
                                        auto encodingAction = R::AppType::template liftPure<typename R::AppType::template Key<Request>>(
                                            [](typename R::AppType::template Key<Request> &&key) -> typename R::AppType::template Key<basic::proto_interop::Proto<Request>> {
                                                return {
                                                    key.id()
                                                    , {key.key()}
                                                };
                                            }
                                        );
                                        auto decodingAction = R::AppType::template liftPure<typename R::AppType::template Key<basic::proto_interop::Proto<Result>>>(
                                            [](typename R::AppType::template Key<basic::proto_interop::Proto<Result>> &&key) -> typename R::AppType::template Key<Result> {
                                                return {
                                                    key.id()
                                                    , key.key().moveValue()
                                                };
                                            }
                                        );
                                        return R::AppType::wrappedOnOrderFacility(
                                            std::move(*underlyingFacility)
                                            , std::move(*encodingAction)
                                            , std::move(*decodingAction)
                                        );
                                    } else {
                                        throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up grpc interop facility for channel spec '"+channelSpec+"', but the types are not grpc compatible");
                                    }
                                }
                            } else {
                                throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up grpc interop facility for channel spec '"+channelSpec+"', but grpc interop does not support non-string identity on request");
                            }
                        } else {
                            if (hooks) {
                                throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up grpc interop facility for channel spec '"+channelSpec+"', but grpc interop does not support requests with hooks");
                            } else {
                                if constexpr (
                                    basic::bytedata_utils::ProtobufStyleSerializableChecker<Request>::IsProtobufStyleSerializable()
                                    &&
                                    basic::bytedata_utils::ProtobufStyleSerializableChecker<Result>::IsProtobufStyleSerializable()
                                ) {
                                    return grpc_interop::GrpcClientFacilityFactory<typename R::AppType>::template createClientFacility<Request, Result>(std::get<1>(*parsed));
                                } else if constexpr (
                                    basic::proto_interop::ProtoWrappable<Request>::value
                                    &&
                                    basic::proto_interop::ProtoWrappable<Result>::value
                                ) {
                                    auto underlyingFacility = grpc_interop::GrpcClientFacilityFactory<typename R::AppType>::template createClientFacility<basic::proto_interop::Proto<Request>, basic::proto_interop::Proto<Result>>(std::get<1>(*parsed));
                                    auto encodingAction = R::AppType::template liftPure<typename R::AppType::template Key<Request>>(
                                        [](typename R::AppType::template Key<Request> &&key) -> typename R::AppType::template Key<basic::proto_interop::Proto<Request>> {
                                            return {
                                                key.id()
                                                , {key.key()}
                                            };
                                        }
                                    );
                                    auto decodingAction = R::AppType::template liftPure<typename R::AppType::template Key<basic::proto_interop::Proto<Result>>>(
                                        [](typename R::AppType::template Key<basic::proto_interop::Proto<Result>> &&key) -> typename R::AppType::template Key<Result> {
                                            return {
                                                key.id()
                                                , key.key().moveValue()
                                            };
                                        }
                                    );
                                    return R::AppType::wrappedOnOrderFacility(
                                        std::move(*underlyingFacility)
                                        , std::move(*encodingAction)
                                        , std::move(*decodingAction)
                                    );
                                } else {
                                    throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up grpc interop facility for channel spec '"+channelSpec+"', but the types are not grpc compatible");
                                }
                            }
                        }
                    } else {
                        throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up grpc interop facility for channel spec '"+channelSpec+"', but grpc interop is unsupported in the environment");
                    }
                    break;
                case MultiTransportRemoteFacilityConnectionType::JsonREST:
                    if (hooks) {
                        throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up json rest facility for channel spec '"+channelSpec+"', but json rest does not support requests with hooks");
                    }
                    if constexpr(std::is_convertible_v<typename R::EnvironmentType *, json_rest::JsonRESTComponent *>) {
                        if constexpr(DetermineClientSideIdentityForRequest<typename R::EnvironmentType, Request>::HasIdentity) {
                            if constexpr (!std::is_same_v<typename DetermineClientSideIdentityForRequest<typename R::EnvironmentType, Request>::IdentityType, std::string>) {
                                throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up json rest facility for channel spec '"+channelSpec+"', but json rest does not support non-string identity on request");
                            }
                        }
                        if constexpr (
                            basic::nlohmann_json_interop::JsonWrappable<Request>::value
                            &&
                            basic::nlohmann_json_interop::JsonWrappable<Result>::value
                        ) {
                            return json_rest::JsonRESTClientFacilityFactory<typename R::AppType>::template createClientFacility<Request, Result>(std::get<1>(*parsed));
                        } else {
                            throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up json rest facility for channel spec '"+channelSpec+"', but the data types are not json-compatible");
                        }
                    } else {
                        throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up json rest facility for channel spec '"+channelSpec+"', but json rest is unsupported in the environment");
                    }
                    break;
                case MultiTransportRemoteFacilityConnectionType::WebSocket:
                    if constexpr(std::is_convertible_v<typename R::EnvironmentType *, web_socket::WebSocketComponent *>) {
                        return web_socket::WebSocketOnOrderFacilityClient<typename R::EnvironmentType>::template createTypedRPCOnOrderFacility<Request,Result>(std::get<1>(*parsed), hooks);
                    } else {
                        throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils::setupSimpleRemoteFacility] trying to set up web socket facility for channel spec '"+channelSpec+"', but web socket is unsupported in the environment");
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
        template <class Request, class Result>
        static auto setupSimpleRemoteFacility(
            R &r 
            , std::string const &channelSpec
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) ->  typename R::template OnOrderFacilityPtr<Request, Result>
        {
            return setupSimpleRemoteFacility<Request,Result>(channelSpec, hooks);
        }

        template <template<class...> class ProtocolWrapper, class Request, class Result>
        static auto setupSimpleRemoteFacilityWithProtocol(
            std::string const &channelSpec
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) ->  typename R::template OnOrderFacilityPtr<Request, Result>
        {
            if constexpr (std::is_same_v<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<
                ProtocolWrapper, Request
            >, Request>) {
                return setupSimpleRemoteFacility<Request,Result>(channelSpec, hooks);
            } else {
                auto underlyingFacility = setupSimpleRemoteFacility<
                    basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request>
                    ,basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Result>
                >(channelSpec, hooks);
                auto encodingAction = R::AppType::template liftPure<typename R::AppType::template Key<Request>>(
                    [](typename R::AppType::template Key<Request> &&key) -> typename R::AppType::template Key<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Request>> {
                        return {
                            key.id()
                            , {key.key()}
                        };
                    }
                );
                auto decodingAction = R::AppType::template liftPure<typename R::AppType::template Key<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Result>>>(
                    [](typename R::AppType::template Key<basic::WrapFacilitioidConnectorForSerializationHelpers::WrappedType<ProtocolWrapper,Result>> &&key) -> typename R::AppType::template Key<Result> {
                        return {
                            key.id()
                            , basic::WrapFacilitioidConnectorForSerializationHelpers::WrapperUtils<
                                ProtocolWrapper
                            >::template extract<Result>(key.key())
                        };
                    }
                );
                return R::AppType::wrappedOnOrderFacility(
                    std::move(*underlyingFacility)
                    , std::move(*encodingAction)
                    , std::move(*decodingAction)
                );
            }
        }
        template <template<class...> class ProtocolWrapper, class Request, class Result>
        static auto setupSimpleRemoteFacilityWithProtocol(
            R &r 
            , std::string const &channelSpec
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) ->  typename R::template OnOrderFacilityPtr<Request, Result>
        {
            return setupSimpleRemoteFacilityWithProtocol<ProtocolWrapper,Request,Result>(channelSpec, hooks);
        }

        template <class Request, class Result>
        struct SimpleRemoteFacilitioidForGraph {
            typename R::template FacilitioidConnector<Request, Result> facility;
            typename R::template Source<basic::VoidStruct> facilityFirstReady;
        };

        template <class Request, class Result>
        static auto setupSimpleRemoteFacilitioidByHeartbeat(
            R &r 
            , std::string const &heartbeatSpec
            , std::string const &heartbeatTopic
            , std::regex const &facilityServerHeartbeatIdentityRE
            , std::string const &facilityRegistrationName
            , std::string const &prefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
            , std::chrono::system_clock::duration ttl = std::chrono::seconds(3)
            , std::chrono::system_clock::duration checkPeriod = std::chrono::seconds(5)
        ) ->  SimpleRemoteFacilitioidForGraph<Request, Result>
        {
            auto heartbeatSource = MultiTransportBroadcastListenerManagingUtils<R>::template oneBroadcastListener<HeartbeatMessage>(
                r 
                , prefix 
                , heartbeatSpec
                , heartbeatTopic
                , heartbeatHook
            );
            auto facility = setupOneNonDistinguishedRemoteFacility<Request, Result>(
                r 
                , typename R::template ConvertibleToSourceoid<HeartbeatMessage> {std::move(heartbeatSource)}
                , facilityServerHeartbeatIdentityRE
                , facilityRegistrationName
                , hooks
                , ttl
                , checkPeriod
            );
            auto convertToFirstReady = M::template liftMaybe<std::size_t>(
                [](std::size_t &&x) -> std::optional<basic::VoidStruct> {
                    if (x > 0) {
                        return basic::VoidStruct {};
                    } else {
                        return std::nullopt;
                    }
                }
            );
            r.registerAction(prefix+"/convertToFirstReady", convertToFirstReady);
            facility.feedUnderlyingCount(r, r.actionAsSink(convertToFirstReady));
            auto discard = M::template trivialExporter<basic::VoidStruct>();
            r.registerExporter(prefix+"/discard", discard);
            r.exportItem(discard, r.actionAsSource(convertToFirstReady));
            return {
                facility.facility
                , r.actionAsSource(convertToFirstReady)
            };
        }
        
        template <class Request, class Result>
        static auto setupSimpleRemoteFacilitioid(
            R &r 
            , SimpleRemoteFacilitySpec const &spec
            , std::string const &prefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) ->  SimpleRemoteFacilitioidForGraph<Request, Result> 
        {
            return std::visit(
                [&r,prefix,hooks](auto const &x) -> SimpleRemoteFacilitioidForGraph<Request, Result>
                {
                    using T = std::decay_t<decltype(x)>;
                    if constexpr (std::is_same_v<T, std::string>) {
                        auto facility = setupSimpleRemoteFacility<Request,Result>(r, x, hooks);
                        r.registerOnOrderFacility(prefix+"/facility", facility);
                        auto trigger = M::template constFirstPushImporter<basic::VoidStruct>();
                        r.registerImporter(prefix+"/trigger", trigger);
                        auto discard = M::template trivialExporter<basic::VoidStruct>();
                        r.registerExporter(prefix+"/discard", discard);
                        r.exportItem(discard, r.importItem(trigger));
                        return {
                            r.facilityConnector(facility)
                            , r.importItem(trigger)
                        };
                    } else {
                        return setupSimpleRemoteFacilitioidByHeartbeat<Request,Result>(
                            r 
                            , x.heartbeatSpec
                            , x.heartbeatTopic
                            , x.facilityServerHeartbeatIdentityRE
                            , x.facilityRegistrationName
                            , prefix
                            , hooks
                            , x.heartbeatHook
                            , x.ttl
                            , x.checkPeriod
                        );
                    }
                }
                , spec
            );
        }
    };
    
} } } }

#endif