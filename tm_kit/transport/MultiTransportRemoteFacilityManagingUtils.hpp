#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_MANAGING_UTILS_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_MANAGING_UTILS_HPP_

#include <tm_kit/transport/MultiTransportRemoteFacility.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class R>
    class MultiTransportRemoteFacilityManagingUtils {
    public:
        using M = typename R::MonadType;

        template <class ... IdentitiesAndInputsAndOutputs>
        using NonDistinguishedRemoteFacilities =
            std::tuple<
                typename R::template FacilitioidConnector<
                    std::tuple_element_t<1,IdentitiesAndInputsAndOutputs>
                    , std::tuple_element_t<2,IdentitiesAndInputsAndOutputs>
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
            , std::function<std::optional<ByteDataHookPair>(ConnectionLocator const &)> const &hookPairFactory
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
            , std::function<std::optional<ByteDataHookPair>(ConnectionLocator const &)> const &hookPairFactory
            , Output &output
        ) {
            auto facility = M::localOnOrderFacility(
                new MultiTransportRemoteFacility<
                    typename M::EnvironmentType
                    , std::tuple_element_t<1, FirstRemainingIdentityAndInputAndOutput>
                    , std::tuple_element_t<2, FirstRemainingIdentityAndInputAndOutput>
                    , std::tuple_element_t<0, FirstRemainingIdentityAndInputAndOutput>
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
    public:
        template <class ... IdentitiesAndInputsAndOutputs>
        static auto setupNonDistinguishedRemoteFacilities(
            R &r
            , typename R::template Source<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(IdentitiesAndInputsAndOutputs)>
            > &&actionSource
            , std::array<std::string, sizeof...(IdentitiesAndInputsAndOutputs)> const &names
            , std::string const &prefix
            , std::function<std::optional<ByteDataHookPair>(ConnectionLocator const &)> const &hookPairFactory
                = [](ConnectionLocator const &) {return std::nullopt;}
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
                    std::tuple_element_t<1,IdentitiesAndInputsAndOutputs>
                    , std::tuple_element_t<2,IdentitiesAndInputsAndOutputs>
                >...
            >;

        template <class ... IdentitiesAndInputsAndOutputs>
        using DistinguishedRemoteFacilityInitiators =
            std::tuple<
                std::tuple_element_t<1,IdentitiesAndInputsAndOutputs>...
            >;

        template <class Input, class Output>
        using DistinguishedRemoteFacilityInitialCallbackPicker =
            std::function<bool(Input const &, Output const &)>;

        template <class ... IdentitiesAndInputsAndOutputs>
        using DistinguishedRemoteFacilityInitialCallbackPickers =
            std::tuple<
                DistinguishedRemoteFacilityInitialCallbackPicker<
                    std::tuple_element_t<1,IdentitiesAndInputsAndOutputs>
                    , std::tuple_element_t<2,IdentitiesAndInputsAndOutputs>
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
            , std::function<std::optional<ByteDataHookPair>(ConnectionLocator const &)> const &hookPairFactory
            , Output &output
        ) 
        {}

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
            , std::function<std::optional<ByteDataHookPair>(ConnectionLocator const &)> const &hookPairFactory
            , Output &output
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
                    , std::tuple_element_t<1, FirstRemainingIdentityAndInputAndOutput>
                >
            {
                return {std::get<0>(x).connectionLocator, initiator};
            };

            auto initialCallbackPicker = std::get<CurrentIdx>(initialCallbackPickers);

            auto vieInitialCallbackFilter =
                [initialCallbackPicker](typename M::template KeyedData<
                    std::tuple<
                        ConnectionLocator
                        , std::tuple_element_t<1, FirstRemainingIdentityAndInputAndOutput>
                    >
                    , std::tuple_element_t<2, FirstRemainingIdentityAndInputAndOutput>
                > const &x) -> bool {
                    return initialCallbackPicker(std::get<1>(x.key.key()), x.data);
                };

            auto facility = M::vieOnOrderFacility(
                new transport::MultiTransportRemoteFacility<
                    typename M::EnvironmentType
                    , std::tuple_element_t<1, FirstRemainingIdentityAndInputAndOutput>
                    , std::tuple_element_t<2, FirstRemainingIdentityAndInputAndOutput>
                    , std::tuple_element_t<0, FirstRemainingIdentityAndInputAndOutput>
                    , transport::MultiTransportRemoteFacilityDispatchStrategy::Designated
                >
            );

            auto facilityLoopOutput = basic::MonadRunnerUtilComponents<R>::setupVIEFacilitySelfLoopAndWait(
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

            std::get<CurrentIdx>(output) = 
                DistinguishedRemoteFacility<
                    std::tuple_element_t<1, FirstRemainingIdentityAndInputAndOutput>
                    , std::tuple_element_t<2, FirstRemainingIdentityAndInputAndOutput>
                > {
                    facilityLoopOutput.callIntoFacility
                    , r.sourceAsSourceoid(facilityLoopOutput.facilityOutput.clone())
                    , r.sourceAsSourceoid(r.execute(extraOutputConverter, facilityLoopOutput.facilityExtraOutput.clone()))
                };

            if constexpr (sizeof...(RemainingIdentitiesAndInputsAndOutputs) == 0) {
                auto emptyExporter = M::template trivialExporter<InputArray>();
                r.exportItem(prefix+"/"+names[CurrentIdx]+"/loop/emptyExporter", emptyExporter, std::move(facilityLoopOutput.nextTriggeringSource));
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
                    , output
                );
            }
        }
    public:
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
            , std::function<std::optional<ByteDataHookPair>(ConnectionLocator const &)> const &hookPairFactory
                = [](ConnectionLocator const &) {return std::nullopt;}
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
                    , output
                );
                return output;
            }
        }
    };
    
} } } }

#endif