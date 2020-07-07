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
    };
    
} } } }

#endif