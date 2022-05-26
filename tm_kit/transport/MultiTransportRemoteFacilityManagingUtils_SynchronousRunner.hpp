#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_MANAGING_UTILS_SYNCHRONOUS_RUNNER_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_REMOTE_FACILITY_MANAGING_UTILS_SYNCHRONOUS_RUNNER_HPP_

#include <tm_kit/infra/SynchronousRunner.hpp>
#include <tm_kit/transport/MultiTransportRemoteFacilityManagingUtils.hpp>
#include <tm_kit/transport/MultiTransportBroadcastListenerManagingUtils_SynchronousRunner.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class M>
    class MultiTransportRemoteFacilityManagingUtils<
        infra::SynchronousRunner<M>
    > {
    public:
        using R = infra::SynchronousRunner<M>;

        template <class Request, class Result>
        static auto setupSimpleRemoteFacility(
            std::string const &channelSpec
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) ->  typename R::template OnOrderFacilityPtr<Request, Result>
        {
            return MultiTransportRemoteFacilityManagingUtils<infra::AppRunner<M>>
                ::template setupSimpleRemoteFacility<Request,Result>(
                    channelSpec, hooks
                );
        }
        template <class Request, class Result>
        static auto setupSimpleRemoteFacility(
            infra::SynchronousRunner<M> &r 
            , std::string const &channelSpec
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename R::template OnOrderFacilityPtr<Request, Result>
        {
            return setupSimpleRemoteFacility<Request,Result>(
                    channelSpec, hooks
                );
        }

        static std::string getRemoteFacilityChannelFromHeartbeat(
            infra::SynchronousRunner<M> &r 
            , std::string const &heartbeatSpec
            , std::string const &heartbeatTopic
            , std::regex const &facilityServerHeartbeatIdentityRE
            , std::string const &facilityRegistrationName
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
        ) {
            auto heartbeatImporter = MultiTransportBroadcastListenerManagingUtils<R>::template oneBroadcastListenerWithTopic<HeartbeatMessage>(
                r 
                , heartbeatSpec
                , heartbeatTopic
                , heartbeatHook
            );
            auto heartbeatMsg = r.importItemUntil(
                heartbeatImporter 
                , [facilityServerHeartbeatIdentityRE,facilityRegistrationName](typename M::template InnerData<basic::TypedDataWithTopic<HeartbeatMessage>> const &h) {
                    if (!std::regex_match(h.timedData.value.content.senderDescription(), facilityServerHeartbeatIdentityRE)) {
                        return false;
                    }
                    auto iter = h.timedData.value.content.facilityChannels().find(facilityRegistrationName);
                    return (iter != h.timedData.value.content.facilityChannels().end());
                }
            )->consumeUntilLastFuture().get();
            if (!std::regex_match(heartbeatMsg.timedData.value.content.senderDescription(), facilityServerHeartbeatIdentityRE)) {
                throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils (Synchronous Runner)::setupSimpleRemoteFacilityByHeartbeat] Cannot find heartbeat for server that matches the RE");
            }
            auto iter = heartbeatMsg.timedData.value.content.facilityChannels().find(facilityRegistrationName);
            if (iter == heartbeatMsg.timedData.value.content.facilityChannels().end()) {
                throw std::runtime_error("[MultiTransportRemoteFacilityManagingUtils (Synchronous Runner)::setupSimpleRemoteFacilityByHeartbeat] Cannot find heartbeat entry for facility '"+facilityRegistrationName+"'");
            }
            return iter->second;
        }
        template <class Request, class Result>
        static auto setupSimpleRemoteFacilityByHeartbeat(
            infra::SynchronousRunner<M> &r 
            , std::string const &heartbeatSpec
            , std::string const &heartbeatTopic
            , std::regex const &facilityServerHeartbeatIdentityRE
            , std::string const &facilityRegistrationName
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
        ) ->  typename R::template OnOrderFacilityPtr<Request, Result>
        {
            auto channel = getRemoteFacilityChannelFromHeartbeat(
                r 
                , heartbeatSpec
                , heartbeatTopic
                , facilityServerHeartbeatIdentityRE
                , facilityRegistrationName
                , heartbeatHook
            );
            return setupSimpleRemoteFacility<Request,Result>(
                r 
                , channel
                , hooks
            );
        }
        
        template <class Request, class Result>
        static auto setupSimpleRemoteFacility(
            R &r 
            , SimpleRemoteFacilitySpec const &spec
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) ->  typename R::template OnOrderFacilityPtr<Request, Result>
        {
            return std::visit(
                [&r,hooks](auto const &x) -> typename R::template OnOrderFacilityPtr<Request, Result>
                {
                    using T = std::decay_t<decltype(x)>;
                    if constexpr (std::is_same_v<T, std::string>) {
                        return setupSimpleRemoteFacility<Request,Result>(r,x,hooks);
                    } else {
                        return setupSimpleRemoteFacilityByHeartbeat<Request,Result>(
                            r 
                            , x.heartbeatSpec
                            , x.heartbeatTopic
                            , x.facilityServerHeartbeatIdentityRE
                            , x.facilityRegistrationName
                            , hooks
                            , x.heartbeatHook
                        );
                    }
                }
                , spec
            );
        }

        template <class R1, class Request, class Result>
        static auto setupSimpleRemoteFacilityByHeartbeatForAnotherRunner(
            infra::SynchronousRunner<M> &r 
            , R1 &r1
            , std::string const &heartbeatSpec
            , std::string const &heartbeatTopic
            , std::regex const &facilityServerHeartbeatIdentityRE
            , std::string const &facilityRegistrationName
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
        ) {
            auto channel = getRemoteFacilityChannelFromHeartbeat(
                r 
                , heartbeatSpec
                , heartbeatTopic
                , heartbeatHook
                , facilityServerHeartbeatIdentityRE
                , facilityRegistrationName
                , heartbeatHook
            );
            return MultiTransportRemoteFacilityManagingUtils<R1>::
                template setupSimpleRemoteFacility<Request, Result>( 
                    channel, hooks
                );
        }
        template <class R1, class Request, class Result>
        static auto setupOneDistinguishedFacilityByHeartbeatForAnotherRunner(
            infra::SynchronousRunner<M> &r 
            , R1 &r1
            , std::string const &prefix
            , std::string const &heartbeatSpec
            , std::string const &heartbeatTopic
            , std::regex const &facilityServerHeartbeatIdentityRE
            , std::string const &facilityRegistrationName
            , std::function<Request()> requestGenerator
            , std::function<bool(Request const &, Result const &)> resultChecker
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , std::optional<WireToUserHook> heartbeatHook = std::nullopt
        ) {
            auto channel = getRemoteFacilityChannelFromHeartbeat(
                r 
                , heartbeatSpec
                , heartbeatTopic
                , heartbeatHook
                , facilityServerHeartbeatIdentityRE
                , facilityRegistrationName
                , heartbeatHook
            );
            return MultiTransportRemoteFacilityManagingUtils<R1>::
                template setupOneDistinguishedRemoteFacility_NoHeartbeat<Request, Result>( 
                    r1, prefix, channel, requestGenerator, resultChecker, hooks
                );
        }
    };
    
} } } }

#endif