#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_HEARBEAT_MESSAGE_TO_REMOTE_FACILITY_COMMAND_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_HEARBEAT_MESSAGE_TO_REMOTE_FACILITY_COMMAND_HPP_

#include <tm_kit/infra/RealTimeMonad.hpp>

#include <tm_kit/transport/MultiTransportRemoteFacility.hpp>
#include <tm_kit/transport/HeartbeatMessage.hpp>

#include <memory>
#include <chrono>
#include <regex>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    class HeartbeatMessageToRemoteFacilityCommandImpl;

    class HeartbeatMessageToRemoteFacilityCommand {
    private:
        std::unique_ptr<HeartbeatMessageToRemoteFacilityCommandImpl> impl_;
    public:
        HeartbeatMessageToRemoteFacilityCommand(
            std::regex const &senderRE
            , std::regex const &facilityEntryRE
            , std::chrono::system_clock::duration ttl
        );
        ~HeartbeatMessageToRemoteFacilityCommand();

        std::vector<MultiTransportRemoteFacilityAction> processHeartbeatMessage(
            int which
            , std::tuple<std::chrono::system_clock::time_point, HeartbeatMessage> &&heartbeat 
            , std::tuple<std::chrono::system_clock::time_point, basic::VoidStruct> &&timer
        );

        static std::string buildStatusInfo(MultiTransportRemoteFacilityConnectionType connType, ConnectionLocator const &locator);
        static std::string buildStatusInfo(MultiTransportRemoteFacilityConnectionType connType, std::string const &locatorDescription);
        static std::optional<MultiTransportRemoteFacilityAction> parseStatusInfo(MultiTransportRemoteFacilityActionType actionType, std::string const &statusInfo);
    };

    template <class M>
    inline typename M::template ActionPtr<
        std::variant<
            HeartbeatMessage
            , basic::VoidStruct
        >
        , MultiTransportRemoteFacilityAction
    > heartbeatMessageToRemoteFacilityCommand(
        std::regex const &senderRE
        , std::regex const &facilityEntryRE
        , std::chrono::system_clock::duration ttl)
    {
        auto p = std::make_shared<HeartbeatMessageToRemoteFacilityCommand>(
            senderRE, facilityEntryRE, ttl
        );
        return M::template enhancedMulti2<HeartbeatMessage, basic::VoidStruct>(
            [p](int which
            , std::tuple<std::chrono::system_clock::time_point, HeartbeatMessage> &&heartbeat 
            , std::tuple<std::chrono::system_clock::time_point, basic::VoidStruct> &&timer) {
                return p->processHeartbeatMessage(which, std::move(heartbeat), std::move(timer));
            }
        );
    }

} } } }

#endif