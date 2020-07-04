#include <tm_kit/transport/HeartbeatMessageToRemoteFacilityCommand.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    class HeartbeatMessageToRemoteFacilityCommandImpl {
    private:
        std::regex senderRE_;
        std::regex facilityEntryRE_;
        std::chrono::system_clock::duration ttl_;

        std::unordered_map<std::string, std::chrono::system_clock::time_point> lastGoodTime_;
        std::mutex mutex_;
    public:
        HeartbeatMessageToRemoteFacilityCommandImpl(
            std::regex const &senderRE
            , std::regex const &facilityEntryRE
            , std::chrono::system_clock::duration ttl
        ) : senderRE_(senderRE), facilityEntryRE_(facilityEntryRE), ttl_(ttl), lastGoodTime_()
        {}
        ~HeartbeatMessageToRemoteFacilityCommandImpl() {}

        std::vector<MultiTransportRemoteFacilityAction> processHeartbeatMessage(
            int which
            , std::tuple<std::chrono::system_clock::time_point, HeartbeatMessage> &&heartBeat 
            , std::tuple<std::chrono::system_clock::time_point, basic::VoidStruct> &&timer
        ) {
            std::lock_guard<std::mutex> _(mutex_);
            if (which == 0) {
                auto const &h = std::get<1>(heartBeat);
                if (!std::regex_match(h.senderDescription(), senderRE_)) {
                    return {};
                }
                auto entries = h.allEntriesRE(senderRE_);
                if (entries.size() != 1) {
                    return {};
                }
                auto status = h.status(*(entries.begin()));
                if (status.status == HeartbeatMessage::Status::Bad || status.status == HeartbeatMessage::Status::Unknown) {
                    lastGoodTime_.erase(status.info);
                    auto cmd = HeartbeatMessageToRemoteFacilityCommand::parseStatusInfo(
                        MultiTransportRemoteFacilityActionType::Deregister
                        , status.info
                    );
                    if (cmd) {
                        return {*cmd};
                    } else {
                        return {};
                    }
                } else {
                    auto iter = lastGoodTime_.find(status.info);
                    if (iter == lastGoodTime_.end()) {
                        auto cmd = HeartbeatMessageToRemoteFacilityCommand::parseStatusInfo(
                            MultiTransportRemoteFacilityActionType::Register
                            , status.info
                        );
                        if (cmd) {
                            lastGoodTime_.insert({
                                status.info
                                , std::get<0>(heartBeat)
                            });
                            return {*cmd};
                        } else {
                            return {};
                        }
                    } else {
                        iter->second = std::get<0>(heartBeat);
                        return {};
                    }
                }
            } else {
                std::vector<MultiTransportRemoteFacilityAction> ret;
                std::vector<std::string> toDelete;
                for (auto const &item : lastGoodTime_) {
                    if (item.second+ttl_ < std::get<0>(timer)) {
                        auto cmd = HeartbeatMessageToRemoteFacilityCommand::parseStatusInfo(
                            MultiTransportRemoteFacilityActionType::Deregister
                            , item.first
                        );
                        if (cmd) {
                            ret.push_back(*cmd);
                        }
                    }
                    toDelete.push_back(item.first);
                }
                for (auto const &s : toDelete) {
                    lastGoodTime_.erase(s);
                }
                return ret;
            }
        }
    };

    HeartbeatMessageToRemoteFacilityCommand::HeartbeatMessageToRemoteFacilityCommand(
        std::regex const &senderRE
        , std::regex const &facilityEntryRE
        , std::chrono::system_clock::duration ttl
    ) : impl_(std::make_unique<HeartbeatMessageToRemoteFacilityCommandImpl>(
        senderRE, facilityEntryRE, ttl
    )) {}
    HeartbeatMessageToRemoteFacilityCommand::~HeartbeatMessageToRemoteFacilityCommand() {}

    std::vector<MultiTransportRemoteFacilityAction> HeartbeatMessageToRemoteFacilityCommand::processHeartbeatMessage(
        int which
        , std::tuple<std::chrono::system_clock::time_point, HeartbeatMessage> &&heartBeat 
        , std::tuple<std::chrono::system_clock::time_point, basic::VoidStruct> &&timer
    ) {
        return impl_->processHeartbeatMessage(which, std::move(heartBeat), std::move(timer));
    }

    std::string HeartbeatMessageToRemoteFacilityCommand::buildStatusInfo(MultiTransportRemoteFacilityConnectionType connType, ConnectionLocator const &locator) {
        return buildStatusInfo(connType, locator.toSerializationFormat());
    }
    std::string HeartbeatMessageToRemoteFacilityCommand::buildStatusInfo(MultiTransportRemoteFacilityConnectionType connType, std::string const &locatorDescription) {
        std::ostringstream oss;
        switch (connType) {
        case MultiTransportRemoteFacilityConnectionType::RabbitMQ:
            oss << "rabbitmq://";
            break;
        case MultiTransportRemoteFacilityConnectionType::Redis:
            oss << "redis://";
            break;
        default:
            return "";
        }
        oss << locatorDescription;
        return oss.str();
    }
    std::optional<MultiTransportRemoteFacilityAction> HeartbeatMessageToRemoteFacilityCommand::parseStatusInfo(MultiTransportRemoteFacilityActionType actionType, std::string const &statusInfo) {
        if (boost::starts_with(statusInfo, "rabbitmq://")) {
            return MultiTransportRemoteFacilityAction {
                actionType
                , MultiTransportRemoteFacilityConnectionType::RabbitMQ
                , statusInfo.substr(std::string("rabbitmq://").length())
            };
        } else if (boost::starts_with(statusInfo, "redis://")) {
            return MultiTransportRemoteFacilityAction {
                actionType
                , MultiTransportRemoteFacilityConnectionType::Redis
                , statusInfo.substr(std::string("redis://").length())
            };
        } else {
            return std::nullopt;
        }
    }

} } } }