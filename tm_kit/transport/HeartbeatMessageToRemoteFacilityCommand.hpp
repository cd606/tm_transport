#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_HEARBEAT_MESSAGE_TO_REMOTE_FACILITY_COMMAND_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_HEARBEAT_MESSAGE_TO_REMOTE_FACILITY_COMMAND_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>

#include <tm_kit/transport/MultiTransportRemoteFacility.hpp>
#include <tm_kit/transport/HeartbeatMessage.hpp>

#include <memory>
#include <chrono>
#include <regex>
#include <unordered_map>
#include <array>
#include <mutex>

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class ... Specs>
    class HeartbeatMessageToRemoteFacilityCommandConverter {
    };

    /*
    * For now, we do not try to do auto-subscription and auto-de-subscription
    * on broadcast channels. The reason is that it is usually acceptable to
    * simply listen to a fixed list of broadcast channels and do not take any
    * specific action when they get up or down. The broadcast channel data are
    * supposed to be history-less and if there is a serious history dependency
    * then a facility channel is usually the correct choice. 
    */
    template <
        class ... DistinguishedFacilitySpecs
        , class ... NonDistinguishedFacilitySpecs
    >
    class HeartbeatMessageToRemoteFacilityCommandConverter<
        std::tuple<DistinguishedFacilitySpecs...>
        , std::tuple<NonDistinguishedFacilitySpecs...>
    > {
    private:
        std::regex senderRE_;
        std::array<std::string, sizeof...(DistinguishedFacilitySpecs)> distinguishedFacilityNames_;
        std::array<std::string, sizeof...(NonDistinguishedFacilitySpecs)> nonDistinguishedFacilityNames_; 
        std::chrono::system_clock::duration ttl_;

        struct OneServerInfo {
            bool good_ = false;
            std::array<std::tuple<MultiTransportRemoteFacilityConnectionType, ConnectionLocator>, sizeof...(DistinguishedFacilitySpecs)> distinguishedFacilityLocators_;
            std::array<std::tuple<MultiTransportRemoteFacilityConnectionType, ConnectionLocator>, sizeof...(NonDistinguishedFacilitySpecs)> nonDistinguishedFacilityLocators_;
            std::chrono::system_clock::time_point lastGoodTime_;

            bool hasSameInfo(OneServerInfo const &info) const {
                for (int ii=0; ii < sizeof...(DistinguishedFacilitySpecs); ++ii) {
                    if (distinguishedFacilityLocators_[ii] != info.distinguishedFacilityLocators_[ii]) {
                        return false;
                    }
                }
                for (int ii=0; ii < sizeof...(NonDistinguishedFacilitySpecs); ++ii) {
                    if (nonDistinguishedFacilityLocators_[ii] != info.nonDistinguishedFacilityLocators_[ii]) {
                        return false;
                    }
                }
                return true;
            }
        };

        std::unordered_map<std::string, OneServerInfo> serverInfos_;
        std::mutex mutex_;

        static std::optional<std::tuple<MultiTransportRemoteFacilityConnectionType, ConnectionLocator>>
            parseServerSpec(std::string const &s) {
                if (boost::starts_with(s, "rabbitmq://")) {
                    try {
                        return std::tuple<MultiTransportRemoteFacilityConnectionType, ConnectionLocator> {
                            MultiTransportRemoteFacilityConnectionType::RabbitMQ
                            , ConnectionLocator::parse(s.substr(std::string("rabbitmq://").length()))
                        };
                    } catch (ConnectionLocatorParseError const &) {
                        return std::nullopt;
                    }
                } else if (boost::starts_with(s, "redis://")) {
                    try {
                        return std::tuple<MultiTransportRemoteFacilityConnectionType, ConnectionLocator> {
                            MultiTransportRemoteFacilityConnectionType::Redis
                            , ConnectionLocator::parse(s.substr(std::string("redis://").length()))
                        };
                    } catch (ConnectionLocatorParseError const &) {
                        return std::nullopt;
                    }
                } else if (boost::starts_with(s, "socket_rpc://")) {
                    try {
                        return std::tuple<MultiTransportRemoteFacilityConnectionType, ConnectionLocator> {
                            MultiTransportRemoteFacilityConnectionType::SocketRPC
                            , ConnectionLocator::parse(s.substr(std::string("socket_rpc://").length()))
                        };
                    } catch (ConnectionLocatorParseError const &) {
                        return std::nullopt;
                    }
                } else if (boost::starts_with(s, "grpc_interop://")) {
                    try {
                        return std::tuple<MultiTransportRemoteFacilityConnectionType, ConnectionLocator> {
                            MultiTransportRemoteFacilityConnectionType::GrpcInterop
                            , ConnectionLocator::parse(s.substr(std::string("grpc_interop://").length()))
                        };
                    } catch (ConnectionLocatorParseError const &) {
                        return std::nullopt;
                    }
                } else if (boost::starts_with(s, "json_rest://")) {
                    try {
                        return std::tuple<MultiTransportRemoteFacilityConnectionType, ConnectionLocator> {
                            MultiTransportRemoteFacilityConnectionType::JsonREST
                            , ConnectionLocator::parse(s.substr(std::string("json_rest://").length()))
                        };
                    } catch (ConnectionLocatorParseError const &) {
                        return std::nullopt;
                    }
                } else {
                    return std::nullopt;
                }
            }
    public:
        HeartbeatMessageToRemoteFacilityCommandConverter(
            std::regex const &senderRE
            , std::array<std::string, sizeof...(DistinguishedFacilitySpecs)> distinguishedFacilityNames
            , std::array<std::string, sizeof...(NonDistinguishedFacilitySpecs)> nonDistinguishedFacilityNames
            , std::chrono::system_clock::duration ttl
        ) 
            :
            senderRE_(senderRE)
            , distinguishedFacilityNames_(distinguishedFacilityNames)
            , nonDistinguishedFacilityNames_(nonDistinguishedFacilityNames)
            , ttl_(ttl)
            , serverInfos_()
            , mutex_()
        {
        }
        HeartbeatMessageToRemoteFacilityCommandConverter(HeartbeatMessageToRemoteFacilityCommandConverter &&h)
            : 
            senderRE_(std::move(h.senderRE_))
            , distinguishedFacilityNames_(std::move(h.distinguishedFacilityNames_))
            , nonDistinguishedFacilityNames_(std::move(h.nonDistinguishedFacilityNames_))
            , ttl_(std::move(h.ttl_))
            , serverInfos_(std::move(h.serverInfos_))
            , mutex_()
        {}
        HeartbeatMessageToRemoteFacilityCommandConverter(HeartbeatMessageToRemoteFacilityCommandConverter const &) = delete;
        HeartbeatMessageToRemoteFacilityCommandConverter &operator=(HeartbeatMessageToRemoteFacilityCommandConverter const &) = delete;
        HeartbeatMessageToRemoteFacilityCommandConverter &operator=(HeartbeatMessageToRemoteFacilityCommandConverter &&) = delete;
    private:
        void handleHeartbeat(
            HeartbeatMessage const &h 
            , std::vector<std::tuple<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedFacilitySpecs)>
                , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedFacilitySpecs)>
            >> &output
        ) {
            int ii = 0;
            if (!std::regex_match(h.senderDescription(), senderRE_)) {
                output.clear();
                return;
            }
            auto uuidStr = h.uuidStr();
            auto iter = serverInfos_.find(uuidStr);
            if (iter == serverInfos_.end()) {
                iter = serverInfos_.insert({uuidStr, OneServerInfo{}}).first;
            }

            if (!iter->second.good_) {
                auto const &facilityInfo = h.facilityChannels();
                ii = 0;
                for (auto const &nm : distinguishedFacilityNames_) {
                    auto hIter = facilityInfo.find(nm);
                    if (hIter == facilityInfo.end()) {
                        output.clear();
                        return;
                    }
                    auto spec = parseServerSpec(hIter->second);
                    if (!spec) {
                        output.clear();
                        return;
                    }
                    iter->second.distinguishedFacilityLocators_[ii] = *spec;
                    ++ii;
                }
                ii = 0;
                for (auto const &nm : nonDistinguishedFacilityNames_) {
                    auto hIter = facilityInfo.find(nm);
                    if (hIter == facilityInfo.end()) {
                        output.clear();
                        return;
                    }
                    auto spec = parseServerSpec(hIter->second);
                    if (!spec) {
                        output.clear();
                        return;
                    }
                    iter->second.nonDistinguishedFacilityLocators_[ii] = *spec;
                    ++ii;
                }
                
                iter->second.good_ = true;

                std::vector<std::string> duplicates;
                for (auto const &item : serverInfos_) {
                    if (item.first == iter->first) {
                        continue;
                    }
                    if (item.second.hasSameInfo(iter->second)) {
                        duplicates.push_back(item.first);
                    }
                }
                for (auto const &id : duplicates) {
                    serverInfos_.erase(id);
                }

                if (!duplicates.empty()) {
                    std::tuple<
                        std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedFacilitySpecs)>
                        , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedFacilitySpecs)>
                    > deRegisterCmds;
                    ii = 0;
                    for (auto const &l : iter->second.distinguishedFacilityLocators_) {
                        std::get<0>(deRegisterCmds)[ii] = MultiTransportRemoteFacilityAction {
                            MultiTransportRemoteFacilityActionType::Deregister
                            , std::get<0>(l)
                            , std::get<1>(l)
                            , distinguishedFacilityNames_[ii]
                        };
                        ++ii;
                    }
                    ii = 0;
                    for (auto const &l : iter->second.nonDistinguishedFacilityLocators_) {
                        std::get<1>(deRegisterCmds)[ii] = MultiTransportRemoteFacilityAction {
                            MultiTransportRemoteFacilityActionType::Deregister
                            , std::get<0>(l)
                            , std::get<1>(l)
                            , nonDistinguishedFacilityNames_[ii]
                        };
                        ++ii;
                    }
                    output.push_back(deRegisterCmds);
                }
                std::tuple<
                    std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedFacilitySpecs)>
                    , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedFacilitySpecs)>
                > registerCmds;
                ii = 0;
                for (auto const &l : iter->second.distinguishedFacilityLocators_) {
                    std::get<0>(registerCmds)[ii] = MultiTransportRemoteFacilityAction {
                        MultiTransportRemoteFacilityActionType::Register
                        , std::get<0>(l)
                        , std::get<1>(l)
                        , distinguishedFacilityNames_[ii]
                    };
                    ++ii;
                }
                ii = 0;
                for (auto const &l : iter->second.nonDistinguishedFacilityLocators_) {
                    std::get<1>(registerCmds)[ii] = MultiTransportRemoteFacilityAction {
                        MultiTransportRemoteFacilityActionType::Register
                        , std::get<0>(l)
                        , std::get<1>(l)
                        , nonDistinguishedFacilityNames_[ii]
                    };
                    ++ii;
                }
                output.push_back(registerCmds);
            }
            //we don't trust the heartbeat message's "time" since the clocks
            //may be out of sync
            iter->second.lastGoodTime_ = std::chrono::system_clock::now();
        }
        void handleTimer(
            std::chrono::system_clock::time_point const &tp
            , std::vector<std::tuple<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedFacilitySpecs)>
                , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedFacilitySpecs)>
            >> &output
        ) {
            int ii = 0;
            std::vector<std::string> toDelete;
            for (auto const &item : serverInfos_) {
                if (!item.second.good_) {
                    continue;
                }
                if (item.second.lastGoodTime_+ttl_ < tp) {
                    std::tuple<
                        std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedFacilitySpecs)>
                        , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedFacilitySpecs)>
                    > deRegisterCmds;
                    ii = 0;
                    for (auto const &l : item.second.distinguishedFacilityLocators_) {
                        std::get<0>(deRegisterCmds)[ii] = MultiTransportRemoteFacilityAction {
                            MultiTransportRemoteFacilityActionType::Deregister
                            , std::get<0>(l)
                            , std::get<1>(l)
                            , distinguishedFacilityNames_[ii]
                        };
                        ++ii;
                    }
                    ii = 0;
                    for (auto const &l : item.second.nonDistinguishedFacilityLocators_) {
                        std::get<1>(deRegisterCmds)[ii] = MultiTransportRemoteFacilityAction {
                            MultiTransportRemoteFacilityActionType::Deregister
                            , std::get<0>(l)
                            , std::get<1>(l)
                            , nonDistinguishedFacilityNames_[ii]
                        };
                        ++ii;
                    }
                    output.push_back(deRegisterCmds);
                    toDelete.push_back(item.first);
                }
            }
            for (auto const &s : toDelete) {
                serverInfos_.erase(s);
            }
        }
    public:
        std::vector<std::tuple<
            std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedFacilitySpecs)>
            , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedFacilitySpecs)>
        >> operator()(
            std::tuple<std::chrono::system_clock::time_point, std::variant<HeartbeatMessage, basic::VoidStruct>> &&data
        ) {
            std::vector<std::tuple<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedFacilitySpecs)>
                , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedFacilitySpecs)>
            >> output;
            int ii = 0;
            std::lock_guard<std::mutex> _(mutex_);
            if (std::get<1>(data).index() == 0) {
                handleHeartbeat(std::get<0>(std::get<1>(data)), output);
            } else {
                handleTimer(std::get<0>(data), output);
            }
            return output;
        }
        std::vector<std::tuple<
            std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedFacilitySpecs)>
            , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedFacilitySpecs)>
        >> operator()(
            std::tuple<std::chrono::system_clock::time_point, std::variant<std::shared_ptr<HeartbeatMessage const>, basic::VoidStruct>> &&data
        ) {
            std::vector<std::tuple<
                std::array<MultiTransportRemoteFacilityAction, sizeof...(DistinguishedFacilitySpecs)>
                , std::array<MultiTransportRemoteFacilityAction, sizeof...(NonDistinguishedFacilitySpecs)>
            >> output;
            std::lock_guard<std::mutex> _(mutex_);
            if (std::get<1>(data).index() == 0) {
                handleHeartbeat(*(std::get<0>(std::get<1>(data))), output);
            } else {
                handleTimer(std::get<0>(data), output);
            }
            return output;
        }
    };

} } } }

#endif