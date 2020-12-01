#ifndef TM_KIT_TRANSPORT_HEARTBEAT_MESSAGE_HPP_
#define TM_KIT_TRANSPORT_HEARTBEAT_MESSAGE_HPP_

#include <string>
#include <chrono>
#include <map>
#include <unordered_set>
#include <regex>
#include <optional>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    class HeartbeatMessage {
    public:
        enum class Status {
            Good
            , Warning
            , Bad
            , Unknown
        };
        struct OneItemStatus {
            Status status;
            std::string info;
        };
        static std::string statusString(Status);
        static Status parseStatus(std::string const &);
    private:
        std::string uuidStr_;
        std::chrono::system_clock::time_point heartbeatTime_;
        std::string host_;
        int64_t pid_;
        std::string senderDescription_;
        std::map<std::string,std::vector<std::string>> broadcastChannels_;
        std::map<std::string,std::string> facilityChannels_;
        std::map<std::string, OneItemStatus> details_;
    public:
        HeartbeatMessage() = default;
        HeartbeatMessage(
            std::string uuidStr
            , std::chrono::system_clock::time_point heartbeatTime
            , std::string const &host
            , int64_t pid
            , std::string const &senderDescription
            , std::map<std::string,std::vector<std::string>> &&broadcastChannels 
            , std::map<std::string,std::string> &&facilityChannels
            , std::map<std::string,OneItemStatus> const &details=std::map<std::string,OneItemStatus>()
        ) 
            : uuidStr_(uuidStr), heartbeatTime_(heartbeatTime), host_(host), pid_(pid)
            , senderDescription_(senderDescription) 
            , broadcastChannels_(std::move(broadcastChannels))
            , facilityChannels_(std::move(facilityChannels))
            , details_(details)
        {
        }
        HeartbeatMessage(HeartbeatMessage const &) = default;
        HeartbeatMessage(HeartbeatMessage &&) = default;
        HeartbeatMessage &operator=(HeartbeatMessage const &) = default;
        HeartbeatMessage &operator=(HeartbeatMessage &&) = default;

        void SerializeToString(std::string *s) const;
        bool ParseFromString(std::string const &s);

        std::string const &uuidStr() const {
            return uuidStr_;
        }
        std::chrono::system_clock::time_point heartbeatTime() const {
            return heartbeatTime_;
        }
        std::string const &host() const {
            return host_;
        }
        int64_t pid() const {
            return pid_;
        }
        std::string const &senderDescription() const {
            return senderDescription_;
        }
        std::map<std::string, std::vector<std::string>> const &broadcastChannels() const {
            return broadcastChannels_;
        }
        std::map<std::string,std::string> const &facilityChannels() const {
            return facilityChannels_;
        }
        std::optional<HeartbeatMessage::OneItemStatus> status(std::string const &entry) const;
        std::unordered_set<std::string> allEntries() const;
        std::unordered_set<std::string> allEntriesRE(std::regex const &re) const;
    };

} } } }

#endif