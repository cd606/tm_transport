#ifndef TM_KIT_TRANSPORT_HEARTBEAT_MESSAGE_HPP_
#define TM_KIT_TRANSPORT_HEARTBEAT_MESSAGE_HPP_

#include <string>
#include <chrono>
#include <map>
#include <unordered_set>
#include <regex>

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
        //In the current implementation, facility channels must
        //have names (i.e. the registered name in the node graph)
        //but broadcast channels don't. There are two reasons for
        //this. First, facility channels info will be used by clients
        //to do auto subscription-desubscription, and the clients need
        //to have a way to know which channel is which type. The clients
        //can supply the types, but the mapping can only be done by matching
        //on the name, since the channel spec is usually not very informational.
        //Second, facility channels are registered during the wrapping
        //of facilities, where we have registered names for nodes, but broadcast
        //channels, in the current implementation, are registered in start() call
        //where we don't have registered names for nodes. It is of course possible
        //to add names to broadcast channels if needed, but for now there does
        //not seem to be the need.
        std::vector<std::string> broadcastChannels_;
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
            , std::vector<std::string> const &broadcastChannels 
            , std::map<std::string,std::string> const &facilityChannels
            , std::map<std::string,OneItemStatus> const &details=std::map<std::string,OneItemStatus>()
        ) 
            : uuidStr_(uuidStr), heartbeatTime_(heartbeatTime), host_(host), pid_(pid)
            , senderDescription_(senderDescription) 
            , broadcastChannels_(broadcastChannels)
            , facilityChannels_(facilityChannels)
            , details_(details)
        {
        }

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
        std::vector<std::string> const &broadcastChannels() const {
            return broadcastChannels_;
        }
        std::map<std::string,std::string> const &facilityChannels() const {
            return facilityChannels_;
        }
        HeartbeatMessage::OneItemStatus const &status(std::string const &entry) const;
        std::unordered_set<std::string> allEntries() const;
        std::unordered_set<std::string> allEntriesRE(std::regex const &re) const;
    };

} } } }

#endif