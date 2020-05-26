#ifndef TM_KIT_TRANSPORT_HEARTBEAT_MESSAGE_HPP_
#define TM_KIT_TRANSPORT_HEARTBEAT_MESSAGE_HPP_

#include <string>
#include <chrono>
#include <map>

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
        std::chrono::system_clock::time_point heartbeatTime_;
        std::string host_;
        int64_t pid_;
        std::string senderDescription_;
        std::map<std::string, OneItemStatus> details_;
    public:
        HeartbeatMessage() = default;
        HeartbeatMessage(std::chrono::system_clock::time_point heartbeatTime, std::string const &host, int64_t pid, std::string const &senderDescription, std::map<std::string,OneItemStatus> const &details=std::map<std::string,OneItemStatus>()) 
            : heartbeatTime_(heartbeatTime), host_(host), pid_(pid)
            , senderDescription_(senderDescription), details_(details)
        {
        }

        void SerializeToString(std::string *s) const;
        bool ParseFromString(std::string const &s);
    };
} } } }

#endif