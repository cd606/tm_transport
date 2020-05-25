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
        template <class Env>
        static HeartbeatMessage create(Env *env, std::string const &host, int64_t pid, std::string const &senderDescription, std::map<std::string,OneItemStatus> const &details=std::map<std::string,OneItemStatus>()) {
            HeartbeatMessage msg;
            msg.heartbeatTime_ = env->now();
            msg.host_ = host;
            msg.pid_ = pid;
            msg.senderDescription_ = senderDescription;
            msg.details_ = details;
            return msg;
        }

        void SerializeToString(std::string *s) const;
        bool ParseFromString(std::string const &s);
    };
} } } }

#endif