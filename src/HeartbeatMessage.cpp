#include <tm_kit/transport/HeartbeatMessage.hpp>
#include <tm_kit/infra/ChronoUtils.hpp>

#include <cstddef>
#include <cstring>
#include <sstream>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

using boost::property_tree::ptree;
using boost::property_tree::read_json;
using boost::property_tree::write_json;

namespace dev { namespace cd606 { namespace tm { namespace transport {
    std::string HeartbeatMessage::statusString(HeartbeatMessage::Status status) {
        switch (status) {
        case HeartbeatMessage::Status::Good:
            return "Good";
        case HeartbeatMessage::Status::Warning:
            return "Warning";
        case HeartbeatMessage::Status::Bad:
            return "Bad";
        default:
            return "Unknown";
        }
    }
    HeartbeatMessage::Status HeartbeatMessage::parseStatus(std::string const &statusStr) {
        if (statusStr == "Good") {
            return HeartbeatMessage::Status::Good;
        }
        if (statusStr == "Warning") {
            return HeartbeatMessage::Status::Warning;
        }
        if (statusStr == "Bad") {
            return HeartbeatMessage::Status::Bad;
        }
        return HeartbeatMessage::Status::Unknown;
    }
    void HeartbeatMessage::SerializeToString(std::string *s) const {
        ptree pt;
        pt.put("heartbeatTime", infra::withtime_utils::sinceEpoch<std::chrono::microseconds>(heartbeatTime_));
        pt.put("host", host_);
        pt.put("pid", pid_);
        pt.put("sender", senderDescription_);
        for (auto const &item : details_) {
            pt.put(std::string("details.")+item.first+".status", statusString(item.second.status));
            pt.put(std::string("details.")+item.first+".info", item.second.info);
        }
        std::ostringstream oss;
        write_json(oss, pt, false);
        *s = oss.str();
    }
    bool HeartbeatMessage::ParseFromString(std::string const &s) {
        HeartbeatMessage msg;
        try {
            ptree pt;
            std::istringstream iss (s);
            read_json(iss, pt);
            msg.heartbeatTime_ = infra::withtime_utils::epochDurationToTime<std::chrono::microseconds>(pt.get<int64_t>("heartbeatTime"));
            msg.host_ = pt.get<std::string>("host");
            msg.pid_ = pt.get<int64_t>("pid");
            msg.senderDescription_ = pt.get<std::string>("sender");
            for (auto const &item : pt.get_child("details")) {
                auto status = parseStatus(item.second.get<std::string>("status"));
                auto info = item.second.get<std::string>("info");
                msg.details_.insert({item.first, {status, info}});
            }
            *this = msg;
            return true;
        } catch (...) {
            return false;
        }  
    }
    HeartbeatMessage::OneItemStatus const &HeartbeatMessage::status(std::string const &entry) const {
        static const HeartbeatMessage::OneItemStatus EMPTY_STATUS {Status::Unknown, ""};
        auto iter = details_.find(entry);
        if (iter == details_.end()) {
            return EMPTY_STATUS;
        } 
        return iter->second;
    }
    std::unordered_set<std::string> HeartbeatMessage::allEntries() const {
        std::unordered_set<std::string> ret;
        for (auto const &item : details_) {
            ret.insert(item.first);
        }
        return ret;
    }
    std::unordered_set<std::string> HeartbeatMessage::allEntriesRE(std::regex const &re) const {
        std::unordered_set<std::string> ret;
        for (auto const &item : details_) {
            if (std::regex_match(item.first, re)) {
                ret.insert(item.first);
            }
        }
        return ret;
    }
} } } }