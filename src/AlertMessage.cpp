#include <tm_kit/transport/AlertMessage.hpp>
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
    namespace {
        inline infra::LogLevel parseLevel(std::string const &levelStr) {
            if (levelStr == infra::logLevelToString(infra::LogLevel::Trace)) {
                return infra::LogLevel::Trace;
            } else if (levelStr == infra::logLevelToString(infra::LogLevel::Debug)) {
                return infra::LogLevel::Debug;
            } else if (levelStr == infra::logLevelToString(infra::LogLevel::Info)) {
                return infra::LogLevel::Info;
            } else if (levelStr == infra::logLevelToString(infra::LogLevel::Warning)) {
                return infra::LogLevel::Warning;
            } else if (levelStr == infra::logLevelToString(infra::LogLevel::Error)) {
                return infra::LogLevel::Error;
            } else if (levelStr == infra::logLevelToString(infra::LogLevel::Critical)) {
                return infra::LogLevel::Critical;
            } else {
                return infra::LogLevel::Trace;
            }
        }
    }
    void AlertMessage::SerializeToString(std::string *s) const {
        ptree pt;
        pt.put("alertTime", infra::withtime_utils::sinceEpoch<std::chrono::microseconds>(alertTime_));
        pt.put("host", host_);
        pt.put("pid", pid_);
        pt.put("sender", senderDescription_);
        pt.put("level", infra::logLevelToString(level_));
        pt.put("message", message_);
        std::ostringstream oss;
        write_json(oss, pt, false);
        *s = oss.str();
    }
    bool AlertMessage::ParseFromString(std::string const &s) {
        AlertMessage msg;
        try {
            ptree pt;
            std::istringstream iss (s);
            read_json(iss, pt);
            msg.alertTime_ = infra::withtime_utils::epochDurationToTime<std::chrono::microseconds>(pt.get<int64_t>("alertTime"));
            msg.host_ = pt.get<std::string>("host");
            msg.pid_ = pt.get<int64_t>("pid");
            msg.senderDescription_ = pt.get<std::string>("sender");
            msg.level_ = parseLevel(pt.get<std::string>("level"));
            msg.message_ = pt.get<std::string>("message");
            *this = msg;
            return true;
        } catch (...) {
            return false;
        }  
    }
} } } }