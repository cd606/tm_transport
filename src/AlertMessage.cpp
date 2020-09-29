#include <tm_kit/transport/AlertMessage.hpp>
#include <tm_kit/infra/ChronoUtils.hpp>
#include <tm_kit/basic/ByteData.hpp>

#include <cstddef>
#include <cstring>
#include <sstream>

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
        std::tuple<
            int64_t
            , std::string const *
            , int64_t const *
            , std::string const *
            , std::string
            , std::string const *
        > x {
            infra::withtime_utils::sinceEpoch<std::chrono::microseconds>(alertTime_)
            , &host_
            , &pid_
            , &senderDescription_
            , infra::logLevelToString(level_)
            , &message_
        };
        *s = basic::bytedata_utils::RunCBORSerializerWithNameList<
            std::tuple<
                int64_t
                , std::string const *
                , int64_t const *
                , std::string const *
                , std::string
                , std::string const *
            >
            , 6
        >::apply(
            x
            , {
                "alertTime", "host", "pid"
                , "sender", "level", "message"
            }
        );
    }
    bool AlertMessage::ParseFromString(std::string const &s) {
        auto t = basic::bytedata_utils::RunCBORDeserializerWithNameList<
            std::tuple<
                int64_t
                , std::string
                , int64_t
                , std::string
                , std::string
                , std::string
            >
            , 6
        >::apply(
            std::string_view {s}, 0
            , {
                "alertTime", "host", "pid"
                , "sender", "level", "message"
            }
        );
        if (!t) {
            return false;
        }
        if (std::get<1>(*t) != s.length()) {
            return false;
        }
        auto const &x = std::get<0>(*t);
        alertTime_ = infra::withtime_utils::epochDurationToTime<std::chrono::microseconds>(std::get<0>(x));
        host_ = std::get<1>(x);
        pid_ = std::get<2>(x);
        senderDescription_ = std::get<3>(x);
        level_ = parseLevel(std::get<4>(x));
        message_ = std::get<5>(x);
        return true;
    }
} } } }