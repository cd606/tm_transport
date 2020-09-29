#include <tm_kit/transport/HeartbeatMessage.hpp>
#include <tm_kit/infra/ChronoUtils.hpp>
#include <tm_kit/basic/ByteData.hpp>

#include <cstddef>
#include <cstring>
#include <sstream>

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace bytedata_utils {
    
    namespace {
        const std::array<std::string,4> STATUS_NAMES = {
            "Good", "Warning", "Bad", "Unknown"
        };
    }

    template <>
    struct RunCBORSerializer<transport::HeartbeatMessage::Status, void> {
        static std::string apply(transport::HeartbeatMessage::Status const &x) {
            return RunCBORSerializer<std::string>::apply(
                STATUS_NAMES[static_cast<int>(x)]
            );
        } 
        static std::size_t apply(transport::HeartbeatMessage::Status const &x, char *output) {
            return RunCBORSerializer<std::string>::apply(
                STATUS_NAMES[static_cast<int>(x)], output
            );
        }  
        static std::size_t calculateSize(transport::HeartbeatMessage::Status const &x) {
            return RunCBORSerializer<std::string>::calculateSize(
                STATUS_NAMES[static_cast<int>(x)]
            );
        }
    };
    template <>
    struct RunCBORDeserializer<transport::HeartbeatMessage::Status, void> {
        static std::optional<std::tuple<transport::HeartbeatMessage::Status,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<std::string>::apply(data, start);
            if (!t) {
                return std::nullopt;
            }
            size_t ii=0;
            bool good = false;
            for (ii=0; ii<STATUS_NAMES.size(); ++ii) {
                if (STATUS_NAMES[ii] == std::get<0>(*t)) {
                    good = true;
                    break;
                }
            }
            if (!good) {
                return std::nullopt;
            }
            return std::tuple<transport::HeartbeatMessage::Status,size_t> {
                static_cast<transport::HeartbeatMessage::Status>(ii)
                , std::get<1>(*t)
            };
        }
    };
    template <>
    struct RunCBORSerializer<transport::HeartbeatMessage::OneItemStatus, void> {
        static std::string apply(transport::HeartbeatMessage::OneItemStatus const &x) {
            std::string s;
            s.resize(calculateSize(x)); 
            apply(x, const_cast<char *>(s.data()));
            return s;
        }  
        static std::size_t apply(transport::HeartbeatMessage::OneItemStatus const &x, char *output) {
            std::tuple<transport::HeartbeatMessage::Status const *, std::string const *> t {
                &(x.status), &(x.info)
            };
            return RunCBORSerializerWithNameList<
                std::tuple<transport::HeartbeatMessage::Status const *, std::string const *>
                , 2
            >::apply(
                t
                , {
                    "status", "info"
                }
                , output
            );
        }
        static std::size_t calculateSize(transport::HeartbeatMessage::OneItemStatus const &x) {
            std::tuple<transport::HeartbeatMessage::Status const *, std::string const *> t {
                &(x.status), &(x.info)
            };
            return RunCBORSerializerWithNameList<
                std::tuple<transport::HeartbeatMessage::Status const *, std::string const *>
                , 2
            >::calculateSize(
                t
                , {
                    "status", "info"
                }
            );
        } 
    };
    template <>
    struct RunCBORDeserializer<transport::HeartbeatMessage::OneItemStatus, void> {
        static std::optional<std::tuple<transport::HeartbeatMessage::OneItemStatus,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializerWithNameList<
                std::tuple<transport::HeartbeatMessage::Status, std::string>
                , 2
            >::apply(
                data, start
                , {
                    "status", "info"
                }
            );
            if (t) {
                return std::tuple<transport::HeartbeatMessage::OneItemStatus,size_t> {
                    transport::HeartbeatMessage::OneItemStatus {
                        std::move(std::get<0>(std::get<0>(*t)))
                        , std::move(std::get<1>(std::get<0>(*t)))
                    }
                    , std::get<1>(*t)
                };
            } else {
                return std::nullopt;
            }
        }
    };
} } } } }

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
        int64_t t = infra::withtime_utils::sinceEpoch<std::chrono::microseconds>(heartbeatTime_);

        std::tuple<
            std::string const *
            , int64_t const *
            , std::string const *
            , int64_t const *
            , std::string const *
            , std::map<std::string, std::vector<std::string>> const *
            , std::map<std::string, std::string> const *
            , std::map<std::string, OneItemStatus> const *
        > x {
            &uuidStr_
            , &t 
            , &host_
            , &pid_
            , &senderDescription_
            , &broadcastChannels_
            , &facilityChannels_
            , &details_
        };
        *s = basic::bytedata_utils::RunCBORSerializerWithNameList<
            std::tuple<
                std::string const *
                , int64_t const *
                , std::string const *
                , int64_t const *
                , std::string const *
                , std::map<std::string, std::vector<std::string>> const *
                , std::map<std::string, std::string> const *
                , std::map<std::string, OneItemStatus> const *
            >
            , 8
        >::apply(
            x
            , {
                "uuid_str", "timestamp", "host", "pid", "sender_description"
                , "broadcast_channels", "facility_channels"
                , "details"
            }
        );
    }
    bool HeartbeatMessage::ParseFromString(std::string const &s) {
        auto t = basic::bytedata_utils::RunCBORDeserializerWithNameList<
            std::tuple<
                std::string
                , int64_t
                , std::string
                , int64_t
                , std::string
                , std::map<std::string, std::vector<std::string>>
                , std::map<std::string, std::string>
                , std::map<std::string, OneItemStatus>
            >
            , 8
        >::apply(
            std::string_view {s}, 0
            , {
                "uuid_str", "timestamp", "host", "pid", "sender_description"
                , "broadcast_channels", "facility_channels"
                , "details"
            }
        );
        if (!t) {
            return false;
        }
        if (std::get<1>(*t) != s.length()) {
            return false;
        }
        auto const &x = std::get<0>(*t);
        uuidStr_ = std::get<0>(x);
        heartbeatTime_ = infra::withtime_utils::epochDurationToTime<std::chrono::microseconds>(std::get<1>(x));
        host_ = std::get<2>(x);
        pid_ = std::get<3>(x);
        senderDescription_ = std::get<4>(x);
        broadcastChannels_ = std::get<5>(x);
        facilityChannels_ = std::get<6>(x);
        details_ = std::get<7>(x);
        return true; 
    }
    std::optional<HeartbeatMessage::OneItemStatus> HeartbeatMessage::status(std::string const &entry) const {
        auto iter = details_.find(entry);
        if (iter == details_.end()) {
            return std::nullopt;
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