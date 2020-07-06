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

        std::string const &host() const {
            return host_;
        }
        int64_t pid() const {
            return pid_;
        }
        std::string const &senderDescription() const {
            return senderDescription_;
        }
        HeartbeatMessage::OneItemStatus const &status(std::string const &entry) const;
        std::unordered_set<std::string> allEntries() const;
        std::unordered_set<std::string> allEntriesRE(std::regex const &re) const;
    };

    /*
    template <class R, class TriggeringT=basic::VoidStruct>
    inline typename R::Source<HeartbeatMessage> triggerDedicatedHeartbeatListener(
        R &r 
        , R::Source<TriggeringT> &&triggeringSource
        , MultiTransportBroadcastListenerAddSubscription const &listenerSpec
        , std::string const &listenerName
    ) {

        auto createHeartbeatListenKey = M::simpleImporter<M::Key<transport::MultiTransportBroadcastListenerInput>>(
        [](M::PublisherCall<M::Key<transport::MultiTransportBroadcastListenerInput>> &pub) {
            pub(infra::withtime_utils::keyify<transport::MultiTransportBroadcastListenerInput,TheEnvironment>(
                transport::MultiTransportBroadcastListenerInput { {
                    transport::MultiTransportBroadcastListenerAddSubscription {
                        transport::MultiTransportBroadcastListenerConnectionType::Redis
                        , "127.0.0.1:6379"
                        , "heartbeats.transaction_test_server"
                    }
                } }
            ));
        }
    );
    }*/
} } } }

#endif