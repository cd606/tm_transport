#ifndef TM_KIT_TRANSPORT_ALERT_MESSAGE_HPP_
#define TM_KIT_TRANSPORT_ALERT_MESSAGE_HPP_

#include <string>
#include <chrono>

#include <tm_kit/infra/LogLevel.hpp>
#include <tm_kit/basic/SerializationHelperMacros.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    class AlertMessage {
    private:
        std::chrono::system_clock::time_point alertTime_;
        std::string host_;
        int64_t pid_;
        std::string senderDescription_;
        infra::LogLevel level_;
        std::string message_;
    public:
        AlertMessage() = default;
        AlertMessage(std::chrono::system_clock::time_point alertTime, std::string const &host, int64_t pid, std::string const &senderDescription, infra::LogLevel level, std::string const &message) 
            : alertTime_(alertTime), host_(host), pid_(pid)
            , senderDescription_(senderDescription), level_(level), message_(message)
        {
        }

        void SerializeToString(std::string *s) const;
        bool ParseFromString(std::string const &s);
        std::chrono::system_clock::time_point alertTime() const {return alertTime_;}
        std::string const &host() const {return host_;}
        int64_t pid() const {return pid_;}
        std::string const &senderDescription() const {return senderDescription_;}
        infra::LogLevel level() const {return level_;}
        std::string const &message() const {return message_;}
    };

#define CUSTOM_DATA_MESSAGE_THROUGH_ALERT_CHANNEL_FIELDS \
    ((std::chrono::system_clock::time_point, messageTime)) \
    ((std::string, host)) \
    ((int64_t, pid)) \
    ((std::string, senderDescription)) \
    ((T, data))

    TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT(((typename, T)), CustomDataMessageThroughAlertChannel, CUSTOM_DATA_MESSAGE_THROUGH_ALERT_CHANNEL_FIELDS);
} } } }

TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT_SERIALIZE(((typename, T)), dev::cd606::tm::transport::CustomDataMessageThroughAlertChannel, CUSTOM_DATA_MESSAGE_THROUGH_ALERT_CHANNEL_FIELDS);

#endif
