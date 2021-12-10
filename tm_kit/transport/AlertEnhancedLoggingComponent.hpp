#ifndef TM_KIT_TRANSPORT_ALERT_ENHANCED_LOGGING_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_ALERT_ENHANCED_LOGGING_COMPONENT_HPP_

#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>
#include <tm_kit/basic/LoggingComponentBase.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class BaseLoggingComponent>
    class AlertEnhancedLoggingComponent : public virtual BaseLoggingComponent {
    public:
        AlertEnhancedLoggingComponent() : BaseLoggingComponent() {}
        virtual ~AlertEnhancedLoggingComponent() {}
        void log(infra::LogLevel l, std::string const &s) {
            if (static_cast<int>(l) >= static_cast<int>(infra::LogLevel::Warning)) {
                auto *alert = dynamic_cast<HeartbeatAndAlertComponent *>(this);              
                if (alert != nullptr) {
                    alert->sendAlert("AlertEnhancedLogging", l, s);
                }
            }
            BaseLoggingComponent::log(l, s);
        }
        virtual void logThroughLoggingComponentBase(infra::LogLevel l, std::string const &s) override {
            log(l, s);
        }
    };
} } } }

#endif