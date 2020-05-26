#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>

#ifdef _MSC_VER
#include <windows.h>
#include <processthreadsapi.h>
#include <winsock.h>
#else
#include <sys/types.h>
#include <unistd.h>
#endif

namespace dev { namespace cd606 { namespace tm { namespace transport {
    class HeartbeatAndAlertComponentImpl {
    private:
        basic::real_time_clock::ClockComponent *clock_;
        std::string host_;
        int64_t pid_;
        std::string identity_;
        std::optional<std::function<void(basic::ByteDataWithTopic &&)>> publisher_;
        std::mutex mutex_;
        std::map<std::string, HeartbeatMessage::OneItemStatus> status_;
        static std::string getHost() {
            char buf[1024];
            if (gethostname(buf, 1024) == 0) {
                return buf;
            } else {
                return "";
            }
        }
        static int64_t getPid() {
            #ifdef _MSC_VER
                return (int64_t) GetCurrentProcessId();
            #else
                return (int64_t) getpid();
            #endif
        }
    public:
        HeartbeatAndAlertComponentImpl() : clock_(nullptr), host_(), pid_(0), identity_(), publisher_(std::nullopt), mutex_(), status_() {}
        HeartbeatAndAlertComponentImpl(basic::real_time_clock::ClockComponent *clock, std::string const &identity) : clock_(clock), host_(getHost()), pid_(getPid()), identity_(identity), publisher_(std::nullopt), mutex_(), status_() {}
        HeartbeatAndAlertComponentImpl(basic::real_time_clock::ClockComponent *clock, std::string const &identity, std::function<void(basic::ByteDataWithTopic &&)> pub) : clock_(clock), host_(getHost()), pid_(getPid()), identity_(identity), publisher_(pub), mutex_(), status_() {}
        void setStatus(std::string const &itemDescription, HeartbeatMessage::Status status, std::string const &info="") {
            std::lock_guard<std::mutex> _(mutex_);
            status_[itemDescription] = {status, info};
        }
        void sendAlert(std::string const &alertTopic, infra::LogLevel level, std::string const &message) {
            if (publisher_ && clock_) {
                AlertMessage msg {clock_->now(), host_, pid_, identity_, level, message};
                std::string buf;
                msg.SerializeToString(&buf);
                (*publisher_)({alertTopic, std::move(buf)});
            }
        }
        void publishHeartbeat(std::string const &heartbeatTopic) {
            if (publisher_ && clock_) {
                HeartbeatMessage msg;
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    msg = HeartbeatMessage {clock_->now(), host_, pid_, identity_, status_};
                }
                std::string buf;
                msg.SerializeToString(&buf);
                (*publisher_)({heartbeatTopic, std::move(buf)});
            }
        }
    };

    HeartbeatAndAlertComponent::HeartbeatAndAlertComponent() : impl_(std::make_unique<HeartbeatAndAlertComponentImpl>()) {}
    HeartbeatAndAlertComponent::HeartbeatAndAlertComponent(basic::real_time_clock::ClockComponent *clock, std::string const &identity)
        : impl_(std::make_unique<HeartbeatAndAlertComponentImpl>(clock, identity))
    {}
    HeartbeatAndAlertComponent::HeartbeatAndAlertComponent(basic::real_time_clock::ClockComponent *clock, std::string const &identity, std::function<void(basic::ByteDataWithTopic &&)> pub)
        : impl_(std::make_unique<HeartbeatAndAlertComponentImpl>(clock, identity, pub))
    {}
    HeartbeatAndAlertComponent::~HeartbeatAndAlertComponent() {}
    HeartbeatAndAlertComponent::HeartbeatAndAlertComponent(HeartbeatAndAlertComponent &&) = default;
    HeartbeatAndAlertComponent &HeartbeatAndAlertComponent::operator=(HeartbeatAndAlertComponent &&) = default;
    
    void HeartbeatAndAlertComponent::setStatus(std::string const &itemDescription, HeartbeatMessage::Status status, std::string const &info) {
        impl_->setStatus(itemDescription, status, info);
    }
    void HeartbeatAndAlertComponent::sendAlert(std::string const &alertTopic, infra::LogLevel level, std::string const &message) {
        impl_->sendAlert(alertTopic, level, message);
    }
    void HeartbeatAndAlertComponent::publishHeartbeat(std::string const &heartbeatTopic) {
        impl_->publishHeartbeat(heartbeatTopic);
    }

} } } }