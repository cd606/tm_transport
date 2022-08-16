#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>
#include <tm_kit/transport/BoostUUIDComponent.hpp>
#include <tm_kit/transport/HostNameUtil.hpp>
#include <tm_kit/infra/PidUtil.hpp>

#include <unordered_set>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    class HeartbeatAndAlertComponentImpl {
    private:
        std::string uuidStr_;
        basic::real_time_clock::ClockComponent *clock_;
        std::string host_;
        int64_t pid_;
        std::string identity_;
        std::optional<std::function<void(basic::ByteDataWithTopic &&)>> publisher_;
        std::mutex mutex_;
        std::unordered_map<std::string, std::vector<std::string>> broadcastChannels_;
        std::unordered_map<std::string, std::string> facilityChannels_;
        std::map<std::string, HeartbeatMessage::OneItemStatus> status_;
        std::vector<std::function<void(HeartbeatMessage &&)>> extraHandlers_;
    public:
        HeartbeatAndAlertComponentImpl() : uuidStr_(BoostUUIDComponent::id_to_string(BoostUUIDComponent::new_id())), clock_(nullptr), host_(), pid_(0), identity_(), publisher_(std::nullopt), mutex_(), broadcastChannels_(), facilityChannels_(), status_(), extraHandlers_() {}
        HeartbeatAndAlertComponentImpl(basic::real_time_clock::ClockComponent *clock, std::string const &identity) : uuidStr_(BoostUUIDComponent::id_to_string(BoostUUIDComponent::new_id())), clock_(clock), host_(hostname_util::hostname()), pid_(infra::pid_util::getpid()), identity_(identity), publisher_(std::nullopt), mutex_(), broadcastChannels_(), facilityChannels_(), status_(), extraHandlers_() {}
        HeartbeatAndAlertComponentImpl(basic::real_time_clock::ClockComponent *clock, std::string const &identity, std::function<void(basic::ByteDataWithTopic &&)> pub) : uuidStr_(BoostUUIDComponent::id_to_string(BoostUUIDComponent::new_id())), clock_(clock), host_(hostname_util::hostname()), pid_(infra::pid_util::getpid()), identity_(identity), publisher_(pub), mutex_(), broadcastChannels_(), facilityChannels_(), status_(), extraHandlers_() {}
        void assignIdentity(HeartbeatAndAlertComponentImpl &&another) {
            clock_ = std::move(another.clock_);
            host_ = std::move(another.host_);
            pid_ = std::move(another.pid_);
            identity_ = std::move(another.identity_);
            publisher_ = std::move(another.publisher_);
            extraHandlers_ = std::move(another.extraHandlers_);
        }
        void setStatus(std::string const &itemDescription, HeartbeatMessage::Status status, std::string const &info="") {
            std::lock_guard<std::mutex> _(mutex_);
            status_[itemDescription] = {status, info};
        }
        void addBroadcastChannel(std::string const &name, std::string const &c) {
            std::lock_guard<std::mutex> _(mutex_);
            auto &channelVec = broadcastChannels_[name];
            if (std::find(channelVec.begin(), channelVec.end(), c) == channelVec.end()) {
                channelVec.push_back(c);
            }
        }
        void addFacilityChannel(std::string const &name, std::string const &c) {
            std::lock_guard<std::mutex> _(mutex_);
            if (facilityChannels_.find(name) == facilityChannels_.end()) {
                facilityChannels_.insert({name, c});
            }
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
                    msg = HeartbeatMessage {
                        uuidStr_
                        , clock_->now()
                        , host_
                        , pid_
                        , identity_
                        , std::map<std::string, std::vector<std::string>> {broadcastChannels_.begin(), broadcastChannels_.end()}
                        , std::map<std::string, std::string> {facilityChannels_.begin(), facilityChannels_.end()}
                        , status_
                    };
                }
                std::string buf;
                msg.SerializeToString(&buf);
                (*publisher_)({heartbeatTopic, std::move(buf)});
                auto sz = extraHandlers_.size();
                for (std::size_t ii=0; ii<sz; ++ii) {
                    if (ii == sz-1) {
                        (extraHandlers_[ii])(std::move(msg));
                    } else {
                        (extraHandlers_[ii])(HeartbeatMessage {msg});
                    }
                }
            }
        }
        void addExtraHeartbeatHandler(std::function<void(HeartbeatMessage &&)> handler) {
            std::lock_guard<std::mutex> _(mutex_);
            extraHandlers_.push_back(handler);
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
    
    void HeartbeatAndAlertComponent::assignIdentity(HeartbeatAndAlertComponent &&another) {
        impl_->assignIdentity(std::move(*(another.impl_)));
    }
    void HeartbeatAndAlertComponent::addBroadcastChannel(std::string const &name, std::string const &channel) {
        impl_->addBroadcastChannel(name, channel);
    }
    void HeartbeatAndAlertComponent::addFacilityChannel(std::string const &name, std::string const &channel) {
        impl_->addFacilityChannel(name, channel);
    }
    void HeartbeatAndAlertComponent::setStatus(std::string const &itemDescription, HeartbeatMessage::Status status, std::string const &info) {
        impl_->setStatus(itemDescription, status, info);
    }
    void HeartbeatAndAlertComponent::sendAlert(std::string const &alertTopic, infra::LogLevel level, std::string const &message) {
        impl_->sendAlert(alertTopic, level, message);
    }
    void HeartbeatAndAlertComponent::publishHeartbeat(std::string const &heartbeatTopic) {
        impl_->publishHeartbeat(heartbeatTopic);
    }
    void HeartbeatAndAlertComponent::addExtraHeartbeatHandler(std::function<void(HeartbeatMessage &&)> handler) {
        impl_->addExtraHeartbeatHandler(handler);
    }

} } } }