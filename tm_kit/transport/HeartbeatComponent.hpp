#ifndef TM_KIT_TRANSPORT_HEARTBEAT_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_HEARTBEAT_COMPONENT_HPP_

#include <map>
#include <functional>
#include <optional>
#include <mutex>
#include <type_traits>

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/VoidStruct.hpp>
#include <tm_kit/basic/real_time_clock/ClockComponent.hpp>
#include <tm_kit/basic/real_time_clock/ClockImporter.hpp>
#include <tm_kit/transport/HeartbeatMessage.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>
#include <tm_kit/transport/multicast/MulticastComponent.hpp>
#include <tm_kit/transport/zeromq/ZeroMQComponent.hpp>
#include <tm_kit/transport/redis/RedisComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    class HeartbeatComponent {
    private:
        std::string host_;
        int64_t pid_;
        std::string identity_;
        std::optional<std::function<void(basic::ByteDataWithTopic &&)>> publisher_;
        std::mutex mutex_;
        std::map<std::string, HeartbeatMessage::OneItemStatus> status_;
        static std::string getHost();
        static int64_t getPid();
    public:
        HeartbeatComponent() : host_(), pid_(0), identity_(), publisher_(std::nullopt), mutex_(), status_() {}
        HeartbeatComponent(std::string const &identity) : host_(getHost()), pid_(getPid()), identity_(identity), publisher_(std::nullopt), mutex_(), status_() {}
        HeartbeatComponent(std::string const &identity, std::function<void(basic::ByteDataWithTopic &&)> pub) : host_(getHost()), pid_(getPid()), identity_(identity), publisher_(pub), mutex_(), status_() {}
        HeartbeatComponent(HeartbeatComponent const &) = delete;
        HeartbeatComponent &operator=(HeartbeatComponent const &) = delete;
        HeartbeatComponent(HeartbeatComponent &&c) 
            : host_(std::move(c.host_)), pid_(std::move(c.pid_)), identity_(std::move(c.identity_))
            , publisher_(std::move(c.publisher_)), mutex_(), status_(std::move(c.status_)) {} 
        HeartbeatComponent &operator=(HeartbeatComponent &&c) {
            if (this == &c) {
                return *this;
            }
            host_ = std::move(c.host_);
            pid_ = std::move(c.pid_);
            identity_ = std::move(c.identity_);
            publisher_ = std::move(c.publisher_);
            status_ = std::move(c.status_);
            return *this;
        }
        void setStatus(std::string const &itemDescription, HeartbeatMessage::Status status, std::string const &info="") {
            std::lock_guard<std::mutex> _(mutex_);
            status_[itemDescription] = {status, info};
        }
        template <class Env>
        void publish(Env *env, std::string const &topic) {
            if (publisher_) {
                HeartbeatMessage msg;
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    msg = HeartbeatMessage::create(env, host_, pid_, identity_, status_);
                }
                std::string buf;
                msg.SerializeToString(&buf);
                (*publisher_)({topic, std::move(buf)});
            }
        }
    };

    template <class Env, class TransportComponent>
    class HeartbeatComponentInitializer {
    public:
        void operator()(Env *env, std::string const &identity, ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt) {
            env->HeartbeatComponent::operator=(HeartbeatComponent {identity});
        }
    };
    template <class Env>
    class HeartbeatComponentInitializer<Env, rabbitmq::RabbitMQComponent> {
    public:
        void operator()(Env *env, std::string const &identity, ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt) {
            env->HeartbeatComponent::operator=(HeartbeatComponent {
                identity
                , std::bind(
                    static_cast<rabbitmq::RabbitMQComponent *>(env)
                        ->rabbitmq_getExchangePublisher(locator, hook)
                    , std::string("")
                    , std::placeholders::_1
                )
            });
        }
    };
    template <class Env>
    class HeartbeatComponentInitializer<Env, multicast::MulticastComponent> {
    public:
        void operator()(Env *env, std::string const &identity, ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt) {
            env->HeartbeatComponent::operator=(HeartbeatComponent {
                identity
                , std::bind(
                    static_cast<multicast::MulticastComponent *>(env)
                        ->multicast_getPublisher(locator, hook)
                    , std::placeholders::_1
                    , 1
                )
            });
        }
    };
    template <class Env>
    class HeartbeatComponentInitializer<Env, zeromq::ZeroMQComponent> {
    public:
        void operator()(Env *env, std::string const &identity, ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt) {
            env->HeartbeatComponent::operator=(HeartbeatComponent {
                identity
                , static_cast<zeromq::ZeroMQComponent *>(env)
                    ->zeroMQ_getPublisher(locator, hook)
            });
        }
    };
    template <class Env>
    class HeartbeatComponentInitializer<Env, redis::RedisComponent> {
    public:
        void operator()(Env *env, std::string const &identity, ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt) {
            env->HeartbeatComponent::operator=(HeartbeatComponent {
                identity
                , static_cast<redis::RedisComponent *>(env)
                    ->redis_getPublisher(locator, hook)
            });
        }
    };

    template <class R
        , std::enable_if_t<
            std::is_base_of_v<HeartbeatComponent, typename R::EnvironmentType>
            && std::is_base_of_v<basic::real_time_clock::ClockComponent, typename R::EnvironmentType>
            , int> = 0
    >
    void attachHeartbeatComponent(R &r, typename R::EnvironmentType *env, std::string const &topic, std::chrono::system_clock::duration period, std::chrono::system_clock::duration furthestPoint=std::chrono::hours(24)) {
        auto nowTp = env->now();
        auto importer = basic::real_time_clock::ClockImporter<typename R::EnvironmentType>
            ::template createRecurringClockConstImporter<basic::VoidStruct>(
            nowTp
            , nowTp+furthestPoint
            , period
            , basic::VoidStruct {}
        );
        r.registerImporter("__heartbeat_clock_importer", importer);
        auto runHeartbeat = R::MonadType::template simpleExporter<basic::VoidStruct>(
            [topic](typename R::MonadType::template InnerData<basic::VoidStruct> &&d) {
                static_cast<HeartbeatComponent *>(d.environment)->publish<typename R::EnvironmentType>(d.environment, topic);
            }
        );
        r.registerExporter("__heartbeat_run_heartbeat", runHeartbeat);
        r.exportItem(runHeartbeat, r.importItem(importer));
    }

} } } }

#endif