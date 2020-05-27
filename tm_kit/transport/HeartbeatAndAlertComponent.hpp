#ifndef TM_KIT_TRANSPORT_HEARTBEAT_AND_ALERT_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_HEARTBEAT_AND_ALERT_COMPONENT_HPP_

#include <map>
#include <functional>
#include <optional>
#include <mutex>
#include <memory>
#include <type_traits>

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/VoidStruct.hpp>
#include <tm_kit/basic/real_time_clock/ClockComponent.hpp>
#include <tm_kit/basic/real_time_clock/ClockImporter.hpp>
#include <tm_kit/transport/HeartbeatMessage.hpp>
#include <tm_kit/transport/AlertMessage.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>
#include <tm_kit/transport/multicast/MulticastComponent.hpp>
#include <tm_kit/transport/zeromq/ZeroMQComponent.hpp>
#include <tm_kit/transport/redis/RedisComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    class HeartbeatAndAlertComponentImpl;

    class HeartbeatAndAlertComponent {
    private:
        std::unique_ptr<HeartbeatAndAlertComponentImpl> impl_;
    public:
        HeartbeatAndAlertComponent();
        HeartbeatAndAlertComponent(basic::real_time_clock::ClockComponent *clock, std::string const &identity);
        HeartbeatAndAlertComponent(basic::real_time_clock::ClockComponent *clock, std::string const &identity, std::function<void(basic::ByteDataWithTopic &&)> pub);
        ~HeartbeatAndAlertComponent();
        HeartbeatAndAlertComponent(HeartbeatAndAlertComponent &&);
        HeartbeatAndAlertComponent &operator=(HeartbeatAndAlertComponent &&);
        void setStatus(std::string const &itemDescription, HeartbeatMessage::Status status, std::string const &info="");
        void sendAlert(std::string const &alertTopic, infra::LogLevel level, std::string const &message);
        void publishHeartbeat(std::string const &heartbeatTopic);
    };

    template <class Env, class TransportComponent>
    class HeartbeatAndAlertComponentInitializer {
    public:
        void operator()(Env *env, std::string const &identity, ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt) {
            env->HeartbeatAndAlertComponent::operator=(HeartbeatAndAlertComponent {
                static_cast<basic::real_time_clock::ClockComponent *>(env)
                , identity
            });
        }
    };
    template <class Env>
    class HeartbeatAndAlertComponentInitializer<Env, rabbitmq::RabbitMQComponent> {
    public:
        void operator()(Env *env, std::string const &identity, ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt) {
            env->HeartbeatAndAlertComponent::operator=(HeartbeatAndAlertComponent {
                static_cast<basic::real_time_clock::ClockComponent *>(env)
                , identity
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
    class HeartbeatAndAlertComponentInitializer<Env, multicast::MulticastComponent> {
    public:
        void operator()(Env *env, std::string const &identity, ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt) {
            env->HeartbeatAndAlertComponent::operator=(HeartbeatAndAlertComponent {
                static_cast<basic::real_time_clock::ClockComponent *>(env)
                , identity
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
    class HeartbeatAndAlertComponentInitializer<Env, zeromq::ZeroMQComponent> {
    public:
        //It is a known issue that even if a ZMQ SUB client is started first and then
        //ZMQ PUB server is started, the SUB client may still lose the first few messages
        //from the PUB server. See e.g. https://stackoverflow.com/questions/45740168/zeromq-cppzmq-subscriber-skips-first-message
        //
        //It is quite likely that a program that employs HeartbeatAndAlertComponent
        //will want to send some alert messages at the beginning to indicate correct start,
        //and to lose these messages is not satisfactory. Therefore, the current implementation
        //does not allow using ZMQ as the transport for HeartbeatAndAlertComponent.
        void operator()(Env *env, std::string const &identity, ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt) {
            env->log(infra::LogLevel::Error, "[HeartbeatAndAlertComponentInitializer] You are trying to use ZeroMQ transport to send heartbeat and alert messages. Due to a known issue where the first few messages sent on this transport are likely to get lost, this transport is not suitable for this kind of function, and therefore this is currently disabled.");
            throw std::runtime_error("HeartbeatAndAlertComponentInitializer on ZeroMQ transport is currently disabled");
        }
    };
    template <class Env>
    class HeartbeatAndAlertComponentInitializer<Env, redis::RedisComponent> {
    public:
        void operator()(Env *env, std::string const &identity, ConnectionLocator const &locator, std::optional<UserToWireHook> hook=std::nullopt) {
            env->HeartbeatAndAlertComponent::operator=(HeartbeatAndAlertComponent {
                static_cast<basic::real_time_clock::ClockComponent *>(env)
                , identity
                , static_cast<redis::RedisComponent *>(env)
                    ->redis_getPublisher(locator, hook)
            });
        }
    };

    template <class R
        , std::enable_if_t<
            std::is_base_of_v<HeartbeatAndAlertComponent, typename R::EnvironmentType>
            && std::is_base_of_v<basic::real_time_clock::ClockComponent, typename R::EnvironmentType>
            , int> = 0
    >
    void attachHeartbeatAndAlertComponent(R &r, typename R::EnvironmentType *env, std::string const &heartbeatTopic, std::chrono::system_clock::duration period, std::chrono::system_clock::duration furthestPoint=std::chrono::hours(24)) {
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
            [heartbeatTopic](typename R::MonadType::template InnerData<basic::VoidStruct> &&d) {
                static_cast<HeartbeatAndAlertComponent *>(d.environment)->publishHeartbeat(heartbeatTopic);
            }
        );
        r.registerExporter("__heartbeat_run_heartbeat", runHeartbeat);
        r.exportItem(runHeartbeat, r.importItem(importer));
    }

} } } }

#endif