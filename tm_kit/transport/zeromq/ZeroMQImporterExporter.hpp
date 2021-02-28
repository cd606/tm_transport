#ifndef TM_KIT_TRANSPORT_ZEROMQ_ZEROMQ_IMPORTER_EXPORTER_HPP_
#define TM_KIT_TRANSPORT_ZEROMQ_ZEROMQ_IMPORTER_EXPORTER_HPP_

#include <type_traits>

#include <boost/lexical_cast.hpp>

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/TraceNodesComponent.hpp>
#include <tm_kit/infra/ControllableNode.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/zeromq/ZeroMQComponent.hpp>
#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>
#include <tm_kit/transport/AbstractHookFactoryComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace zeromq {

    template <class Env, std::enable_if_t<std::is_base_of_v<ZeroMQComponent, Env>, int> = 0>
    class ZeroMQImporterExporter {
    public:
        using M = infra::RealTimeApp<Env>;
        static std::shared_ptr<typename M::template Importer<basic::ByteDataWithTopic>> createImporter(ConnectionLocator const &locator, std::variant<ZeroMQComponent::NoTopicSelection, std::string, std::regex> const &topic=ZeroMQComponent::NoTopicSelection(), std::optional<WireToUserHook> wireToUserHook=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::ByteDataWithTopic>, public infra::IControllableNode<Env> {
            private:
                ConnectionLocator locator_;
                std::variant<ZeroMQComponent::NoTopicSelection, std::string, std::regex> topic_;
                std::optional<WireToUserHook> wireToUserHook_;
                std::optional<uint32_t> client_;
            public:
                LocalI(ConnectionLocator const &locator, std::variant<ZeroMQComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<WireToUserHook> wireToUserHook)
                    : locator_(locator), topic_(topic), wireToUserHook_(wireToUserHook), client_(std::nullopt)
                {
                }
                virtual void start(Env *env) override final {
                    client_ = env->zeroMQ_addSubscriptionClient(
                        locator_
                        , topic_
                        , [this,env](basic::ByteDataWithTopic &&d) {
                            TM_INFRA_IMPORTER_TRACER(env);
                            this->publish(M::template pureInnerData<basic::ByteDataWithTopic>(env, std::move(d)));
                        }
                        , wireToUserHook_
                    );
                }
                virtual void control(Env *env, std::string const &command, std::vector<std::string> const &params) override final {
                    if (command == "stop") {
                        if (client_) {
                            env->zeroMQ_removeSubscriptionClient(*client_);
                        }
                    }
                }
            };
            return M::importer(new LocalI(locator, topic, wireToUserHook));
        }
        template <class T>
        static std::shared_ptr<typename M::template Importer<basic::TypedDataWithTopic<T>>> createTypedImporter(ConnectionLocator const &locator, std::variant<ZeroMQComponent::NoTopicSelection, std::string, std::regex> const &topic=ZeroMQComponent::NoTopicSelection(), std::optional<WireToUserHook> wireToUserHook=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::TypedDataWithTopic<T>>, public infra::IControllableNode<Env> {
            private:
                ConnectionLocator locator_;
                std::variant<ZeroMQComponent::NoTopicSelection, std::string, std::regex> topic_;
                std::optional<WireToUserHook> wireToUserHook_;
                std::optional<uint32_t> client_;
            public:
                LocalI(ConnectionLocator const &locator, std::variant<ZeroMQComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<WireToUserHook> wireToUserHook)
                    : locator_(locator), topic_(topic), wireToUserHook_(wireToUserHook), client_(std::nullopt)
                {
                }
                virtual void start(Env *env) override final {
                    if (!wireToUserHook_) {
                        wireToUserHook_ = DefaultHookFactory<Env>::template incomingHook<T>(env);
                    }
                    client_ = env->zeroMQ_addSubscriptionClient(
                       locator_
                        , topic_
                        , [this,env](basic::ByteDataWithTopic &&d) {
                            TM_INFRA_IMPORTER_TRACER(env);
                            auto t = basic::bytedata_utils::RunDeserializer<T>::apply(d.content);
                            if (t) {
                                this->publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(*t)}));
                            }
                        }
                        , wireToUserHook_
                    );
                }
                virtual void control(Env *env, std::string const &command, std::vector<std::string> const &params) override final {
                    if (command == "stop") {
                        if (client_) {
                            env->zeroMQ_removeSubscriptionClient(*client_);
                        }
                    }
                }
            };
            return M::importer(new LocalI(locator, topic, wireToUserHook));
        }
        static std::shared_ptr<typename M::template Exporter<basic::ByteDataWithTopic>> createExporter(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook=std::nullopt, std::string const &heartbeatName = "") {
            class LocalE final : public M::template AbstractExporter<basic::ByteDataWithTopic> {
            private:
                ConnectionLocator locator_;
                Env *env_;
                std::function<void(basic::ByteDataWithTopic &&)> publisher_;
                std::optional<UserToWireHook> userToWireHook_;
                std::string heartbeatName_;
            public:
                LocalE(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook, std::string const &heartbeatName)
                    : locator_(locator), env_(nullptr), publisher_(), userToWireHook_(userToWireHook), heartbeatName_(heartbeatName)
                {
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                    publisher_ = env->zeroMQ_getPublisher(locator_, userToWireHook_);
                    if constexpr (std::is_convertible_v<
                        Env *
                        , HeartbeatAndAlertComponent *
                    >) {
                        static_cast<HeartbeatAndAlertComponent *>(env)->addBroadcastChannel(
                            heartbeatName_
                            , std::string("zeromq://")+locator_.toSerializationFormat()
                        );
                    }
                }
                virtual void handle(typename M::template InnerData<basic::ByteDataWithTopic> &&data) override final {
                    if (env_) {
                        TM_INFRA_EXPORTER_TRACER(env_);
                        publisher_(std::move(data.timedData.value));
                    }
                }
            };
            return M::exporter(new LocalE(locator, userToWireHook, heartbeatName));
        }
        template <class T>
        static std::shared_ptr<typename M::template Exporter<basic::TypedDataWithTopic<T>>> createTypedExporter(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook=std::nullopt, std::string const &heartbeatName = "") {
            class LocalE final : public M::template AbstractExporter<basic::TypedDataWithTopic<T>> {
            private:
                ConnectionLocator locator_;
                Env *env_;
                std::function<void(basic::ByteDataWithTopic &&)> publisher_;
                std::optional<UserToWireHook> userToWireHook_;
                std::string heartbeatName_;
            public:
                LocalE(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook, std::string const &heartbeatName)
                    : locator_(locator), env_(nullptr), publisher_(), userToWireHook_(userToWireHook), heartbeatName_(heartbeatName)
                {
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                    if (!userToWireHook_) {
                        userToWireHook_ = DefaultHookFactory<Env>::template outgoingHook<T>(env);
                    }
                    publisher_ = env->zeroMQ_getPublisher(locator_, userToWireHook_);
                    if constexpr (std::is_convertible_v<
                        Env *
                        , HeartbeatAndAlertComponent *
                    >) {
                        static_cast<HeartbeatAndAlertComponent *>(env)->addBroadcastChannel(
                            heartbeatName_
                            , std::string("zeromq://")+locator_.toSerializationFormat()
                        );
                    }
                }
                virtual void handle(typename M::template InnerData<basic::TypedDataWithTopic<T>> &&data) override final {
                    if (env_) {
                        TM_INFRA_EXPORTER_TRACER(env_);
                        std::string s = basic::bytedata_utils::RunSerializer<T>::apply(data.timedData.value.content);
                        publisher_({std::move(data.timedData.value.topic), std::move(s)});
                    }
                }
            };
            return M::exporter(new LocalE(locator, userToWireHook, heartbeatName));
        }
    };

} } } } }

#endif