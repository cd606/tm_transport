#ifndef TM_KIT_TRANSPORT_RABBITMQ_RABBITMQ_IMPORTER_EXPORTER_HPP_
#define TM_KIT_TRANSPORT_RABBITMQ_RABBITMQ_IMPORTER_EXPORTER_HPP_

#include <type_traits>

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>
#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>
#include <tm_kit/transport/AbstractBroadcastHookFactoryComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace rabbitmq {

    template <class Env, std::enable_if_t<std::is_base_of_v<RabbitMQComponent, Env>, int> = 0>
    class RabbitMQImporterExporter {
    public:
        using M = infra::RealTimeApp<Env>;
        static std::shared_ptr<typename M::template Importer<basic::ByteDataWithTopic>> createImporter(ConnectionLocator const &exchangeLocator, std::string const &topic="", std::optional<WireToUserHook> wireToUserHook=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::ByteDataWithTopic> {
            private:
                ConnectionLocator exchangeLocator_;
                std::string topic_;
                std::optional<WireToUserHook> wireToUserHook_;
            public:
                LocalI(ConnectionLocator const &exchangeLocator, std::string const &topic, std::optional<WireToUserHook> wireToUserHook)
                    : exchangeLocator_(exchangeLocator), topic_(topic), wireToUserHook_(wireToUserHook)
                {
                }
                virtual void start(Env *env) override final {
                    env->rabbitmq_addExchangeSubscriptionClient(
                        exchangeLocator_
                        , topic_
                        , [this,env](std::string const &, basic::ByteDataWithTopic &&d) {
                            this->publish(M::template pureInnerData<basic::ByteDataWithTopic>(env, std::move(d)));
                        }
                        , wireToUserHook_
                    );
                }
            };
            return M::importer(new LocalI(exchangeLocator, topic, wireToUserHook));
        }
        template <class T>
        static std::shared_ptr<typename M::template Importer<basic::TypedDataWithTopic<T>>> createTypedImporter(ConnectionLocator const &exchangeLocator, std::string const &topic="", std::optional<WireToUserHook> wireToUserHook=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::TypedDataWithTopic<T>> {
            private:
                ConnectionLocator exchangeLocator_;
                std::string topic_;
                std::optional<WireToUserHook> wireToUserHook_;
            public:
                LocalI(ConnectionLocator const &exchangeLocator, std::string const &topic, std::optional<WireToUserHook> wireToUserHook)
                    : exchangeLocator_(exchangeLocator), topic_(topic), wireToUserHook_(wireToUserHook)
                {
                }
                virtual void start(Env *env) override final {
                    if (!wireToUserHook_) {
                        wireToUserHook_ = DefaultBroadcastHookFactory<Env>::template incomingHook<T>(env);
                    }
                    env->rabbitmq_addExchangeSubscriptionClient(
                        exchangeLocator_
                        , topic_
                        , [this,env](std::string const &, basic::ByteDataWithTopic &&d) {
                            auto t = basic::bytedata_utils::RunDeserializer<T>::apply(d.content);
                            if (t) {
                                this->publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(*t)}));
                            }
                        }
                        , wireToUserHook_
                    );
                }
            };
            return M::importer(new LocalI(exchangeLocator, topic, wireToUserHook));
        }
        static std::shared_ptr<typename M::template Exporter<basic::ByteDataWithTopic>> createExporter(ConnectionLocator const &exchangeLocator, std::optional<UserToWireHook> userToWireHook=std::nullopt, std::string const &heartbeatName="") {
            class LocalE final : public M::template AbstractExporter<basic::ByteDataWithTopic> {
            private:
                ConnectionLocator exchangeLocator_;
                Env *env_;
                std::function<void(std::string const &, basic::ByteDataWithTopic &&)> publisher_;
                std::optional<UserToWireHook> userToWireHook_;
                std::string heartbeatName_;
            public:
                LocalE(ConnectionLocator const &exchangeLocator, std::optional<UserToWireHook> userToWireHook, std::string const &heartbeatName)
                    : exchangeLocator_(exchangeLocator), env_(nullptr), publisher_(), userToWireHook_(userToWireHook), heartbeatName_(heartbeatName)
                {
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                    publisher_ = env_->rabbitmq_getExchangePublisher(exchangeLocator_, userToWireHook_);
                    if constexpr (std::is_convertible_v<
                        Env *
                        , HeartbeatAndAlertComponent *
                    >) {
                        static_cast<HeartbeatAndAlertComponent *>(env)->addBroadcastChannel(
                            heartbeatName_
                            , std::string("rabbitmq://")+exchangeLocator_.toSerializationFormat()
                        );
                    }
                }
                virtual void handle(typename M::template InnerData<basic::ByteDataWithTopic> &&data) override final {
                    if (env_) {
                        publisher_("", std::move(data.timedData.value));
                    }
                }
            };
            return M::exporter(new LocalE(exchangeLocator, userToWireHook, heartbeatName));
        }
        template <class T>
        static std::shared_ptr<typename M::template Exporter<basic::TypedDataWithTopic<T>>> createTypedExporter(ConnectionLocator const &exchangeLocator, std::optional<UserToWireHook> userToWireHook=std::nullopt, std::string const &heartbeatName = "") {
            class LocalE final : public M::template AbstractExporter<basic::TypedDataWithTopic<T>> {
            private:
                ConnectionLocator exchangeLocator_;
                Env *env_;
                std::function<void(std::string const &, basic::ByteDataWithTopic &&)> publisher_;
                std::optional<UserToWireHook> userToWireHook_;
                std::string heartbeatName_;
            public:
                LocalE(ConnectionLocator const &exchangeLocator, std::optional<UserToWireHook> userToWireHook, std::string const &heartbeatName)
                    : exchangeLocator_(exchangeLocator), env_(nullptr), publisher_(), userToWireHook_(userToWireHook)
                {
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                    if (!userToWireHook_) {
                        userToWireHook_ = DefaultBroadcastHookFactory<Env>::template outgoingHook<T>(env);
                    }
                    publisher_ = env_->rabbitmq_getExchangePublisher(exchangeLocator_, userToWireHook_);
                    if constexpr (std::is_convertible_v<
                        Env *
                        , HeartbeatAndAlertComponent *
                    >) {
                        static_cast<HeartbeatAndAlertComponent *>(env)->addBroadcastChannel(
                            heartbeatName_
                            , std::string("rabbitmq://")+exchangeLocator_.toSerializationFormat()
                        );
                    }
                }
                virtual void handle(typename M::template InnerData<basic::TypedDataWithTopic<T>> &&data) override final {
                    if (env_) {
                        std::string s = basic::bytedata_utils::RunSerializer<T>::apply(data.timedData.value.content);
                        publisher_("", {std::move(data.timedData.value.topic), std::move(s)});
                    }
                }
            };
            return M::exporter(new LocalE(exchangeLocator, userToWireHook, heartbeatName));
        }
    };

} } } } }

#endif