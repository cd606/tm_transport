#ifndef TM_KIT_TRANSPORT_MULTICAST_MULTICAST_IMPORTER_EXPORTER_HPP_
#define TM_KIT_TRANSPORT_MULTICAST_MULTICAST_IMPORTER_EXPORTER_HPP_

#include <type_traits>

#include <boost/lexical_cast.hpp>

#include <tm_kit/infra/RealTimeMonad.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/multicast/MulticastComponent.hpp>
#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace multicast {

    template <class Env, std::enable_if_t<std::is_base_of_v<MulticastComponent, Env>, int> = 0>
    class MulticastImporterExporter {
    public:
        using M = infra::RealTimeMonad<Env>;
        static std::shared_ptr<typename M::template Importer<basic::ByteDataWithTopic>> createImporter(ConnectionLocator const &locator, std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> const &topic=MulticastComponent::NoTopicSelection(), std::optional<WireToUserHook> wireToUserHook=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::ByteDataWithTopic> {
            private:
                ConnectionLocator locator_;
                std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> topic_;
                std::optional<WireToUserHook> wireToUserHook_;
            public:
                LocalI(ConnectionLocator const &locator, std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<WireToUserHook> wireToUserHook)
                    : locator_(locator), topic_(topic), wireToUserHook_(wireToUserHook)
                {
                }
                virtual void start(Env *env) override final {
                    env->multicast_addSubscriptionClient(
                        locator_
                        , topic_
                        , [this,env](basic::ByteDataWithTopic &&d) {
                            this->publish(M::template pureInnerData<basic::ByteDataWithTopic>(env, std::move(d)));
                        }
                        , wireToUserHook_
                    );
                    if constexpr (std::is_convertible_v<
                        Env *
                        , HeartbeatAndAlertComponent *
                    >) {
                        static_cast<HeartbeatAndAlertComponent *>(env)->addBroadcastChannel(
                            std::string("multicast://")+locator_.toSerializationFormat()
                        );
                    }
                }
            };
            return M::importer(new LocalI(locator, topic, wireToUserHook));
        }
        template <class T>
        static std::shared_ptr<typename M::template Importer<basic::TypedDataWithTopic<T>>> createTypedImporter(ConnectionLocator const &locator, std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> const &topic=MulticastComponent::NoTopicSelection(), std::optional<WireToUserHook> wireToUserHook=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::TypedDataWithTopic<T>> {
            private:
                ConnectionLocator locator_;
                std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> topic_;
                std::optional<WireToUserHook> wireToUserHook_;
            public:
                LocalI(ConnectionLocator const &locator, std::variant<MulticastComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<WireToUserHook> wireToUserHook)
                    : locator_(locator), topic_(topic), wireToUserHook_(wireToUserHook)
                {
                }
                virtual void start(Env *env) override final {
                    env->multicast_addSubscriptionClient(
                       locator_
                        , topic_
                        , [this,env](basic::ByteDataWithTopic &&d) {
                            auto t = basic::bytedata_utils::RunDeserializer<T>::apply(d.content);
                            if (t) {
                                this->publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(*t)}));
                            }
                        }
                        , wireToUserHook_
                    );
                    if constexpr (std::is_convertible_v<
                        Env *
                        , HeartbeatAndAlertComponent *
                    >) {
                        static_cast<HeartbeatAndAlertComponent *>(env)->addBroadcastChannel(
                            std::string("multicast://")+locator_.toSerializationFormat()
                        );
                    }
                }
            };
            return M::importer(new LocalI(locator, topic, wireToUserHook));
        }
        static std::shared_ptr<typename M::template Exporter<basic::ByteDataWithTopic>> createExporter(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook=std::nullopt) {
            class LocalE final : public M::template AbstractExporter<basic::ByteDataWithTopic> {
            private:
                ConnectionLocator locator_;
                Env *env_;
                int ttl_;
                std::function<void(basic::ByteDataWithTopic &&, int)> publisher_;
                std::optional<UserToWireHook> userToWireHook_;
            public:
                LocalE(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook)
                    : locator_(locator), env_(nullptr), publisher_(), userToWireHook_(userToWireHook)
                {
                    try {
                        ttl_ = boost::lexical_cast<int>(locator.query("ttl", "0"));
                    } catch (boost::bad_lexical_cast const &) {
                        ttl_ = 0;
                    }   
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                    publisher_ = env->multicast_getPublisher(locator_, userToWireHook_);
                }
                virtual void handle(typename M::template InnerData<basic::ByteDataWithTopic> &&data) override final {
                    if (env_) {
                        publisher_(std::move(data.timedData.value), ttl_);
                    }
                }
            };
            return M::exporter(new LocalE(locator, userToWireHook));
        }
        template <class T>
        static std::shared_ptr<typename M::template Exporter<basic::TypedDataWithTopic<T>>> createTypedExporter(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook=std::nullopt) {
            class LocalE final : public M::template AbstractExporter<basic::TypedDataWithTopic<T>> {
            private:
                ConnectionLocator locator_;
                Env *env_;
                int ttl_;
                std::function<void(basic::ByteDataWithTopic &&, int)> publisher_;
                std::optional<UserToWireHook> userToWireHook_;
            public:
                LocalE(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook)
                    : locator_(locator), env_(nullptr), publisher_(), userToWireHook_(userToWireHook)
                {
                    try {
                        ttl_ = boost::lexical_cast<int>(locator.query("ttl", "0"));
                    } catch (boost::bad_lexical_cast const &) {
                        ttl_ = 0;
                    } 
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                    publisher_ = env->multicast_getPublisher(locator_, userToWireHook_);
                }
                virtual void handle(typename M::template InnerData<basic::TypedDataWithTopic<T>> &&data) override final {
                    if (env_) {
                        std::string s = basic::bytedata_utils::RunSerializer<T>::apply(data.timedData.value.content);
                        publisher_({std::move(data.timedData.value.topic), std::move(s)}, ttl_);
                    }
                }
            };
            return M::exporter(new LocalE(locator, userToWireHook));
        }
    };

} } } } }

#endif