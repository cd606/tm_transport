#ifndef TM_KIT_TRANSPORT_NNG_NNG_IMPORTER_EXPORTER_HPP_
#define TM_KIT_TRANSPORT_NNG_NNG_IMPORTER_EXPORTER_HPP_

#include <type_traits>

#include <boost/lexical_cast.hpp>

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/nng/NNGComponent.hpp>
#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace nng {

    template <class Env, std::enable_if_t<std::is_base_of_v<NNGComponent, Env>, int> = 0>
    class NNGImporterExporter {
    public:
        using M = infra::RealTimeApp<Env>;
        static std::shared_ptr<typename M::template Importer<basic::ByteDataWithTopic>> createImporter(ConnectionLocator const &locator, std::variant<NNGComponent::NoTopicSelection, std::string, std::regex> const &topic=NNGComponent::NoTopicSelection(), std::optional<WireToUserHook> wireToUserHook=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::ByteDataWithTopic> {
            private:
                ConnectionLocator locator_;
                std::variant<NNGComponent::NoTopicSelection, std::string, std::regex> topic_;
                std::optional<WireToUserHook> wireToUserHook_;
            public:
                LocalI(ConnectionLocator const &locator, std::variant<NNGComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<WireToUserHook> wireToUserHook)
                    : locator_(locator), topic_(topic), wireToUserHook_(wireToUserHook)
                {
                }
                virtual void start(Env *env) override final {
                    env->nng_addSubscriptionClient(
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
                            std::string("nng://")+locator_.toSerializationFormat()
                        );
                    }
                }
            };
            return M::importer(new LocalI(locator, topic, wireToUserHook));
        }
        template <class T>
        static std::shared_ptr<typename M::template Importer<basic::TypedDataWithTopic<T>>> createTypedImporter(ConnectionLocator const &locator, std::variant<NNGComponent::NoTopicSelection, std::string, std::regex> const &topic=NNGComponent::NoTopicSelection(), std::optional<WireToUserHook> wireToUserHook=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::TypedDataWithTopic<T>> {
            private:
                ConnectionLocator locator_;
                std::variant<NNGComponent::NoTopicSelection, std::string, std::regex> topic_;
                std::optional<WireToUserHook> wireToUserHook_;
            public:
                LocalI(ConnectionLocator const &locator, std::variant<NNGComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<WireToUserHook> wireToUserHook)
                    : locator_(locator), topic_(topic), wireToUserHook_(wireToUserHook)
                {
                }
                virtual void start(Env *env) override final {
                    env->nng_addSubscriptionClient(
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
                            std::string("nng://")+locator_.toSerializationFormat()
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
                std::function<void(basic::ByteDataWithTopic &&)> publisher_;
                std::optional<UserToWireHook> userToWireHook_;
            public:
                LocalE(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook)
                    : locator_(locator), env_(nullptr), publisher_(), userToWireHook_(userToWireHook)
                {
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                    publisher_ = env->nng_getPublisher(locator_, userToWireHook_);
                }
                virtual void handle(typename M::template InnerData<basic::ByteDataWithTopic> &&data) override final {
                    if (env_) {
                        publisher_(std::move(data.timedData.value));
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
                std::function<void(basic::ByteDataWithTopic &&)> publisher_;
                std::optional<UserToWireHook> userToWireHook_;
            public:
                LocalE(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook)
                    : locator_(locator), env_(nullptr), publisher_(), userToWireHook_(userToWireHook)
                {
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                    publisher_ = env->nng_getPublisher(locator_, userToWireHook_);
                }
                virtual void handle(typename M::template InnerData<basic::TypedDataWithTopic<T>> &&data) override final {
                    if (env_) {
                        std::string s = basic::bytedata_utils::RunSerializer<T>::apply(data.timedData.value.content);
                        publisher_({std::move(data.timedData.value.topic), std::move(s)});
                    }
                }
            };
            return M::exporter(new LocalE(locator, userToWireHook));
        }
    };

} } } } }

#endif