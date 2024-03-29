#ifndef TM_KIT_TRANSPORT_SINGLECAST_SINGLECAST_IMPORTER_EXPORTER_HPP_
#define TM_KIT_TRANSPORT_SINGLECAST_SINGLECAST_IMPORTER_EXPORTER_HPP_

#include <type_traits>

#include <boost/lexical_cast.hpp>

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/TraceNodesComponent.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/singlecast/SinglecastComponent.hpp>
#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>
#include <tm_kit/transport/AbstractHookFactoryComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace singlecast {

    template <class Env, std::enable_if_t<std::is_base_of_v<SinglecastComponent, Env>, int> = 0>
    class SinglecastImporterExporter {
    public:
        using M = infra::RealTimeApp<Env>;
        static std::shared_ptr<typename M::template Importer<basic::ByteDataWithTopic>> createImporter(ConnectionLocator const &locator, std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> const &topic=SinglecastComponent::NoTopicSelection(), std::optional<WireToUserHook> wireToUserHook=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::ByteDataWithTopic>, public virtual infra::IControllableNode<Env> {
            private:
                ConnectionLocator locator_;
                std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> topic_;
                std::optional<WireToUserHook> wireToUserHook_;
                std::optional<uint32_t> client_;
                std::mutex mutex_;
            public:
                LocalI(ConnectionLocator const &locator, std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<WireToUserHook> wireToUserHook)
                    : locator_(locator), topic_(topic), wireToUserHook_(wireToUserHook), client_(std::nullopt), mutex_()
                {
                }
                virtual void start(Env *env) override final {
                    std::lock_guard<std::mutex> _(mutex_);
                    if (!client_) {
                        client_ = env->singlecast_addSubscriptionClient(
                            locator_
                            , topic_
                            , [this,env](basic::ByteDataWithTopic &&d) {
                                TM_INFRA_IMPORTER_TRACER(env);
                                this->publish(M::template pureInnerData<basic::ByteDataWithTopic>(env, std::move(d)));
                            }
                            , wireToUserHook_
                        );
                    }
                }
                virtual void control(Env *env, std::string const &command, std::vector<std::string> const &params) override final {
                    std::thread th([this,env,command]() {
                        if (command == "stop") {
                            std::lock_guard<std::mutex> _(mutex_);
                            if (client_) {
                                env->singlecast_removeSubscriptionClient(*client_);
                                client_ = std::nullopt;
                            }
                        } else if (command == "restart") {
                            start(env);
                        }
                    });
                    th.detach();
                }
            };
            return M::importer(new LocalI(locator, topic, wireToUserHook));
        }
        template <class T>
        static std::shared_ptr<typename M::template Importer<basic::TypedDataWithTopic<T>>> createTypedImporter(ConnectionLocator const &locator, std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> const &topic=SinglecastComponent::NoTopicSelection(), std::optional<WireToUserHook> wireToUserHook=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::TypedDataWithTopic<T>>, public virtual infra::IControllableNode<Env> {
            private:
                ConnectionLocator locator_;
                std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> topic_;
                std::optional<WireToUserHook> wireToUserHook_;
                std::optional<uint32_t> client_;
                std::mutex mutex_;
            public:
                LocalI(ConnectionLocator const &locator, std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<WireToUserHook> wireToUserHook)
                    : locator_(locator), topic_(topic), wireToUserHook_(wireToUserHook), client_(std::nullopt), mutex_()
                {
                }
                virtual void start(Env *env) override final {
                    std::lock_guard<std::mutex> _(mutex_);
                    if (!wireToUserHook_) {
                        wireToUserHook_ = DefaultHookFactory<Env>::template incomingHook<T>(env);
                    }
                    if (!client_) {
                        client_ = env->singlecast_addSubscriptionClient(
                        locator_
                            , topic_
                            , [this,env](basic::ByteDataWithTopic &&d) {
                                TM_INFRA_IMPORTER_TRACER(env);
                                T t;
                                auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                                if (tRes) {
                                    this->publish(M::template pureInnerData<basic::TypedDataWithTopic<T>>(env, {std::move(d.topic), std::move(t)}));
                                }
                            }
                            , wireToUserHook_
                        );
                    }
                }
                virtual void control(Env *env, std::string const &command, std::vector<std::string> const &/*params*/) override final {
                    std::thread th([this,env,command]() {
                        if (command == "stop") {
                            std::lock_guard<std::mutex> _(mutex_);
                            if (client_) {
                                env->singlecast_removeSubscriptionClient(*client_);
                                client_ = std::nullopt;
                            }
                        } else if (command == "restart") {
                            start(env);
                        }
                    });
                    th.detach();
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
                    publisher_ = env->singlecast_getPublisher(locator_, userToWireHook_);
                    if constexpr (std::is_convertible_v<
                        Env *
                        , HeartbeatAndAlertComponent *
                    >) {
                        static_cast<HeartbeatAndAlertComponent *>(env)->addBroadcastChannel(
                            heartbeatName_
                            , std::string("singlecast://")+locator_.toSerializationFormat()
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
                    publisher_ = env->singlecast_getPublisher(locator_, userToWireHook_);
                    if constexpr (std::is_convertible_v<
                        Env *
                        , HeartbeatAndAlertComponent *
                    >) {
                        static_cast<HeartbeatAndAlertComponent *>(env)->addBroadcastChannel(
                            heartbeatName_
                            , std::string("singlecast://")+locator_.toSerializationFormat()
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
        static std::future<basic::ByteDataWithTopic> fetchFirstUpdateAndDisconnect(Env *env, ConnectionLocator const &locator, std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<WireToUserHook> hook = std::nullopt) {
            std::shared_ptr<std::promise<basic::ByteDataWithTopic>> ret = std::make_shared<std::promise<basic::ByteDataWithTopic>>();
            std::shared_ptr<std::atomic<uint32_t>> id = std::make_shared<std::atomic<uint32_t>>();
                
            bool done = false;
            *id = env->singlecast_addSubscriptionClient(
                locator
                , topic 
                , [env, ret, id, done](basic::ByteDataWithTopic &&d) mutable {
                    if (!done) {
                        done = true;
                        std::thread([env, ret, id, d = std::move(d)]() {
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                            env->singlecast_removeSubscriptionClient(*id);
                            try {
                                ret->set_value_at_thread_exit(std::move(d));
                            } catch (std::future_error const &) {
                            }
                        }).detach();
                    }
                }
                , hook
            );
            return ret->get_future();
        }
        template <class T>
        static std::future<basic::TypedDataWithTopic<T>> fetchTypedFirstUpdateAndDisconnect(Env *env, ConnectionLocator const &locator, std::variant<SinglecastComponent::NoTopicSelection, std::string, std::regex> const &topic, std::function<bool(T const &)> predicate = std::function<bool(T const &)>(), std::optional<WireToUserHook> hook = std::nullopt) {
            std::shared_ptr<std::promise<basic::TypedDataWithTopic<T>>> ret = std::make_shared<std::promise<basic::TypedDataWithTopic<T>>>();
            std::shared_ptr<std::atomic<uint32_t>> id = std::make_shared<std::atomic<uint32_t>>();
                
            bool done = false;
            *id = env->singlecast_addSubscriptionClient(
                locator
                , topic 
                , [env, ret, id, predicate, done](basic::ByteDataWithTopic &&d) mutable {
                    if (!done) {
                        T t;
                        auto tRes = basic::bytedata_utils::RunDeserializer<T>::applyInPlace(t, d.content);
                        if (tRes) {
                            if (!predicate || predicate(t)) {
                                basic::TypedDataWithTopic<T> res {std::move(d.topic), std::move(t)};
                                done = true;
                                std::thread([env, ret, id, res = std::move(res)]() mutable {
                                    try {
                                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                                        env->singlecast_removeSubscriptionClient(*id);
                                        ret->set_value_at_thread_exit(std::move(res));
                                    } catch (std::future_error const &) {
                                    } catch (std::exception const &) {
                                        try {
                                            ret->set_exception_at_thread_exit(std::current_exception());
                                        } catch (std::future_error const &) {
                                        }
                                    }
                                }).detach();
                            }
                        }
                    }
                }
                , DefaultHookFactory<Env>::template supplyIncomingHook<T>(env, hook)
            );
            return ret->get_future();
        }
    };

} } } } }

#endif