#ifndef TM_KIT_TRANSPORT_WEB_SOCKET_WEB_SOCKET_LAZY_IMPORTER_HPP_
#define TM_KIT_TRANSPORT_WEB_SOCKET_WEB_SOCKET_LAZY_IMPORTER_HPP_

#include <type_traits>

#include <boost/lexical_cast.hpp>

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/TraceNodesComponent.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/websocket/WebSocketComponent.hpp>
#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>
#include <tm_kit/transport/AbstractHookFactoryComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace web_socket {

    template <class Env, std::enable_if_t<std::is_base_of_v<WebSocketComponent, Env>, int> = 0>
    class WebSocketLazyImporter {
    public:
        using M = infra::RealTimeApp<Env>;
        static auto createLazyImporter(
            ConnectionLocator const &locator
            , std::variant<WebSocketComponent::NoTopicSelection, std::string, std::regex> const &topic=WebSocketComponent::NoTopicSelection()
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
            , std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> const &protocolReactor = {}
        ) -> std::shared_ptr<typename M::template Action<basic::ByteData, basic::ByteDataWithTopic>>  
        {
            class LocalA final : public M::template AbstractAction<basic::ByteData, basic::ByteDataWithTopic> {
            private:
                ConnectionLocator locator_;
                std::variant<WebSocketComponent::NoTopicSelection, std::string, std::regex> topic_;
                std::optional<ByteDataHookPair> hookPair_;
                std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> protocolReactor_;
                std::optional<uint32_t> client_;
                Env *env_;
                std::mutex mutex_;
            public:
                LocalA(ConnectionLocator const &locator, std::variant<WebSocketComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<ByteDataHookPair> const &hookPair, std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> const &protocolReactor)
                    : locator_(locator), topic_(topic), hookPair_(hookPair), protocolReactor_(protocolReactor), client_(std::nullopt), env_(nullptr), mutex_()
                {
                }
                ~LocalA() {
                    std::lock_guard<std::mutex> _(mutex_);
                    if (client_ && env_) {
                        env_->websocket_removeSubscriptionClient(*client_);
                        client_ = std::nullopt;
                    }
                }
                virtual bool isThreaded() const override final {
                    return true;
                }
                virtual bool isOneTimeOnly() const override final {
                    return false;
                }
                virtual void setIdleWorker(std::function<void(void *)> worker) override final {}
                virtual void setStartWaiter(std::function<void()> waiter) override final {}
                virtual void handle(typename M::template InnerData<basic::ByteData> &&input) override final {
                    std::lock_guard<std::mutex> _(mutex_);
                    if (!client_) {
                        env_ = input.environment;
                        if (hookPair_ && hookPair_->userToWire) {
                            client_ = env_->websocket_addSubscriptionClient(
                                locator_
                                , topic_
                                , [this](basic::ByteDataWithTopic &&d) {
                                    infra::TraceNodesComponentWrapper<Env, typename M::template AbstractAction<basic::ByteData, basic::ByteDataWithTopic>> _tracer(env_, this);
                                    this->publish(M::template pureInnerData<basic::ByteDataWithTopic>(env_, std::move(d)));
                                }
                                , (hookPair_?hookPair_->wireToUser:std::nullopt)
                                , hookPair_->userToWire->hook(std::move(input.timedData.value))
                                , protocolReactor_
                            );
                        } else {
                            client_ = env_->websocket_addSubscriptionClient(
                                locator_
                                , topic_
                                , [this](basic::ByteDataWithTopic &&d) {
                                    infra::TraceNodesComponentWrapper<Env, typename M::template AbstractAction<basic::ByteData, basic::ByteDataWithTopic>> _tracer(env_, this);
                                    this->publish(M::template pureInnerData<basic::ByteDataWithTopic>(env_, std::move(d)));
                                }
                                , (hookPair_?hookPair_->wireToUser:std::nullopt)
                                , std::move(input.timedData.value)
                                , protocolReactor_
                            );
                        }
                    }
                }
            };
            return M::fromAbstractAction(new LocalA(locator, topic, hookPair, protocolReactor));
        }
        template <class A, class B>
        static auto createTypedLazyImporter(
            ConnectionLocator const &locator
            , std::variant<WebSocketComponent::NoTopicSelection, std::string, std::regex> const &topic=WebSocketComponent::NoTopicSelection()
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
            , std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> const &protocolReactor = {}
        ) -> std::shared_ptr<typename M::template Action<A, basic::TypedDataWithTopic<B>>>  
        {
            class LocalA final : public M::template AbstractAction<A, basic::TypedDataWithTopic<B>> {
            private:
                ConnectionLocator locator_;
                std::variant<WebSocketComponent::NoTopicSelection, std::string, std::regex> topic_;
                std::optional<ByteDataHookPair> hookPair_;
                std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> protocolReactor_;
                std::optional<uint32_t> client_;
                Env *env_;
                std::mutex mutex_;
            public:
                LocalA(ConnectionLocator const &locator, std::variant<WebSocketComponent::NoTopicSelection, std::string, std::regex> const &topic, std::optional<ByteDataHookPair> const &hookPair, std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> const &protocolReactor)
                    : locator_(locator), topic_(topic), hookPair_(hookPair), protocolReactor_(protocolReactor), client_(std::nullopt), env_(nullptr), mutex_()
                {
                }
                ~LocalA() {
                    std::lock_guard<std::mutex> _(mutex_);
                    if (client_ && env_) {
                        env_->websocket_removeSubscriptionClient(*client_);
                        client_ = std::nullopt;
                    }
                }
                virtual bool isThreaded() const override final {
                    return true;
                }
                virtual bool isOneTimeOnly() const override final {
                    return false;
                }
                virtual void setIdleWorker(std::function<void(void *)> worker) override final {}
                virtual void setStartWaiter(std::function<void()> waiter) override final {}
                virtual void handle(typename M::template InnerData<A> &&input) override final {
                    std::lock_guard<std::mutex> _(mutex_);
                    if (!client_) {
                        env_ = input.environment;
                        std::optional<UserToWireHook> userToWire = (hookPair_?hookPair_->userToWire:std::nullopt);
                        if (!userToWire) {
                            userToWire = DefaultHookFactory<Env>::template outgoingHook<A>(env_);
                        }
                        std::optional<WireToUserHook> wireToUser = (hookPair_?hookPair_->wireToUser:std::nullopt);
                        if (!wireToUser) {
                            wireToUser = DefaultHookFactory<Env>::template incomingHook<B>(env_);
                        }
                        auto initialData = basic::bytedata_utils::RunSerializer<A>::apply(std::move(input.timedData.value));
                        if (userToWire) {
                            client_ = env_->websocket_addSubscriptionClient(
                                locator_
                                , topic_
                                , [this](basic::ByteDataWithTopic &&d) {
                                    infra::TraceNodesComponentWrapper<Env, typename M::template AbstractAction<A, basic::TypedDataWithTopic<B>>> _tracer(env_, this);
                                    B b;
                                    auto bRes = basic::bytedata_utils::RunDeserializer<B>::applyInPlace(b, d.content);
                                    if (bRes) {
                                        this->publish(M::template pureInnerData<basic::TypedDataWithTopic<B>>(env_, {std::move(d.topic), std::move(b)}));
                                    }
                                }
                                , wireToUser
                                , userToWire->hook(std::move(initialData))
                                , protocolReactor_
                            );
                        } else {
                            client_ = env_->websocket_addSubscriptionClient(
                                locator_
                                , topic_
                                , [this](basic::ByteDataWithTopic &&d) {
                                    infra::TraceNodesComponentWrapper<Env, typename M::template AbstractAction<A, basic::TypedDataWithTopic<B>>> _tracer(env_, this);
                                    B b;
                                    auto bRes = basic::bytedata_utils::RunDeserializer<B>::applyInPlace(b, d.content);
                                    if (bRes) {
                                        this->publish(M::template pureInnerData<basic::TypedDataWithTopic<B>>(env_, {std::move(d.topic), std::move(b)}));
                                    }
                                }
                                , wireToUser
                                , std::move(initialData)
                                , protocolReactor_
                            );
                        }
                    }
                }
            };
            return M::fromAbstractAction(new LocalA(locator, topic, hookPair, protocolReactor));
        }
    };

} } } } }

#endif