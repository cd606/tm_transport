#ifndef TM_KIT_TRANSPORT_WEB_SOCKET_WEB_SOCKET_CLIENT_FACILITY_HPP_
#define TM_KIT_TRANSPORT_WEB_SOCKET_WEB_SOCKET_CLIENT_FACILITY_HPP_

#include <type_traits>
#include <mutex>
#include <unordered_map>
#include <future>

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/infra/TraceNodesComponent.hpp>
#include <tm_kit/infra/ControllableNode.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/WrapFacilitioidConnectorForSerialization.hpp>
#include <tm_kit/transport/websocket/WebSocketComponent.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>
#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace web_socket {

    template <class Env, std::enable_if_t<std::is_base_of_v<WebSocketComponent, Env>, int> = 0>
    class WebSocketOnOrderFacilityClient {
    public:
        using M = infra::RealTimeApp<Env>;
    private:
        static void sendRequest(Env *env, std::function<void(basic::ByteDataWithID &&)>requester, basic::ByteDataWithID &&req) {
            requester(std::move(req));
        }

        template <class Identity, class Request>
        static void sendRequestWithIdentity(Env *env, std::function<void(basic::ByteDataWithID &&)>requester, basic::ByteDataWithID &&req) {
            requester({
                std::move(req.id)
                , static_cast<typename DetermineClientSideIdentityForRequest<Env,Request>::ComponentType *>(env)->attach_identity(basic::ByteData {std::move(req.content)}).content
            });
        }

    private:
        class WithoutIdentity {
        public:
            template <class A, class B>
            static std::shared_ptr<typename M::template OnOrderFacility<A, B>> createTypedRPCOnOrderFacility(
                ConnectionLocator const &locator
                , std::optional<ByteDataHookPair> hooks = std::nullopt) {
                class LocalCore final : public virtual infra::RealTimeAppComponents<Env>::IExternalComponent, public virtual infra::RealTimeAppComponents<Env>::template AbstractOnOrderFacility<A,B>, public infra::IControllableNode<Env> {
                private:
                    Env *env_;
                    ConnectionLocator locator_;
                    std::function<void(basic::ByteDataWithID &&)> requester_;
                    uint32_t clientNum_;
                    std::optional<ByteDataHookPair> hooks_;
                public:
                    LocalCore(ConnectionLocator const &locator, std::optional<ByteDataHookPair> hooks) : env_(nullptr), locator_(locator), clientNum_(0), hooks_(hooks) {}
                    virtual void start(Env *env) override final {
                        env_ = env;
                        requester_ = env->websocket_setRPCClient(
                            locator_
                            , [this](bool isFinal, basic::ByteDataWithID &&data) {
                                B b;
                                auto result = basic::bytedata_utils::RunDeserializer<B>::applyInPlace(b, data.content);
                                if (!result) {
                                    return;
                                }
                                this->publish(env_, typename M::template Key<B> {Env::id_from_string(data.id), std::move(b)}, isFinal);
                            }
                            , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env_, hooks_), &clientNum_);
                    }
                    virtual void handle(typename M::template InnerData<typename M::template Key<A>> &&data) override final {
                        if (env_) {
                            TM_INFRA_FACILITY_TRACER(env_);
                            basic::ByteData s = { basic::SerializationActions<M>::template serializeFunc<A>(
                                data.timedData.value.key()
                            ) };
                            sendRequest(env_, requester_, basic::ByteDataWithID {
                                Env::id_to_string(data.timedData.value.id())
                                , std::move(s.content)
                            });
                        }     
                    }
                    virtual void control(Env *env, std::string const &command, std::vector<std::string> const &params) override final {
                        if (command == "stop") {
                            env->websocket_removeRPCClient(locator_, clientNum_);
                        }
                    }
                };
                return M::fromAbstractOnOrderFacility(new LocalCore(locator, hooks));
            }

            template <class A, class B>
            static std::future<B> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt, bool autoDisconnect=false, uint32_t *clientNumberOutput=nullptr) {
                std::shared_ptr<std::promise<B>> ret = std::make_shared<std::promise<B>>();
                basic::ByteData byteData = { basic::SerializationActions<M>::template serializeFunc<A>(request) };
                typename M::template Key<basic::ByteData> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(byteData));
                
                bool done = false;
                auto clientNum = std::make_shared<uint32_t>(0);
                auto requester = env->websocket_setRPCClient(
                    rpcQueueLocator
                    , [autoDisconnect,rpcQueueLocator,env,ret,done,clientNum](bool isFinal, basic::ByteDataWithID &&data) mutable {
                        if (!done) {
                            try {
                                B b;
                                auto val = basic::bytedata_utils::RunDeserializer<B>::applyInPlace(b, data.content);
                                if (!val) {
                                    throw std::runtime_error("RabbitMQOnOrderFacility::typedOneShotRemoteCall: deserialization error");
                                } else {
                                    done = true;
                                    if (autoDisconnect) {
                                        std::thread([env,rpcQueueLocator,ret,clientNum,b=std::move(b)]() mutable {
                                            try {
                                                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                                                env->websocket_removeRPCClient(rpcQueueLocator, *clientNum);
                                                ret->set_value_at_thread_exit(std::move(b));
                                            } catch (std::future_error const &) {
                                            } catch (std::exception const &) {
                                                try {
                                                    ret->set_exception_at_thread_exit(std::current_exception());
                                                } catch (std::future_error const &) {
                                                }
                                            }
                                        }).detach();
                                    } else {
                                        ret->set_value(std::move(b));
                                    }
                                }
                            } catch (std::future_error const &) {
                            } catch (std::exception const &) {
                                if (autoDisconnect) {
                                    std::thread([env,rpcQueueLocator,ret,clientNum,ex=std::current_exception()]() {
                                        try {
                                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                                            env->websocket_removeRPCClient(rpcQueueLocator, *clientNum);
                                            ret->set_exception_at_thread_exit(std::move(ex));
                                        } catch (std::future_error const &) {
                                        } catch (std::exception const &) {
                                            try {
                                                ret->set_exception_at_thread_exit(std::current_exception());
                                            } catch (std::future_error const &) {
                                            }
                                        }
                                    }).detach();
                                } else {
                                    try {
                                        ret->set_exception(std::current_exception());
                                    } catch (std::future_error const &) {
                                    }
                                }
                            }
                        }
                    }
                    , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hooks)
                    , clientNum.get()
                );
                if (clientNumberOutput) {
                    *clientNumberOutput = *clientNum;
                }
                sendRequest(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key().content)
                });
                return ret->get_future();
            }

            template <class A>
            static void typedOneShotRemoteCallNoReply(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt, bool autoDisconnect=false, uint32_t *clientNumberOutput=nullptr) {
                basic::ByteData byteData = { basic::SerializationActions<M>::template serializeFunc<A>(request) };
                typename M::template Key<basic::ByteData> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(byteData));
                
                uint32_t clientNum = 0;
                auto requester = env->websocket_setRPCClient(
                    rpcQueueLocator
                    , [](bool isFinal, basic::ByteDataWithID &&data) {
                    }
                    , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSideOutgoingOnly<A>(env, hooks)
                    , &clientNum
                );
                if (clientNumberOutput) {
                    *clientNumberOutput = clientNum;
                }
                sendRequest(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key().content)
                });
                if (autoDisconnect) {
                    env->websocket_removeRPCClient(rpcQueueLocator, clientNum);
                }
            }
        };
        
        template <class Identity>
        class WithIdentity {
        public:
            template <class A, class B>
            static std::shared_ptr<typename M::template OnOrderFacility<A, B>> createTypedRPCOnOrderFacility(
                ConnectionLocator const &locator
                , std::optional<ByteDataHookPair> hooks = std::nullopt) {
                class LocalCore final : public virtual infra::RealTimeAppComponents<Env>::IExternalComponent, public virtual infra::RealTimeAppComponents<Env>::template AbstractOnOrderFacility<A,B>, public infra::IControllableNode<Env> {
                private:
                    Env *env_;
                    ConnectionLocator locator_;
                    uint32_t clientNum_;
                    std::function<void(basic::ByteDataWithID &&)> requester_;
                    std::optional<ByteDataHookPair> hooks_;
                public:
                    LocalCore(ConnectionLocator const &locator, std::optional<ByteDataHookPair> hooks) : env_(nullptr), locator_(locator), clientNum_(0), hooks_(hooks) {}
                    virtual void start(Env *env) override final {
                        env_ = env;
                        requester_ = env->websocket_setRPCClient(
                            locator_
                            , [this](bool isFinal, basic::ByteDataWithID &&data) {
                                auto processRes = static_cast<typename DetermineClientSideIdentityForRequest<Env,A>::ComponentType *>(env_)->process_incoming_data(
                                    basic::ByteData {std::move(data.content)}
                                );
                                if (processRes) {
                                    B b;
                                    auto result = basic::bytedata_utils::RunDeserializer<B>::applyInPlace(b, processRes->content);
                                    if (!result) {
                                        return;
                                    }
                                    this->publish(env_, typename M::template Key<B> {Env::id_from_string(data.id), std::move(b)}, isFinal);
                                }
                            }
                            , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env_, hooks_)
                            , &clientNum_
                        );
                    }
                    virtual void handle(typename M::template InnerData<typename M::template Key<A>> &&data) override final {
                        if (env_) {
                            TM_INFRA_FACILITY_TRACER(env_);
                            basic::ByteData s = { basic::SerializationActions<M>::template serializeFunc<A>(
                                data.timedData.value.key()
                            ) };
                            sendRequestWithIdentity<Identity,A>(env_, requester_, basic::ByteDataWithID {
                                Env::id_to_string(data.timedData.value.id())
                                , std::move(s.content)
                            });
                        }     
                    }
                    virtual void control(Env *env, std::string const &command, std::vector<std::string> const &params) override final {
                        if (command == "stop") {
                            env->websocket_removeRPCClient(locator_, clientNum_);
                        }
                    }
                };
                return M::fromAbstractOnOrderFacility(new LocalCore(locator, hooks));
            }

            template <class A, class B>
            static std::future<B> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt, bool autoDisconnect=false, uint32_t *clientNumberOutput=nullptr) {
                std::shared_ptr<std::promise<B>> ret = std::make_shared<std::promise<B>>();
                basic::ByteData byteData = { basic::SerializationActions<M>::template serializeFunc<A>(request) };
                typename M::template Key<basic::ByteData> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(byteData));
                
                bool done = false;
                auto clientNum = std::make_shared<uint32_t>(0);
                auto requester = env->websocket_setRPCClient(
                    rpcQueueLocator
                    , [autoDisconnect,ret,env,rpcQueueLocator,done,clientNum](bool isFinal, basic::ByteDataWithID &&data) mutable {    
                        if (!done) {
                            try {
                                auto processRes = static_cast<typename DetermineClientSideIdentityForRequest<Env,A>::ComponentType *>(env)->process_incoming_data(
                                    basic::ByteData {std::move(data.content)}
                                );
                                if (processRes) {
                                    B b;
                                    auto val = basic::bytedata_utils::RunDeserializer<B>::applyInPlace(b, processRes->content);
                                    if (!val) {
                                        throw std::runtime_error("RabbitMQOnOrderFacility::typedOneShotRemoteCall: deserialization error"); 
                                    } else {
                                        done = true;
                                        if (autoDisconnect) {
                                            std::thread([env,rpcQueueLocator,ret,clientNum,b=std::move(b)]() mutable {
                                                try {
                                                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                                                    env->websocket_removeRPCClient(rpcQueueLocator, *clientNum);
                                                    ret->set_value_at_thread_exit(std::move(b));
                                                } catch (std::future_error const &) {
                                                } catch (std::exception const &) {
                                                    try {
                                                        ret->set_exception_at_thread_exit(std::current_exception());
                                                    } catch (std::future_error const &) {
                                                    }
                                                }
                                            }).detach();
                                        } else {
                                            ret->set_value(std::move(b));
                                        }
                                    }
                                }
                            } catch (std::future_error const &) {
                            } catch (std::exception const &) {
                                if (autoDisconnect) {
                                    std::thread([env,rpcQueueLocator,ret,clientNum,ex=std::current_exception()]() {
                                        try {
                                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                                            env->websocket_removeRPCClient(rpcQueueLocator, *clientNum);
                                            ret->set_exception_at_thread_exit(std::move(ex));
                                        } catch (std::future_error const &) {
                                        } catch (std::exception const &) {
                                            try {
                                                ret->set_exception_at_thread_exit(std::current_exception());
                                            } catch (std::future_error const &) {
                                            }
                                        }
                                    }).detach();
                                } else {
                                    try {
                                        ret->set_exception(std::current_exception());
                                    } catch (std::future_error const &) {
                                    }
                                }
                            }
                        }
                    }
                    , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hooks)
                    , clientNum.get()
                );
                if (clientNumberOutput) {
                    *clientNumberOutput = *clientNum;
                }
                sendRequestWithIdentity<Identity,A>(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key().content)
                });
                return ret->get_future();
            }

            template <class A>
            static void typedOneShotRemoteCallNoReply(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt, bool autoDisconnect=false, uint32_t *clientNumberOutput=nullptr) {
                basic::ByteData byteData = { basic::SerializationActions<M>::template serializeFunc<A>(request) };
                typename M::template Key<basic::ByteData> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(byteData));
                uint32_t clientNum;
                auto requester = env->websocket_setRPCClient(
                    rpcQueueLocator
                    , [](bool isFinal, basic::ByteDataWithID &&data) {
                    }
                    , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSideOutgoingOnly<A>(env, hooks)
                    , &clientNum
                );
                if (clientNumberOutput) {
                    *clientNumberOutput = clientNum;
                }
                sendRequestWithIdentity<Identity,A>(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key().content)
                });
                if (autoDisconnect) {
                    env->websocket_removeRPCClient(rpcQueueLocator, clientNum);
                }
            }
        };
    public:
        template <class A, class B>
        static std::shared_ptr<typename M::template OnOrderFacility<A, B>> createTypedRPCOnOrderFacility(
            ConnectionLocator const &locator
            , std::optional<ByteDataHookPair> hooks = std::nullopt) {
            if constexpr(DetermineClientSideIdentityForRequest<Env, A>::HasIdentity) {
                return WithIdentity<typename DetermineClientSideIdentityForRequest<Env, A>::IdentityType>
                    ::template createTypedRPCOnOrderFacility<A,B>(locator, hooks);
            } else {
                return WithoutIdentity
                    ::template createTypedRPCOnOrderFacility<A,B>(locator, hooks);
            }
        }

        template <class A, class B>
        static std::future<B> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt, bool autoDisconnect=false, uint32_t *clientNumberOutput=nullptr) {
            if constexpr(DetermineClientSideIdentityForRequest<Env, A>::HasIdentity) {
                return WithIdentity<typename DetermineClientSideIdentityForRequest<Env, A>::IdentityType>
                    ::template typedOneShotRemoteCall<A,B>(env, rpcQueueLocator, std::move(request), hooks, autoDisconnect, clientNumberOutput);
            } else {
                return WithoutIdentity
                    ::template typedOneShotRemoteCall<A,B>(env, rpcQueueLocator, std::move(request), hooks, autoDisconnect, clientNumberOutput);
            }
        }

        template <class A>
        static void typedOneShotRemoteCallNoReply(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt, bool autoDisconnect=false, uint32_t *clientNumberOutput=nullptr) {
            if constexpr(DetermineClientSideIdentityForRequest<Env, A>::HasIdentity) {
                WithIdentity<typename DetermineClientSideIdentityForRequest<Env, A>::IdentityType>
                    ::template typedOneShotRemoteCallNoReply<A>(env, rpcQueueLocator, std::move(request), hooks, autoDisconnect, clientNumberOutput);
            } else {
                WithoutIdentity
                    ::template typedOneShotRemoteCallNoReply<A>(env, rpcQueueLocator, std::move(request), hooks, autoDisconnect, clientNumberOutput);
            }
        }

    };

} } } } }

#endif
