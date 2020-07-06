#ifndef TM_KIT_TRANSPORT_REDIS_REDIS_ON_ORDER_FACILITY_HPP_
#define TM_KIT_TRANSPORT_REDIS_REDIS_ON_ORDER_FACILITY_HPP_

#include <type_traits>
#include <mutex>
#include <unordered_map>
#include <future>

#include <tm_kit/infra/RealTimeMonad.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/redis/RedisComponent.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace redis {

    template <class Env, std::enable_if_t<std::is_base_of_v<RedisComponent, Env>, int> = 0>
    class RedisOnOrderFacility {
    public:
        using M = infra::RealTimeMonad<Env>;
    private:
        struct OnOrderFacilityCommunicationInfo {
            bool isFinal;
        };

        static std::tuple<
            basic::ByteDataWithID
            , std::optional<OnOrderFacilityCommunicationInfo>
            >parseReplyData(basic::ByteDataWithID &&d) {
            auto l = d.content.length();
            if (l == 0) {
                return {std::move(d), {OnOrderFacilityCommunicationInfo {false}}};
            } else {
                char lastByte = d.content[l-1];
                d.content.resize(l-1);
                return {std::move(d), {OnOrderFacilityCommunicationInfo {(lastByte != (char) 0)}}};
            }
        } 

        static basic::ByteDataWithID createReplyData(basic::ByteDataWithID &&d, OnOrderFacilityCommunicationInfo const &info) {
            auto l = d.content.length();
            d.content.resize(l+1);
            d.content[l] = (info.isFinal?(char) 1:(char) 0);
            return std::move(d);
        }

        static void sendRequest(Env *env, std::function<void(basic::ByteDataWithID &&)>requester, basic::ByteDataWithID &&req) {
            requester(std::move(req));
        }

        template <class Identity, class Request>
        static void sendRequestWithIdentity(Env *env, std::function<void(basic::ByteDataWithID &&)>requester, basic::ByteDataWithID &&req) {
            requester({
                std::move(req.id)
                , static_cast<ClientSideAbstractIdentityAttacherComponent<Identity,Request> *>(env)->attach_identity(basic::ByteData {std::move(req.content)}).content
            });
        }

        static std::shared_ptr<typename M::template Importer<basic::ByteDataWithID>> createOnOrderFacilityRPCConnectorIncomingLegOnly(ConnectionLocator const &locator, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::ByteDataWithID> {
            private:
                ConnectionLocator locator_;
                std::optional<ByteDataHookPair> hooks_;
            public:
                LocalI(ConnectionLocator const &locator, std::optional<ByteDataHookPair> hooks)
                    : locator_(locator), hooks_(hooks)
                {
                }
                virtual void start(Env *env) override final {
                    env->redis_setRPCServer(
                        locator_
                        , [this,env](basic::ByteDataWithID &&d) {
                            this->publish(M::template pureInnerData<basic::ByteDataWithID>(env, std::move(d)));
                        }
                        , hooks_
                    );
                }
            };
            return M::importer(new LocalI(locator, hooks));
        }

        static std::tuple<
            std::shared_ptr<typename M::template Importer<basic::ByteDataWithID>>
            , std::shared_ptr<typename M::template Exporter<basic::ByteDataWithID>> 
        > createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(ConnectionLocator const &locator, std::optional<ByteDataHookPair> hooks=std::nullopt) {
            class LocalI final : public M::template AbstractImporter<basic::ByteDataWithID> {
            private:
                ConnectionLocator locator_;
                std::shared_ptr<std::function<void(bool, basic::ByteDataWithID &&)>> replierPtr_;
                std::optional<ByteDataHookPair> hooks_;
            public:
                LocalI(ConnectionLocator const &locator, std::shared_ptr<std::function<void(bool, basic::ByteDataWithID &&)>> const & replierPtr, std::optional<ByteDataHookPair> hooks)
                    : locator_(locator), replierPtr_(replierPtr), hooks_(hooks)
                {
                }
                virtual void start(Env *env) override final {
                    *replierPtr_ = env->redis_setRPCServer(
                        locator_
                        , [this,env](basic::ByteDataWithID &&d) {
                            this->publish(M::template pureInnerData<basic::ByteDataWithID>(env, std::move(d)));
                        }
                        , hooks_
                    );
                }
            };
            class LocalE final : public M::template AbstractExporter<basic::ByteDataWithID> {
            private:
                ConnectionLocator locator_;
                Env *env_;
                std::shared_ptr<std::function<void(bool, basic::ByteDataWithID &&)>> replierPtr_;
            public:
                LocalE(ConnectionLocator const &locator, std::shared_ptr<std::function<void(bool, basic::ByteDataWithID &&)>> const &replierPtr)
                    : locator_(locator), env_(nullptr), replierPtr_(replierPtr)
                {
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                }
                virtual void handle(typename M::template InnerData<basic::ByteDataWithID> &&data) override final {
                    if (env_) {
                        OnOrderFacilityCommunicationInfo info;
                        info.isFinal = data.timedData.finalFlag;
                        auto wireData = createReplyData(std::move(data.timedData.value), info);
                        (*replierPtr_)(info.isFinal, std::move(wireData));
                    }
                }
            };
            auto replierPtr = std::make_shared<std::function<void(bool, basic::ByteDataWithID &&)>>(
                [](bool, basic::ByteDataWithID &&) {}
            );
            return { M::importer(new LocalI(locator, replierPtr, hooks)), M::exporter(new LocalE(locator, replierPtr)) };
        }

        template <class A>
        static auto simplyDeserialize()
            -> typename infra::MonadRunner<M>::template ActionPtr<basic::ByteDataWithID, typename M::template Key<A>>
        {
            return basic::SerializationActions<M>::template deserializeWithKey<A>();
        }

        template <class Identity, class A>
        static auto checkIdentityAndDeserialize()
            -> typename infra::MonadRunner<M>::template ActionPtr<basic::ByteDataWithID, typename M::template Key<std::tuple<Identity,A>>>
        {
            return M::template kleisli<basic::ByteDataWithID>(
                [](typename M::template InnerData<basic::ByteDataWithID> &&input) -> typename M::template Data<typename M::template Key<std::tuple<Identity,A>>> {
                    auto checkIdentityRes = static_cast<ServerSideAbstractIdentityCheckerComponent<Identity,A> *>(input.environment)->check_identity(basic::ByteData {std::move(input.timedData.value.content)});
                    if (!checkIdentityRes) {
                        return std::nullopt;
                    }
                    auto parseRes = basic::SerializationActions<M>::template deserializeFunc<A>(std::move(std::get<1>(*checkIdentityRes).content));
                    if (!parseRes) {
                        return std::nullopt;
                    }
                    auto retVal = typename M::template Key<std::tuple<Identity, A>> {
                        Env::id_from_string(input.timedData.value.id)
                        , std::tuple<Identity, A> {
                            std::move(std::get<0>(*checkIdentityRes))
                            , std::move(*parseRes)
                        }
                    };
                    return M::template pureInnerData<typename M::template Key<std::tuple<Identity, A>>> (
                        input.environment
                        , std::move(retVal)
                    );
                }
            );
        }

    public:
        class WithoutIdentity {
        public:
            template <class A, class B>
            static std::shared_ptr<typename M::template OnOrderFacility<A, B>> createTypedRPCOnOrderFacility(
                ConnectionLocator const &locator
                , std::optional<ByteDataHookPair> hooks = std::nullopt) {
                class LocalCore final : public virtual infra::RealTimeMonadComponents<Env>::IExternalComponent, public virtual infra::RealTimeMonadComponents<Env>::template AbstractOnOrderFacility<A,B> {
                private:
                    Env *env_;
                    ConnectionLocator locator_;
                    std::function<void(basic::ByteDataWithID &&)> requester_;
                    std::optional<ByteDataHookPair> hooks_;
                public:
                    LocalCore(ConnectionLocator const &locator, std::optional<ByteDataHookPair> hooks) : env_(nullptr), locator_(locator), hooks_(hooks) {}
                    virtual void start(Env *env) override final {
                        env_ = env;
                        requester_ = env->redis_setRPCClient(
                            locator_
                            , [this]() {
                                return Env::id_to_string(env_->new_id());
                            }
                            , [this](basic::ByteDataWithID &&data) {
                                auto parseRes = parseReplyData(std::move(data));
                                auto result = basic::bytedata_utils::RunDeserializer<B>::apply(std::get<0>(parseRes).content);
                                if (!result) {
                                    return;
                                }
                                this->publish(env_, typename M::template Key<B> {Env::id_from_string(std::get<0>(parseRes).id), std::move(*result)}, (std::get<1>(parseRes)?std::get<1>(parseRes)->isFinal:false));
                            }
                            , hooks_);
                    }
                    virtual void handle(typename M::template InnerData<typename M::template Key<A>> &&data) override final {
                        if (env_) {
                            basic::ByteData s = { basic::SerializationActions<M>::template serializeFunc<A>(
                                data.timedData.value.key()
                            ) };
                            sendRequest(env_, requester_, basic::ByteDataWithID {
                                Env::id_to_string(data.timedData.value.id())
                                , std::move(s.content)
                            });
                        }     
                    }
                };
                return M::fromAbstractOnOrderFacility(new LocalCore(locator, hooks));
            }

            template <class A, class B>
            static void wrapOnOrderFacility(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacility<A,B>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, hooks);
                auto deserializer = simplyDeserialize<A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));
            }
            template <class A, class B>
            static void wrapOnOrderFacilityWithoutReply(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacility<A,B>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, hooks);
                auto deserializer = simplyDeserialize<A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);
            }
            template <class A, class B, class C>
            static void wrapLocalOnOrderFacility(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template LocalOnOrderFacility<A,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, hooks);
                auto deserializer = simplyDeserialize<A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithLocalFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));
            }
            template <class A, class B, class C>
            static void wrapLocalOnOrderFacilityWithoutReply(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template LocalOnOrderFacility<A,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, hooks);
                auto deserializer = simplyDeserialize<A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithLocalFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);
            }
            template <class A, class B, class C>
            static void wrapOnOrderFacilityWithExternalEffects(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<A,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, hooks);
                auto deserializer = simplyDeserialize<A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithFacilityWithExternalEffects(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));
            }
            template <class A, class B, class C>
            static void wrapOnOrderFacilityWithExternalEffectsWithoutReply(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<A,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, hooks);
                auto deserializer = simplyDeserialize<A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithFacilityWithExternalEffectsAndForget(runner.actionAsSource(deserializer), toBeWrapped);
            }
            template <class A, class B, class C, class D>
            static void wrapVIEOnOrderFacility(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template VIEOnOrderFacility<A,B,C,D>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, hooks);
                auto deserializer = simplyDeserialize<A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithVIEFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));
            }
            template <class A, class B, class C, class D>
            static void wrapVIEOnOrderFacilityWithoutReply(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template VIEOnOrderFacility<A,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, hooks);
                auto deserializer = simplyDeserialize<A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithVIEFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);
            }
            
            template <class A, class B>
            static auto facilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template FacilityWrapper<A,B> {
                return { std::bind(wrapOnOrderFacility<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B>
            static auto facilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template FacilityWrapper<A,B> {
                return { std::bind(wrapOnOrderFacilityWithoutReply<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto localFacilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template LocalFacilityWrapper<A,B,C> {
                return { std::bind(wrapLocalOnOrderFacility<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto localFacilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template LocalFacilityWrapper<A,B,C> {
                return { std::bind(wrapLocalOnOrderFacilityWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto facilityWithExternalEffectsWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template FacilityWithExternalEffectsWrapper<A,B,C> {
                return { std::bind(wrapOnOrderFacilityWithExternalEffects<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto facilityWithExternalEffectsWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template FacilityWithExternalEffectsWrapper<A,B,C> {
                return { std::bind(wrapOnOrderFacilityWithExternalEffectsWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C, class D>
            static auto vieFacilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template VIEFacilityWrapper<A,B,C,D> {
                return { std::bind(wrapVIEOnOrderFacility<A,B,C,D>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C, class D>
            static auto vieFacilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template VIEFacilityWrapper<A,B,C,D> {
                return { std::bind(wrapVIEOnOrderFacilityWithoutReply<A,B,C,D>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }

            template <class A, class B>
            static std::future<B> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
                std::shared_ptr<std::promise<B>> ret = std::make_shared<std::promise<B>>();
                basic::ByteData byteData = { basic::SerializationActions<M>::template serializeFunc<A>(request) };
                typename M::template Key<basic::ByteData> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(byteData));
                
                auto requester = env->redis_setRPCClient(
                    rpcQueueLocator
                    , [env]() {
                        return Env::id_to_string(env->new_id());
                    }
                    , [ret](basic::ByteDataWithID &&data) {
                        auto parseRes = parseReplyData(std::move(data));
                        auto val = basic::bytedata_utils::RunDeserializer<B>::apply(std::get<0>(parseRes).content);
                        if (!val) {
                            return;
                        }
                        ret->set_value(std::move(*val));
                    }
                    , hooks
                );
                sendRequest(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key())
                });
                return ret->get_future();
            }

            template <class A>
            static void typedOneShotRemoteCallNoReply(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
                basic::ByteData byteData = { basic::SerializationActions<M>::template serializeFunc<A>(request) };
                typename M::template Key<basic::ByteData> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(byteData));
                
                auto requester = env->redis_setRPCClient(
                    rpcQueueLocator
                    , [env]() {
                        return Env::id_to_string(env->new_id());
                    }
                    , [](basic::ByteDataWithID &&data) {
                    }
                    , hooks
                );
                sendRequest(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key())
                });
            }
        };
        
        template <class Identity>
        class WithIdentity {
        public:
            template <class A, class B>
            static std::shared_ptr<typename M::template OnOrderFacility<A, B>> createTypedRPCOnOrderFacility(
                ConnectionLocator const &locator
                , std::optional<ByteDataHookPair> hooks = std::nullopt) {
                class LocalCore final : public virtual infra::RealTimeMonadComponents<Env>::IExternalComponent, public virtual infra::RealTimeMonadComponents<Env>::template AbstractOnOrderFacility<A,B> {
                private:
                    Env *env_;
                    ConnectionLocator locator_;
                    std::function<void(basic::ByteDataWithID &&)> requester_;
                    std::optional<ByteDataHookPair> hooks_;
                public:
                    LocalCore(ConnectionLocator const &locator, std::optional<ByteDataHookPair> hooks) : env_(nullptr), locator_(locator), hooks_(hooks) {}
                    virtual void start(Env *env) override final {
                        env_ = env;
                        requester_ = env->redis_setRPCClient(
                            locator_
                            , [this]() {
                                return Env::id_to_string(env_->new_id());
                            }
                            , [this](basic::ByteDataWithID &&data) {
                                auto parseRes = parseReplyData(std::move(data));
                                auto result = basic::bytedata_utils::RunDeserializer<B>::apply(std::get<0>(parseRes).content);
                                if (!result) {
                                    return;
                                }
                                this->publish(env_, typename M::template Key<B> {Env::id_from_string(std::get<0>(parseRes).id), std::move(*result)}, (std::get<1>(parseRes)?std::get<1>(parseRes)->isFinal:false));
                            }
                            , hooks_
                        );
                    }
                    virtual void handle(typename M::template InnerData<typename M::template Key<A>> &&data) override final {
                        if (env_) {
                            basic::ByteData s = { basic::SerializationActions<M>::template serializeFunc<A>(
                                data.timedData.value.key()
                            ) };
                            sendRequestWithIdentity<Identity,A>(env_, requester_, basic::ByteDataWithID {
                                Env::id_to_string(data.timedData.value.id())
                                , std::move(s.content)
                            });
                        }     
                    }
                };
                return M::fromAbstractOnOrderFacility(new LocalCore(locator, hooks));
            }

            template <class A, class B>
            static void wrapOnOrderFacility(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacility<std::tuple<Identity,A>,B>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, hooks);
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<std::tuple<Identity,A>,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));
            }
            template <class A, class B>
            static void wrapOnOrderFacilityWithoutReply(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacility<std::tuple<Identity,A>,B>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, hooks);
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);
            }
            template <class A, class B, class C>
            static void wrapLocalOnOrderFacility(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template LocalOnOrderFacility<std::tuple<Identity,A>,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, hooks);
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<std::tuple<Identity,A>,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithLocalFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));
            }
            template <class A, class B, class C>
            static void wrapLocalOnOrderFacilityWithoutReply(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template LocalOnOrderFacility<std::tuple<Identity,A>,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, hooks);
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithLocalFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);
            }
            template <class A, class B, class C>
            static void wrapOnOrderFacilityWithExternalEffects(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<std::tuple<Identity,A>,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, hooks);
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<std::tuple<Identity,A>,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithFacilityWithExternalEffects(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));
            }
            template <class A, class B, class C>
            static void wrapOnOrderFacilityWithExternalEffectsWithoutReply(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<std::tuple<Identity,A>,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, hooks);
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithFacilityWithExternalEffectsAndForget(runner.actionAsSource(deserializer), toBeWrapped);
            }
            template <class A, class B, class C, class D>
            static void wrapVIEOnOrderFacility(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template VIEOnOrderFacility<std::tuple<Identity,A>,B,C,D>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, hooks);
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<std::tuple<Identity,A>,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithVIEFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));
            }
            template <class A, class B, class C, class D>
            static void wrapVIEOnOrderFacilityWithoutReply(
                infra::MonadRunner<M> &runner
                , std::shared_ptr<typename M::template VIEOnOrderFacility<std::tuple<Identity,A>,B,C,D>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, hooks);
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithVIEFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);
            }

            template <class A, class B>
            static auto facilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template FacilityWrapper<std::tuple<Identity,A>,B> {
                return { std::bind(wrapOnOrderFacility<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B>
            static auto facilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template FacilityWrapper<std::tuple<Identity,A>,B> {
                return { std::bind(wrapOnOrderFacilityWithoutReply<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto localFacilityWrapperWithIdentity(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template LocalFacilityWrapper<std::tuple<Identity,A>,B,C> {
                return { std::bind(wrapLocalOnOrderFacility<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto localFacilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template LocalFacilityWrapper<std::tuple<Identity,A>,B,C> {
                return { std::bind(wrapLocalOnOrderFacilityWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto facilityWithExternalEffectsWrapperWithIdentity(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template FacilityWithExternalEffectsWrapper<std::tuple<Identity,A>,B,C> {
                return { std::bind(wrapOnOrderFacilityWithExternalEffects<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto facilityWithExternalEffectsWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template FacilityWithExternalEffectsWrapper<std::tuple<Identity,A>,B,C> {
                return { std::bind(wrapOnOrderFacilityWithExternalEffectsWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C, class D>
            static auto vieFacilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template VIEFacilityWrapper<A,B,C,D> {
                return { std::bind(wrapVIEOnOrderFacility<A,B,C,D>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C, class D>
            static auto vieFacilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::MonadRunner<M>::template VIEFacilityWrapper<A,B,C,D> {
                return { std::bind(wrapVIEOnOrderFacilityWithoutReply<A,B,C,D>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }

            template <class A, class B>
            static std::future<B> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
                std::shared_ptr<std::promise<B>> ret = std::make_shared<std::promise<B>>();
                basic::ByteData byteData = { basic::SerializationActions<M>::template serializeFunc<A>(request) };
                typename M::template Key<basic::ByteData> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(byteData));
                
                auto requester = env->redis_setRPCClient(
                    rpcQueueLocator
                    , [env]() {
                        return Env::id_to_string(env->new_id());
                    }
                    , [ret](basic::ByteDataWithID &&data) {
                        auto parseRes = parseReplyData(std::move(data));
                        auto val = basic::bytedata_utils::RunDeserializer<B>::apply(std::get<0>(parseRes).content);
                        if (!val) {
                            return;
                        }
                        ret->set_value(std::move(*val));
                    }
                    , hooks
                );
                sendRequestWithIdentity<Identity,A>(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key())
                });
                return ret->get_future();
            }

            template <class A>
            static void typedOneShotRemoteCallNoReply(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
                basic::ByteData byteData = { basic::SerializationActions<M>::template serializeFunc<A>(request) };
                typename M::template Key<basic::ByteData> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(byteData));
                auto requester = env->redis_setRPCClient(
                    rpcQueueLocator
                    , [env]() {
                        return Env::id_to_string(env->new_id());
                    }
                    , [](basic::ByteDataWithID &&data) {
                    }
                    , hooks
                );
                sendRequestWithIdentity<Identity,A>(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key())
                });
            }
        };

    };

} } } } }

#endif
