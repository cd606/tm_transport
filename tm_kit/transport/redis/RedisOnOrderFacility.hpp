#ifndef TM_KIT_TRANSPORT_REDIS_REDIS_ON_ORDER_FACILITY_HPP_
#define TM_KIT_TRANSPORT_REDIS_REDIS_ON_ORDER_FACILITY_HPP_

#include <type_traits>
#include <mutex>
#include <unordered_map>
#include <future>

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/redis/RedisComponent.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>
#include <tm_kit/transport/HeartbeatAndAlertComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace redis {

    template <class Env, std::enable_if_t<std::is_base_of_v<RedisComponent, Env>, int> = 0>
    class RedisOnOrderFacility {
    public:
        using M = infra::RealTimeApp<Env>;
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
            -> typename infra::AppRunner<M>::template ActionPtr<basic::ByteDataWithID, typename M::template Key<A>>
        {
            return basic::SerializationActions<M>::template deserializeWithKey<A>();
        }

        template <class Identity, class A>
        static auto checkIdentityAndDeserialize()
            -> typename infra::AppRunner<M>::template ActionPtr<basic::ByteDataWithID, typename M::template Key<std::tuple<Identity,A>>>
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
                        , input.timedData.finalFlag
                    );
                }
            );
        }

        template <class Identity, class A, class B>
        static auto serializeBasedOnIdentity()
            -> typename infra::AppRunner<M>::template ActionPtr<typename M::template KeyedData<std::tuple<Identity,A>, B>, basic::ByteDataWithID>
        {
            return M::template kleisli<typename M::template KeyedData<std::tuple<Identity,A>, B>>(
                [](typename M::template InnerData<typename M::template KeyedData<std::tuple<Identity,A>, B>> &&input) -> typename M::template Data<basic::ByteDataWithID> {
                    return M::template pureInnerData<typename basic::ByteDataWithID> (
                        input.environment
                        , basic::ByteDataWithID {
                            Env::id_to_string(input.timedData.value.key.id())
                            , std::move(static_cast<ServerSideAbstractIdentityCheckerComponent<Identity,A> *>(input.environment)->process_outgoing_data(
                                std::get<0>(input.timedData.value.key.key())
                                , basic::ByteData { basic::bytedata_utils::RunSerializer<B>::apply(input.timedData.value.data) }
                            ).content)
                        }
                        , input.timedData.finalFlag
                    );
                }
            );
        }

        static void addChannelRegistration(infra::AppRunner<M> &runner, std::string const &name, ConnectionLocator const &locator) {
            if constexpr (std::is_convertible_v<
                Env *
                , HeartbeatAndAlertComponent *
            >) {
                static_cast<HeartbeatAndAlertComponent *>(runner.environment())->addFacilityChannel(
                    name
                    , std::string("redis://")+locator.toSerializationFormat()
                );
            }
        }

    private:
        class WithoutIdentity {
        public:
            template <class A, class B>
            static std::shared_ptr<typename M::template OnOrderFacility<A, B>> createTypedRPCOnOrderFacility(
                ConnectionLocator const &locator
                , std::optional<ByteDataHookPair> hooks = std::nullopt) {
                class LocalCore final : public virtual infra::RealTimeAppComponents<Env>::IExternalComponent, public virtual infra::RealTimeAppComponents<Env>::template AbstractOnOrderFacility<A,B> {
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
                            , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env_, hooks_));
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
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacility<A,B>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = simplyDeserialize<A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B>
            static void wrapOnOrderFacilityWithoutReply(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacility<A,B>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = simplyDeserialize<A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C>
            static void wrapLocalOnOrderFacility(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template LocalOnOrderFacility<A,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = simplyDeserialize<A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithLocalFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C>
            static void wrapLocalOnOrderFacilityWithoutReply(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template LocalOnOrderFacility<A,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = simplyDeserialize<A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithLocalFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C>
            static void wrapOnOrderFacilityWithExternalEffects(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<A,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = simplyDeserialize<A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithFacilityWithExternalEffects(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C>
            static void wrapOnOrderFacilityWithExternalEffectsWithoutReply(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<A,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = simplyDeserialize<A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithFacilityWithExternalEffectsAndForget(runner.actionAsSource(deserializer), toBeWrapped);

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C, class D>
            static void wrapVIEOnOrderFacility(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template VIEOnOrderFacility<A,B,C,D>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = simplyDeserialize<A>();
                auto serializer = basic::SerializationActions<M>::template serializeWithKey<A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithVIEFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C, class D>
            static void wrapVIEOnOrderFacilityWithoutReply(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template VIEOnOrderFacility<A,B,C,D>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = simplyDeserialize<A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithVIEFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            
            template <class A, class B>
            static auto facilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template FacilityWrapper<A,B> {
                return { std::bind(wrapOnOrderFacility<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B>
            static auto facilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template FacilityWrapper<A,B> {
                return { std::bind(wrapOnOrderFacilityWithoutReply<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto localFacilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template LocalFacilityWrapper<A,B,C> {
                return { std::bind(wrapLocalOnOrderFacility<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto localFacilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template LocalFacilityWrapper<A,B,C> {
                return { std::bind(wrapLocalOnOrderFacilityWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto facilityWithExternalEffectsWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template FacilityWithExternalEffectsWrapper<A,B,C> {
                return { std::bind(wrapOnOrderFacilityWithExternalEffects<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto facilityWithExternalEffectsWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template FacilityWithExternalEffectsWrapper<A,B,C> {
                return { std::bind(wrapOnOrderFacilityWithExternalEffectsWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C, class D>
            static auto vieFacilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template VIEFacilityWrapper<A,B,C,D> {
                return { std::bind(wrapVIEOnOrderFacility<A,B,C,D>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C, class D>
            static auto vieFacilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template VIEFacilityWrapper<A,B,C,D> {
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
                    , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hooks)
                );
                sendRequest(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key().content)
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
                    , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSideOutgoingOnly<A>(env, hooks)
                );
                sendRequest(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key().content)
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
                class LocalCore final : public virtual infra::RealTimeAppComponents<Env>::IExternalComponent, public virtual infra::RealTimeAppComponents<Env>::template AbstractOnOrderFacility<A,B> {
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
                                auto processRes = static_cast<ClientSideAbstractIdentityAttacherComponent<Identity,A> *>(env_)->process_incoming_data(
                                    basic::ByteData {std::move(std::get<0>(parseRes).content)}
                                );
                                if (processRes) {
                                    auto result = basic::bytedata_utils::RunDeserializer<B>::apply(processRes->content);
                                    if (!result) {
                                        return;
                                    }
                                    this->publish(env_, typename M::template Key<B> {Env::id_from_string(std::get<0>(parseRes).id), std::move(*result)}, (std::get<1>(parseRes)?std::get<1>(parseRes)->isFinal:false));
                                }
                            }
                            , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env_, hooks_)
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
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacility<std::tuple<Identity,A>,B>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();
                auto serializer = serializeBasedOnIdentity<Identity,A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B>
            static void wrapOnOrderFacilityWithoutReply(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacility<std::tuple<Identity,A>,B>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C>
            static void wrapLocalOnOrderFacility(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template LocalOnOrderFacility<std::tuple<Identity,A>,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();
                auto serializer = serializeBasedOnIdentity<Identity,A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithLocalFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C>
            static void wrapLocalOnOrderFacilityWithoutReply(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template LocalOnOrderFacility<std::tuple<Identity,A>,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithLocalFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C>
            static void wrapOnOrderFacilityWithExternalEffects(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<std::tuple<Identity,A>,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();
                auto serializer = serializeBasedOnIdentity<Identity,A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithFacilityWithExternalEffects(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C>
            static void wrapOnOrderFacilityWithExternalEffectsWithoutReply(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<std::tuple<Identity,A>,B,C>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithFacilityWithExternalEffectsAndForget(runner.actionAsSource(deserializer), toBeWrapped);

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C, class D>
            static void wrapVIEOnOrderFacility(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template VIEOnOrderFacility<std::tuple<Identity,A>,B,C,D>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();
                auto serializer = serializeBasedOnIdentity<Identity,A,B>();

                runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.registerAction(serializer, wrapperItemsNamePrefix+"_serializer");
                runner.execute(deserializer, runner.importItem(std::get<0>(importerExporterPair)));
                runner.placeOrderWithVIEFacility(runner.actionAsSource(deserializer), toBeWrapped, runner.actionAsSink(serializer));
                runner.connect(runner.actionAsSource(serializer), runner.exporterAsSink(std::get<1>(importerExporterPair)));

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }
            template <class A, class B, class C, class D>
            static void wrapVIEOnOrderFacilityWithoutReply(
                infra::AppRunner<M> &runner
                , std::shared_ptr<typename M::template VIEOnOrderFacility<std::tuple<Identity,A>,B,C,D>> const &toBeWrapped
                , ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) {
                auto importer = createOnOrderFacilityRPCConnectorIncomingLegOnly(rpcQueueLocator, DefaultHookFactory<Env>::template supplyFacilityHookPair_ServerSide<A,B>(runner.environment(), hooks));
                auto deserializer = checkIdentityAndDeserialize<Identity,A>();

                runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
                runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
                runner.execute(deserializer, runner.importItem(importer));
                runner.placeOrderWithVIEFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);

                addChannelRegistration(runner, runner.getRegisteredName(toBeWrapped), rpcQueueLocator);
            }

            template <class A, class B>
            static auto facilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template FacilityWrapper<std::tuple<Identity,A>,B> {
                return { std::bind(wrapOnOrderFacility<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B>
            static auto facilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template FacilityWrapper<std::tuple<Identity,A>,B> {
                return { std::bind(wrapOnOrderFacilityWithoutReply<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto localFacilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template LocalFacilityWrapper<std::tuple<Identity,A>,B,C> {
                return { std::bind(wrapLocalOnOrderFacility<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto localFacilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template LocalFacilityWrapper<std::tuple<Identity,A>,B,C> {
                return { std::bind(wrapLocalOnOrderFacilityWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto facilityWithExternalEffectsWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template FacilityWithExternalEffectsWrapper<std::tuple<Identity,A>,B,C> {
                return { std::bind(wrapOnOrderFacilityWithExternalEffects<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C>
            static auto facilityWithExternalEffectsWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template FacilityWithExternalEffectsWrapper<std::tuple<Identity,A>,B,C> {
                return { std::bind(wrapOnOrderFacilityWithExternalEffectsWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C, class D>
            static auto vieFacilityWrapper(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template VIEFacilityWrapper<std::tuple<Identity,A>,B,C,D> {
                return { std::bind(wrapVIEOnOrderFacility<A,B,C,D>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
            }
            template <class A, class B, class C, class D>
            static auto vieFacilityWrapperWithoutReply(
                ConnectionLocator const &rpcQueueLocator
                , std::string const &wrapperItemsNamePrefix
                , std::optional<ByteDataHookPair> hooks = std::nullopt
            ) -> typename infra::AppRunner<M>::template VIEFacilityWrapper<std::tuple<Identity,A>,B,C,D> {
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
                    , [ret,env](basic::ByteDataWithID &&data) {
                        auto parseRes = parseReplyData(std::move(data));
                        auto processRes = static_cast<ClientSideAbstractIdentityAttacherComponent<Identity,A> *>(env)->process_incoming_data(
                            basic::ByteData {std::move(std::get<0>(parseRes).content)}
                        );
                        if (processRes) {
                            auto val = basic::bytedata_utils::RunDeserializer<B>::apply(processRes->content);
                            if (!val) {
                                return;
                            }
                            ret->set_value(std::move(*val));
                        }
                    }
                    , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSide<A,B>(env, hooks)
                );
                sendRequestWithIdentity<Identity,A>(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key().content)
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
                    , DefaultHookFactory<Env>::template supplyFacilityHookPair_ClientSideOutgoingOnly<A>(env, hooks)
                );
                sendRequestWithIdentity<Identity,A>(env, requester, basic::ByteDataWithID {
                    Env::id_to_string(keyInput.id())
                    , std::move(keyInput.key().content)
                });
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
        static void wrapOnOrderFacility(
            infra::AppRunner<M> &runner
            , std::shared_ptr<typename M::template OnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
            >> const &toBeWrapped
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            if constexpr(DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                WithIdentity<typename DetermineServerSideIdentityForRequest<Env, A>::IdentityType>
                    ::template wrapOnOrderFacility<A,B>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            } else {
                WithoutIdentity
                    ::template wrapOnOrderFacility<A,B>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            }
        }
        template <class A, class B>
        static void wrapOnOrderFacilityWithoutReply(
            infra::AppRunner<M> &runner
            , std::shared_ptr<typename M::template OnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B
            >> const &toBeWrapped
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            if constexpr(DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                WithIdentity<typename DetermineServerSideIdentityForRequest<Env, A>::IdentityType>
                    ::template wrapOnOrderFacilityWithoutReply<A,B>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            } else {
                WithoutIdentity
                    ::template wrapOnOrderFacilityWithoutReply<A,B>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            }
        }
        template <class A, class B, class C>
        static void wrapLocalOnOrderFacility(
            infra::AppRunner<M> &runner
            , std::shared_ptr<typename M::template LocalOnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B, C
            >> const &toBeWrapped
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            if constexpr(DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                WithIdentity<typename DetermineServerSideIdentityForRequest<Env, A>::IdentityType>
                    ::template wrapLocalOnOrderFacility<A,B,C>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            } else {
                WithoutIdentity
                    ::template wrapLocalOnOrderFacility<A,B,C>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            }
        }
        template <class A, class B, class C>
        static void wrapLocalOnOrderFacilityWithoutReply(
            infra::AppRunner<M> &runner
            , std::shared_ptr<typename M::template LocalOnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B, C
            >> const &toBeWrapped
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            if constexpr(DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                WithIdentity<typename DetermineServerSideIdentityForRequest<Env, A>::IdentityType>
                    ::template wrapLocalOnOrderFacilityWithoutReply<A,B,C>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            } else {
                WithoutIdentity
                    ::template wrapLocalOnOrderFacilityWithoutReply<A,B,C>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            }
        }
        template <class A, class B, class C>
        static void wrapOnOrderFacilityWithExternalEffects(
            infra::AppRunner<M> &runner
            , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B, C
            >> const &toBeWrapped
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            if constexpr(DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                WithIdentity<typename DetermineServerSideIdentityForRequest<Env, A>::IdentityType>
                    ::template wrapOnOrderFacilityWithExternalEffects<A,B,C>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            } else {
                WithoutIdentity
                    ::template wrapOnOrderFacilityWithExternalEffects<A,B,C>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            }
        }
        template <class A, class B, class C>
        static void wrapOnOrderFacilityWithExternalEffectsWithoutReply(
            infra::AppRunner<M> &runner
            , std::shared_ptr<typename M::template OnOrderFacilityWithExternalEffects<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B, C
            >> const &toBeWrapped
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            if constexpr(DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                WithIdentity<typename DetermineServerSideIdentityForRequest<Env, A>::IdentityType>
                    ::template wrapOnOrderFacilityWithExternalEffectsWithoutReply<A,B,C>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            } else {
                WithoutIdentity
                    ::template wrapOnOrderFacilityWithExternalEffectsWithoutReply<A,B,C>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            }
        }
        template <class A, class B, class C, class D>
        static void wrapVIEOnOrderFacility(
            infra::AppRunner<M> &runner
            , std::shared_ptr<typename M::template VIEOnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B, C, D
            >> const &toBeWrapped
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            if constexpr(DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                WithIdentity<typename DetermineServerSideIdentityForRequest<Env, A>::IdentityType>
                    ::template wrapVIEOnOrderFacility<A,B,C,D>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            } else {
                WithoutIdentity
                    ::template wrapVIEOnOrderFacility<A,B,C,D>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            }
        }
        template <class A, class B, class C, class D>
        static void wrapVIEOnOrderFacilityWithoutReply(
            infra::AppRunner<M> &runner
            , std::shared_ptr<typename M::template VIEOnOrderFacility<
                typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType
                , B, C, D
            >> const &toBeWrapped
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) {
            if constexpr(DetermineServerSideIdentityForRequest<Env, A>::HasIdentity) {
                WithIdentity<typename DetermineServerSideIdentityForRequest<Env, A>::IdentityType>
                    ::template wrapVIEOnOrderFacilityWithoutReply<A,B,C,D>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            } else {
                WithoutIdentity
                    ::template wrapVIEOnOrderFacilityWithoutReply<A,B,C,D>(runner, toBeWrapped, rpcQueueLocator, wrapperItemsNamePrefix, hooks);
            }
        }

        template <class A, class B>
        static auto facilityWrapper(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template FacilityWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B> {
            return { std::bind(wrapOnOrderFacility<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
        }
        template <class A, class B>
        static auto facilityWrapperWithoutReply(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template FacilityWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B> {
            return { std::bind(wrapOnOrderFacilityWithoutReply<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
        }
        template <class A, class B, class C>
        static auto localFacilityWrapper(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template LocalFacilityWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B,C> {
            return { std::bind(wrapLocalOnOrderFacility<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
        }
        template <class A, class B, class C>
        static auto localFacilityWrapperWithoutReply(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template LocalFacilityWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B,C> {
            return { std::bind(wrapLocalOnOrderFacilityWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
        }
        template <class A, class B, class C>
        static auto facilityWithExternalEffectsWrapper(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template FacilityWithExternalEffectsWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B,C> {
            return { std::bind(wrapOnOrderFacilityWithExternalEffects<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
        }
        template <class A, class B, class C>
        static auto facilityWithExternalEffectsWrapperWithoutReply(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template FacilityWithExternalEffectsWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B,C> {
            return { std::bind(wrapOnOrderFacilityWithExternalEffectsWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
        }
        template <class A, class B, class C, class D>
        static auto vieFacilityWrapper(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template VIEFacilityWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B,C,D> {
            return { std::bind(wrapVIEOnOrderFacility<A,B,C,D>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
        }
        template <class A, class B, class C, class D>
        static auto vieFacilityWrapperWithoutReply(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
        ) -> typename infra::AppRunner<M>::template VIEFacilityWrapper<typename DetermineServerSideIdentityForRequest<Env, A>::FullRequestType,B,C,D> {
            return { std::bind(wrapVIEOnOrderFacilityWithoutReply<A,B,C,D>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks) };
        }

        template <class A, class B>
        static std::future<B> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            if constexpr(DetermineClientSideIdentityForRequest<Env, A>::HasIdentity) {
                return WithIdentity<typename DetermineClientSideIdentityForRequest<Env, A>::IdentityType>
                    ::template typedOneShotRemoteCall<A,B>(env, rpcQueueLocator, std::move(request), hooks);
            } else {
                return WithoutIdentity
                    ::template typedOneShotRemoteCall<A,B>(env, rpcQueueLocator, std::move(request), hooks);
            }
        }

        template <class A>
        static void typedOneShotRemoteCallNoReply(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            if constexpr(DetermineClientSideIdentityForRequest<Env, A>::HasIdentity) {
                WithIdentity<typename DetermineClientSideIdentityForRequest<Env, A>::IdentityType>
                    ::template typedOneShotRemoteCallNoReply<A>(env, rpcQueueLocator, std::move(request), hooks);
            } else {
                WithoutIdentity
                    ::template typedOneShotRemoteCallNoReply<A>(env, rpcQueueLocator, std::move(request), hooks);
            }
        }

    };

} } } } }

#endif
