#ifndef TM_KIT_TRANSPORT_RABBITMQ_RABBITMQ_ON_ORDER_FACILITY_HPP_
#define TM_KIT_TRANSPORT_RABBITMQ_RABBITMQ_ON_ORDER_FACILITY_HPP_

#include <type_traits>
#include <mutex>
#include <unordered_map>
#include <future>

#include <tm_kit/infra/RealTimeMonad.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace rabbitmq {

    template <class Env, std::enable_if_t<std::is_base_of_v<RabbitMQComponent, Env>, int> = 0>
    class RabbitMQOnOrderFacility {
    public:
        using M = infra::RealTimeMonad<Env>;

        //for the pairs of hooks, if present, the first one is always in the direction of "input to facility",
        //and the second one is always in the direction of "output from facility"
        static typename M::template OnOrderFacility<basic::ByteData, basic::ByteData> createRPCOnOrderFacility(
            ConnectionLocator const &locator
            , std::optional<ByteDataHookPair> hooks = std::nullopt) { 
            class LocalCore final : public virtual infra::RealTimeMonadComponents<Env>::IExternalComponent, public virtual infra::RealTimeMonadComponents<Env>::template AbstractOnOrderFacility<basic::ByteData, basic::ByteData> {
            private:
                Env *env_;
                ConnectionLocator locator_;
                std::function<void(std::string const &, basic::ByteDataWithID &&)> requester_;
                std::optional<ByteDataHookPair> hooks_;
            public:
                LocalCore(ConnectionLocator const &locator, std::optional<ByteDataHookPair> hooks) : env_(nullptr), locator_(locator), hooks_(hooks) {}
                virtual void start(Env *env) override final {
                    env_ = env;
                    requester_ = env->rabbitmq_setRPCQueueClient(locator_, [this](std::string const &, basic::ByteDataWithID &&data) {
                        typename M::template Key<basic::ByteData> reply {Env::id_from_string(data.id), { std::move(data.content) }};
                        this->publish(env_, std::move(reply), false);
                    }, hooks_);
                }
                virtual void handle(typename M::template InnerData<typename M::template Key<basic::ByteData>> &&data) override final {
                    if (env_) {
                        requester_("", {Env::id_to_string(data.timedData.value.id()), std::move(data.timedData.value.key())});
                    }     
                }
            };
            return M::fromAbstractOnOrderFacility(new LocalCore(locator, hooks));
        }
        template <class A, class B>
        static std::shared_ptr<typename M::template OnOrderFacility<A, B>> createTypedRPCOnOrderFacility(
            ConnectionLocator const &locator
            , std::optional<ByteDataHookPair> hooks = std::nullopt) {
            class LocalCore final : public virtual infra::RealTimeMonadComponents<Env>::IExternalComponent, public virtual infra::RealTimeMonadComponents<Env>::template AbstractOnOrderFacility<A,B> {
            private:
                Env *env_;
                ConnectionLocator locator_;
                std::function<void(std::string const &, basic::ByteDataWithID &&)> requester_;
                std::optional<ByteDataHookPair> hooks_;
            public:
                LocalCore(ConnectionLocator const &locator, std::optional<ByteDataHookPair> hooks) : env_(nullptr), locator_(locator), hooks_(hooks) {}
                virtual void start(Env *env) override final {
                    env_ = env;
                    requester_ = env->rabbitmq_setRPCQueueClient(locator_, [this](std::string const &, basic::ByteDataWithID &&data) {
                        B result;
                        if (!result.ParseFromString(std::move(data.content))) {
                            return;
                        }
                        this->publish(env_, typename M::template Key<B> {Env::id_from_string(data.id), std::move(result)}, false);
                    }, hooks_);
                }
                virtual void handle(typename M::template InnerData<typename M::template Key<A>> &&data) override final {
                    if (env_) {
                        std::string s;
                        data.timedData.value.key().SerializeToString(&s);
                        requester_("", {Env::id_to_string(data.timedData.value.id()), std::move(s)});
                    }     
                }
            };
            return M::fromAbstractOnOrderFacility(new LocalCore(locator, hooks));
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
                    env->rabbitmq_setRPCQueueServer(
                        locator_
                        , [this,env](std::string const &, basic::ByteDataWithID &&d) {
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
                std::shared_ptr<std::function<void(std::string const &, basic::ByteDataWithID &&)>> replierPtr_;
                std::optional<ByteDataHookPair> hooks_;
            public:
                LocalI(ConnectionLocator const &locator, std::shared_ptr<std::function<void(std::string const &, basic::ByteDataWithID &&)>> const & replierPtr, std::optional<ByteDataHookPair> hooks)
                    : locator_(locator), replierPtr_(replierPtr), hooks_(hooks)
                {
                }
                virtual void start(Env *env) override final {
                    *replierPtr_ = env->rabbitmq_setRPCQueueServer(
                        locator_
                        , [this,env](std::string const &, basic::ByteDataWithID &&d) {
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
                std::shared_ptr<std::function<void(std::string const &, basic::ByteDataWithID &&)>> replierPtr_;
            public:
                LocalE(ConnectionLocator const &locator, std::shared_ptr<std::function<void(std::string const &, basic::ByteDataWithID &&)>> const &replierPtr)
                    : locator_(locator), env_(nullptr), replierPtr_(replierPtr)
                {
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                }
                virtual void handle(typename M::template InnerData<basic::ByteDataWithID> &&data) override final {
                    if (env_) {
                        (*replierPtr_)("", std::move(data.timedData.value));
                    }
                }
            };
            auto replierPtr = std::make_shared<std::function<void(std::string const &, basic::ByteDataWithID &&)>>(
                [](std::string const &, basic::ByteDataWithID &&) {}
            );
            return { M::importer(new LocalI(locator, replierPtr, hooks)), M::exporter(new LocalE(locator, replierPtr)) };
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
            auto translator1 = basic::SerializationActions<M>::template deserializeWithKey<A>();
            auto translator2 = basic::SerializationActions<M>::template serializeWithKey<A,B>();

            runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
            runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
            runner.registerAction(translator1, wrapperItemsNamePrefix+"_deserializer");
            runner.registerAction(translator2, wrapperItemsNamePrefix+"_serializer");
            runner.execute(translator1, runner.importItem(std::get<0>(importerExporterPair)));
            runner.placeOrderWithFacility(runner.actionAsSource(translator1), toBeWrapped, runner.actionAsSink(translator2));
            runner.connect(runner.actionAsSource(translator2), runner.exporterAsSink(std::get<1>(importerExporterPair)));
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
            auto translator = basic::SerializationActions<M>::template deserializeWithKey<A>();

            runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
            runner.registerAction(translator, wrapperItemsNamePrefix+"_deserializer");
            runner.execute(translator, runner.importItem(importer));
            runner.placeOrderWithFacilityAndForget(runner.actionAsSource(translator), toBeWrapped);
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
            auto translator1 = basic::SerializationActions<M>::template deserializeWithKey<A>();
            auto translator2 = basic::SerializationActions<M>::template serializeWithKey<A,B>();

            runner.registerImporter(std::get<0>(importerExporterPair), wrapperItemsNamePrefix+"_incomingLeg");
            runner.registerExporter(std::get<1>(importerExporterPair), wrapperItemsNamePrefix+"_outgoingLeg");
            runner.registerAction(translator1, wrapperItemsNamePrefix+"_deserializer");
            runner.registerAction(translator2, wrapperItemsNamePrefix+"_serializer");
            runner.execute(translator1, runner.importItem(std::get<0>(importerExporterPair)));
            runner.placeOrderWithLocalFacility(runner.actionAsSource(translator1), toBeWrapped, runner.actionAsSink(translator2));
            runner.connect(runner.actionAsSource(translator2), runner.exporterAsSink(std::get<1>(importerExporterPair)));
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
            auto translator = basic::SerializationActions<M>::template deserializeWithKey<A>();

            runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
            runner.registerAction(translator, wrapperItemsNamePrefix+"_deserializer");
            runner.execute(translator, runner.importItem(importer));
            runner.placeOrderWithLocalFacilityAndForget(runner.actionAsSource(translator), toBeWrapped);
        }

        //The following "oneShot" functions are helper functions that are not intended to go into runner logic
        static std::future<basic::ByteDataWithID> oneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, basic::ByteDataWithID &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            std::shared_ptr<std::promise<basic::ByteDataWithID>> ret = std::make_shared<std::promise<basic::ByteDataWithID>>();
            auto requester = env->rabbitmq_setRPCQueueClient(rpcQueueLocator, [ret](std::string const &, basic::ByteDataWithID &&data) {
                ret->set_value(std::move(data));
            }, hooks);
            requester("", std::move(request));
            return ret->get_future();
        }

        template <class A, class B>
        static std::future<B> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            std::shared_ptr<std::promise<B>> ret = std::make_shared<std::promise<B>>();
            typename M::template Key<A> keyInput = infra::withtime_utils::keyify<A,typename M::EnvironmentType>(std::move(request));
            std::string byteData;
            keyInput.key().SerializeToString(&byteData);
            
            auto requester = env->rabbitmq_setRPCQueueClient(rpcQueueLocator, [ret](std::string const &, basic::ByteDataWithID &&data) {
                B val;
                if (val.ParseFromString(data.content)) {
                    ret->set_value(std::move(val));
                }
            }, hooks);
            requester("", basic::ByteDataWithID {Env::id_to_string(keyInput.id()), std::move(byteData)});
            return ret->get_future();
        }

        static void oneShotRemoteCallNoReply(Env *env, ConnectionLocator const &rpcQueueLocator, basic::ByteDataWithID &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            auto requester = env->rabbitmq_setRPCQueueClient(rpcQueueLocator, [](std::string const &, basic::ByteDataWithID &&) {
            }, hooks);
            requester("", std::move(request));
        }

        template <class A>
        static void typedOneShotRemoteCallNoReply(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            typename M::template Key<A> keyInput = infra::withtime_utils::keyify<A,typename M::EnvironmentType>(std::move(request));
            std::string byteData;
            keyInput.key().SerializeToString(&byteData);
            
            auto requester = env->rabbitmq_setRPCQueueClient(rpcQueueLocator, [](std::string const &, basic::ByteDataWithID &&) {
            }, hooks);
            requester("", basic::ByteDataWithID {Env::id_to_string(keyInput.id()), std::move(byteData)});
        }
    };

} } } } }

#endif
