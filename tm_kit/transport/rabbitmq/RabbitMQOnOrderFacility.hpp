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
    private:
        struct OnOrderFacilityCommunicationInfo {
            bool isFinal;
        };
        static std::tuple<
            basic::ByteDataWithID
            , std::optional<OnOrderFacilityCommunicationInfo>
            >parseReplyData(std::string const &contentEncoding, basic::ByteDataWithID &&d) {
            if (contentEncoding == "") {
                return {std::move(d), std::nullopt};
            } else if (contentEncoding == "with_final") {
                auto l = d.content.length();
                if (l == 0) {
                    return {std::move(d), {OnOrderFacilityCommunicationInfo {false}}};
                } else {
                    char lastByte = d.content[l-1];
                    d.content.resize(l-1);
                    return {std::move(d), {OnOrderFacilityCommunicationInfo {(lastByte != (char) 0)}}};
                }
            } else {
                return {std::move(d), std::nullopt};
            }
        } 
        static basic::ByteDataWithID createReplyData(std::string const &contentEncoding, basic::ByteDataWithID &&d, OnOrderFacilityCommunicationInfo const &info) {
            if (contentEncoding == "") {
                return std::move(d);
            } else if (contentEncoding == "with_final") {
                auto l = d.content.length();
                d.content.resize(l+1);
                d.content[l] = (info.isFinal?(char) 1:(char) 0);
                return std::move(d);
            } else {
                return std::move(d);
            }
        }
    public:
        using M = infra::RealTimeMonad<Env>;
        using Identity = typename Env::IdentityType;

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
                    requester_ = env->rabbitmq_setRPCQueueClient(locator_, [this](std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                        auto parseRes = parseReplyData(contentEncoding, std::move(data));
                        typename M::template Key<basic::ByteData> reply {Env::id_from_string(std::get<0>(parseRes).id), { std::move(std::get<0>(parseRes).content) }};
                        this->publish(env_, std::move(reply), (std::get<1>(parseRes)?std::get<1>(parseRes)->isFinal:false));
                    }, hooks_);
                }
                virtual void handle(typename M::template InnerData<typename M::template Key<basic::ByteData>> &&data) override final {
                    if (env_) {
                        requester_("", {
                            Env::id_to_string(data.timedData.value.id())
                            , env_->attach_identity(std::move(data.timedData.value.key()), (basic::ByteData *) nullptr).content
                        });
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
                    requester_ = env->rabbitmq_setRPCQueueClient(locator_, [this](std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                        auto parseRes = parseReplyData(contentEncoding, std::move(data));
                        B result;
                        if (!result.ParseFromString(std::move(std::get<0>(parseRes).content))) {
                            return;
                        }
                        this->publish(env_, typename M::template Key<B> {Env::id_from_string(std::get<0>(parseRes).id), std::move(result)}, (std::get<1>(parseRes)?std::get<1>(parseRes)->isFinal:false));
                    }, hooks_);
                }
                virtual void handle(typename M::template InnerData<typename M::template Key<A>> &&data) override final {
                    if (env_) {
                        basic::ByteData s = { basic::SerializationActions<M>::template serializeFunc<A>(
                            data.timedData.value.key()
                        ) };
                        requester_(""
                            , basic::ByteDataWithID {
                                Env::id_to_string(data.timedData.value.id())
                                , env_->attach_identity(std::move(s), (A *) nullptr).content
                            }
                        );
                    }     
                }
            };
            return M::fromAbstractOnOrderFacility(new LocalCore(locator, hooks));
        }
    private:
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
        > createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(ConnectionLocator const &locator, std::optional<ByteDataHookPair> hooks=std::nullopt, std::string const &contentEncoding="") {
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
                std::string contentEncoding_;
            public:
                LocalE(ConnectionLocator const &locator, std::shared_ptr<std::function<void(std::string const &, basic::ByteDataWithID &&)>> const &replierPtr, std::string const &contentEncoding)
                    : locator_(locator), env_(nullptr), replierPtr_(replierPtr), contentEncoding_(contentEncoding)
                {
                }
                virtual void start(Env *env) override final {
                    env_ = env;
                }
                virtual void handle(typename M::template InnerData<basic::ByteDataWithID> &&data) override final {
                    if (env_) {
                        OnOrderFacilityCommunicationInfo info;
                        info.isFinal = data.timedData.finalFlag;
                        auto wireData = createReplyData(contentEncoding_, std::move(data.timedData.value), info);
                        (*replierPtr_)(contentEncoding_, std::move(wireData));
                    }
                }
            };
            auto replierPtr = std::make_shared<std::function<void(std::string const &, basic::ByteDataWithID &&)>>(
                [](std::string const &, basic::ByteDataWithID &&) {}
            );
            return { M::importer(new LocalI(locator, replierPtr, hooks)), M::exporter(new LocalE(locator, replierPtr, contentEncoding)) };
        }
    private:
        template <class A>
        static auto checkIdentityAndDeserialize()
            -> typename infra::MonadRunner<M>::template ActionPtr<basic::ByteDataWithID, typename M::template Key<std::tuple<Identity,A>>>
        {
            return M::template kleisli<basic::ByteDataWithID>(
                [](typename M::template InnerData<basic::ByteDataWithID> &&input) -> typename M::template Data<typename M::template Key<std::tuple<Identity,A>>> {
                    auto checkIdentityRes = input.environment->check_identity(basic::ByteData {std::move(input.timedData.value.content)});
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
        template <class A, class B>
        static void wrapOnOrderFacility(
            infra::MonadRunner<M> &runner
            , std::shared_ptr<typename M::template OnOrderFacility<std::tuple<Identity,A>,B>> const &toBeWrapped
            , ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool encodeFinalFlagInReply = false
        ) {
            auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, hooks, (encodeFinalFlagInReply?"with_final":""));
            auto deserializer = checkIdentityAndDeserialize<A>();
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
            auto deserializer = checkIdentityAndDeserialize<A>();

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
            , bool encodeFinalFlagInReply = false
        ) {
            auto importerExporterPair = createOnOrderFacilityRPCConnectorIncomingAndOutgoingLegs(rpcQueueLocator, hooks, (encodeFinalFlagInReply?"with_final":""));
            auto deserializer = checkIdentityAndDeserialize<A>();
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
            auto deserializer = checkIdentityAndDeserialize<A>();

            runner.registerImporter(importer, wrapperItemsNamePrefix+"_incomingLeg");
            runner.registerAction(deserializer, wrapperItemsNamePrefix+"_deserializer");
            runner.execute(deserializer, runner.importItem(importer));
            runner.placeOrderWithLocalFacilityAndForget(runner.actionAsSource(deserializer), toBeWrapped);
        }

        template <class A, class B>
        static auto facilityWrapper(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool encodeFinalFlagInReply = false
        ) -> typename infra::MonadRunner<M>::template FacilityWrapper<A,B> {
            return { std::bind(wrapOnOrderFacility<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks, encodeFinalFlagInReply) };
        }
        template <class A, class B>
        static auto facilityWrapperWithoutReply(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool encodeFinalFlagInReply = false
        ) -> typename infra::MonadRunner<M>::template FacilityWrapper<A,B> {
            return { std::bind(wrapOnOrderFacilityWithoutReply<A,B>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks, encodeFinalFlagInReply) };
        }
        template <class A, class B, class C>
        static auto localFacilityWrapper(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool encodeFinalFlagInReply = false
        ) -> typename infra::MonadRunner<M>::template LocalFacilityWrapper<A,B,C> {
            return { std::bind(wrapLocalOnOrderFacility<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks, encodeFinalFlagInReply) };
        }
        template <class A, class B, class C>
        static auto localFacilityWrapperWithoutReply(
            ConnectionLocator const &rpcQueueLocator
            , std::string const &wrapperItemsNamePrefix
            , std::optional<ByteDataHookPair> hooks = std::nullopt
            , bool encodeFinalFlagInReply = false
        ) -> typename infra::MonadRunner<M>::template LocalFacilityWrapper<A,B,C> {
            return { std::bind(wrapLocalOnOrderFacilityWithoutReply<A,B,C>, std::placeholders::_1, std::placeholders::_2, rpcQueueLocator, wrapperItemsNamePrefix, hooks, encodeFinalFlagInReply) };
        }

        //The following "oneShot" functions are helper functions that are not intended to go into runner logic
        static std::future<basic::ByteData> oneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, basic::ByteData &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            std::shared_ptr<std::promise<basic::ByteData>> ret = std::make_shared<std::promise<basic::ByteData>>();
            typename M::template Key<std::tuple<Identity, basic::ByteData>> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(request));
            auto requester = env->rabbitmq_setRPCQueueClient(rpcQueueLocator, [ret](std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                auto parseRes = parseReplyData(contentEncoding, std::move(data));
                ret->set_value(std::move(std::get<0>(parseRes).content));
            }, hooks);
            requester("", {
                Env::id_to_string(keyInput.id())
                , env->attach_identity(std::move(keyInput.key()), (basic::ByteData *) nullptr).content
            });
            return ret->get_future();
        }

        template <class A, class B>
        static std::future<B> typedOneShotRemoteCall(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            std::shared_ptr<std::promise<B>> ret = std::make_shared<std::promise<B>>();
            basic::ByteData byteData = { basic::SerializationActions<M>::template serializeFunc<A>(request) };
            typename M::template Key<basic::ByteData> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(byteData));
            
            auto requester = env->rabbitmq_setRPCQueueClient(rpcQueueLocator, [ret](std::string const &contentEncoding, basic::ByteDataWithID &&data) {
                auto parseRes = parseReplyData(contentEncoding, std::move(data));
                B val;
                if (val.ParseFromString(std::get<0>(parseRes).content)) {
                    ret->set_value(std::move(val));
                }
            }, hooks);
            requester("", {
                Env::id_to_string(keyInput.id())
                , env->attach_identity(std::move(keyInput.key()), (A *) nullptr).content
            });
            return ret->get_future();
        }

        static void oneShotRemoteCallNoReply(Env *env, ConnectionLocator const &rpcQueueLocator, basic::ByteData &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            typename M::template Key<std::tuple<Identity, basic::ByteData>> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(request));
            auto requester = env->rabbitmq_setRPCQueueClient(rpcQueueLocator, [](std::string const &contentEncoding, basic::ByteDataWithID &&data) {
            }, hooks);
            requester("", {
                Env::id_to_string(keyInput.id())
                , env->attach_identity(std::move(keyInput.key()), (basic::ByteData *) nullptr).content
            });
        }

        template <class A>
        static void typedOneShotRemoteCallNoReply(Env *env, ConnectionLocator const &rpcQueueLocator, A &&request, std::optional<ByteDataHookPair> hooks = std::nullopt) {
            basic::ByteData byteData = { basic::SerializationActions<M>::template serializeFunc<A>(request) };
            typename M::template Key<basic::ByteData> keyInput = infra::withtime_utils::keyify<basic::ByteData,typename M::EnvironmentType>(std::move(byteData));
            
            auto requester = env->rabbitmq_setRPCQueueClient(rpcQueueLocator, [](std::string const &contentEncoding, basic::ByteDataWithID &&data) {
            }, hooks);
            requester("", {
                Env::id_to_string(keyInput.id())
                , env->attach_identity(std::move(keyInput.key()), (A *) nullptr).content
            });
        }
    };

} } } } }

#endif
