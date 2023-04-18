#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_SHARED_CHAIN_CREATOR_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_SHARED_CHAIN_CREATOR_HPP_

#include <tm_kit/infra/Environments.hpp>

#include <tm_kit/basic/simple_shared_chain/ChainReader.hpp>
#include <tm_kit/basic/simple_shared_chain/ChainWriter.hpp>
#include <tm_kit/basic/simple_shared_chain/OneShotChainWriter.hpp>
#include <tm_kit/basic/simple_shared_chain/InMemoryWithLockChain.hpp>
#include <tm_kit/basic/simple_shared_chain/InMemoryLockFreeChain.hpp>
#include <tm_kit/basic/simple_shared_chain/EmptyChain.hpp>

#include <tm_kit/transport/etcd_shared_chain/EtcdChain.hpp>
#include <tm_kit/transport/redis_shared_chain/RedisChain.hpp>
#include <tm_kit/transport/lock_free_in_memory_shared_chain/LockFreeInBoostSharedMemoryChain.hpp>

#include <tm_kit/transport/ConnectionLocator.hpp>
#include <tm_kit/transport/AbstractHookFactoryComponent.hpp>

#include <unordered_map>
#include <mutex>

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    using AllChainComponents = infra::Environment<
        infra::HiddenTimeDependencyComponent
        , etcd_shared_chain::EtcdChainComponent
        , redis_shared_chain::RedisChainComponent
        , lock_free_in_memory_shared_chain::SharedMemoryChainComponent
    >;

    enum class SharedChainProtocol {
        Etcd 
        , Redis 
        , InMemoryWithLock
        , InMemoryLockFree
        , InSharedMemoryWithLock
        , InSharedMemoryLockFree
        , Empty
    };
    inline const std::array<std::string,7> SHARED_CHAIN_PROTOCOL_STR = {
        "etcd"
        , "redis"
        , "in_memory_with_lock"
        , "in_memory_lock_free"
        , "in_shared_memory_with_lock"
        , "in_shared_memory_lock_free"
        , "empty"
    };

    namespace shared_chain_utils {
        inline auto parseSharedChainLocator(std::string const &s)
            -> std::optional<std::tuple<SharedChainProtocol, ConnectionLocator>>
        {
            size_t ii = 0;
            for (auto const &item : SHARED_CHAIN_PROTOCOL_STR) {
                if (boost::starts_with(s, item+"://")) {
                    try {
                        auto locator = ConnectionLocator::parse(s.substr(item.length()+3));
                        return std::tuple<SharedChainProtocol, ConnectionLocator> {
                            static_cast<SharedChainProtocol>(ii)
                            , locator
                        };
                    } catch (ConnectionLocatorParseError const &) {
                        return std::nullopt;
                    }
                }
                ++ii;
            }
            if (boost::starts_with(s, "in_shared_memory://")) {
                try {
                    auto locator = ConnectionLocator::parse(s.substr(std::string("in_shared_memory").length()+3));
                    return std::tuple<SharedChainProtocol, ConnectionLocator> {
                        SharedChainProtocol::InSharedMemoryLockFree
                        , locator
                    };
                } catch (ConnectionLocatorParseError const &) {
                    return std::nullopt;
                }
            }
#ifndef _MSC_VER
            if (boost::starts_with(s, "/dev/shm/")) {
                try {
                    auto locator = ConnectionLocator::parse("::::"+s.substr(std::string("/dev/shm/").length()));                   
                    return std::tuple<SharedChainProtocol, ConnectionLocator> {
                        SharedChainProtocol::InSharedMemoryLockFree
                        , locator
                    };
                } catch (ConnectionLocatorParseError const &) {
                    return std::nullopt;
                }
            }
#endif
            return std::nullopt;
        }

        inline std::string makeSharedChainLocator(SharedChainProtocol protocol, ConnectionLocator const &locator) {
            return SHARED_CHAIN_PROTOCOL_STR[static_cast<int>(protocol)] + "://" + locator.toSerializationFormat();
        }

        inline etcd_shared_chain::EtcdChainConfiguration etcdChainConfigurationFromConnectionLocator(ConnectionLocator const &l) {
            return etcd_shared_chain::EtcdChainConfiguration()
                .InsecureEtcdServerAddr(l.host()+":"+std::to_string(l.port()))
                .HeadKey(l.query("headKey", "head"))
                .SaveDataOnSeparateStorage(l.query("saveDataOnSeparateStorage","true") == "true")
                .UseWatchThread(l.query("useWatchThread","false") == "true")
                .ChainPrefix(l.identifier())
                .DataPrefix(l.query("dataPrefix", l.identifier()+"_data"))
                .ExtraDataPrefix(l.query("extraDataPrefix", l.identifier()+"_extra_data"))
                .RedisServerAddr(l.query("redisServerAddr", ""))
                .DuplicateFromRedis(l.query("redisServerAddr","") != "")
                .RedisTTLSeconds(static_cast<uint16_t>(std::stoul(l.query("redisTTLSeconds", "0"))))
                .AutomaticallyDuplicateToRedis(l.query("automaticallyDuplicateToRedis","false") == "true")
                ;
        }
        inline redis_shared_chain::RedisChainConfiguration redisChainConfigurationFromConnectionLocator(ConnectionLocator const &l) {
            return redis_shared_chain::RedisChainConfiguration()
                .RedisServerAddr(l.host()+":"+std::to_string(l.port()))
                .HeadKey(l.query("headKey", "head"))
                .ChainPrefix(l.identifier())
                .DataPrefix(l.query("dataPrefix", l.identifier()+"_data"))
                .ExtraDataPrefix(l.query("extraDataPrefix", l.identifier()+"_extra_data"))
                ;
        }

        template <class App, class ChainItemFolder, class TriggerT, class ResultTransformer>
        inline auto ImporterOrActionTypeHelper() {
            if constexpr (std::is_same_v<TriggerT, void>) {
                return std::shared_ptr<typename App::template Importer<std::conditional_t<std::is_same_v<ResultTransformer, void>, typename ChainItemFolder::ResultType, typename basic::simple_shared_chain::ResultTypeExtractor<ResultTransformer>::TheType>>>();
            } else {
                return std::shared_ptr<typename App::template Action<TriggerT, std::conditional_t<std::is_same_v<ResultTransformer, void>, typename ChainItemFolder::ResultType, typename basic::simple_shared_chain::ResultTypeExtractor<ResultTransformer>::TheType>>>();
            }
        }
        template <class App, class ChainItemFolder, class TriggerT, class ResultTransformer>
        using ImporterOrAction = decltype(
            ImporterOrActionTypeHelper<App,ChainItemFolder,TriggerT,ResultTransformer>()
        );
        
        template <class App, class ChainItemFolder, class TriggerT, class ResultTransformer, class Chain>
        inline auto chainReaderHelper(
            typename App::EnvironmentType *env
            , Chain *chain 
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy
            , ChainItemFolder &&folder
            , std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer> &&resultTransformer = std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer>()
        ) 
            -> ImporterOrAction<App,ChainItemFolder,TriggerT,ResultTransformer>
        {
            if constexpr (std::is_same_v<TriggerT, void>) {
                return basic::simple_shared_chain::ChainReader<
                    App
                    , Chain
                    , ChainItemFolder
                    , void
                    , ResultTransformer
                >::importer(
                    chain
                    , pollingPolicy
                    , std::move(folder)
                    , std::move(resultTransformer)
                );
            } else {
                return basic::simple_shared_chain::ChainReader<
                    App
                    , Chain
                    , ChainItemFolder
                    , TriggerT
                    , ResultTransformer
                >::action(
                    env
                    , chain
                    , std::move(folder)
                    , std::move(resultTransformer)
                );
            }
        }

        template <class App, class ChainItemFolder, class InputHandler, class IdleLogic, class Chain>
        inline auto chainWriterHelper(
            Chain *chain 
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy
            , ChainItemFolder &&folder
            , InputHandler &&inputHandler
            , std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            > &&idleLogic
        ) 
            -> std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , std::shared_ptr<typename App::template OnOrderFacility<typename InputHandler::InputType, typename InputHandler::ResponseType>>
                , std::shared_ptr<typename App::template OnOrderFacilityWithExternalEffects<typename InputHandler::InputType, typename InputHandler::ResponseType, typename basic::simple_shared_chain::OffChainUpdateTypeExtractor<IdleLogic>::T>> 
            >
        {
            if constexpr (std::is_same_v<IdleLogic, void>) {
                return basic::simple_shared_chain::ChainWriter<
                    App
                    , Chain
                    , ChainItemFolder
                    , InputHandler
                    , void
                >::onOrderFacility(
                    chain
                    , pollingPolicy
                    , std::move(folder)
                    , std::move(inputHandler)
                    , std::move(idleLogic)
                );
            } else {
                return basic::simple_shared_chain::ChainWriter<
                    App
                    , Chain
                    , ChainItemFolder
                    , InputHandler
                    , IdleLogic
                >::onOrderFacilityWithExternalEffects(
                    chain
                    , pollingPolicy
                    , std::move(folder)
                    , std::move(inputHandler)
                    , std::move(idleLogic)
                );
            }
        }

        template <class ExtraData, class Chain>
        inline auto readExtraDataHelper(
            Chain *chain 
            , std::string const &key
        ) -> std::optional<ExtraData> {
            if constexpr (Chain::SupportsExtraData) {
                return chain->template loadExtraData<ExtraData>(key);
            } else {
                return std::nullopt;
            }
        }

        template <class ExtraData, class Chain>
        inline void writeExtraDataHelper(
            Chain *chain 
            , std::string const &key
            , ExtraData const &data
        ) {
            if constexpr (Chain::SupportsExtraData) {
                chain->template saveExtraData<ExtraData>(key, data);
            }
        }
    }

    template <class App>
    class SharedChainCreator {
    private:
        std::mutex mutex_;
        std::unordered_map<std::string, std::tuple<void *, std::function<void(void *)>>> chains_;

        template <class T>
        T *getChain(std::string const &name, std::function<T *()> creator) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = chains_.find(name);
            if (iter == chains_.end()) {
                T *chain = creator();
                chains_.insert({name, {(void *) chain, [](void *p) {delete ((T *) p);}}});
                return chain;
            } else {
                return (T *) std::get<0>(iter->second);
            }
        }
        template <class ChainData>
        etcd_shared_chain::EtcdChain<ChainData> *getEtcdChain(
            typename App::EnvironmentType *env
            , std::string const &name
            , std::optional<ByteDataHookPair> hookPair
            , etcd_shared_chain::EtcdChainConfiguration const &conf
        ) {
            return getChain<etcd_shared_chain::EtcdChain<ChainData>>(
                name
                , [env,hookPair,&conf]() {
                    return new etcd_shared_chain::EtcdChain<ChainData>(
                        conf
                        , DefaultHookFactory<typename App::EnvironmentType>::template supplyFacilityHookPair_SingleType<ChainData>(
                            env, hookPair
                        )
                    );
                }
            );
        }
        template <class ChainData>
        redis_shared_chain::RedisChain<ChainData> *getRedisChain(
            typename App::EnvironmentType *env
            , std::string const &name
            , std::optional<ByteDataHookPair> hookPair
            , redis_shared_chain::RedisChainConfiguration const &conf
        ) {
            return getChain<redis_shared_chain::RedisChain<ChainData>>(
                name
                , [env,hookPair,&conf]() {
                    return new redis_shared_chain::RedisChain<ChainData>(
                        conf
                        , DefaultHookFactory<typename App::EnvironmentType>::template supplyFacilityHookPair_SingleType<ChainData>(
                            env, hookPair
                        )
                    );
                }
            );
        }
        template <class T>
        T *getInMemoryChain(
            typename App::EnvironmentType *env
            , std::string const &name
        ) {
            return getChain<T>(
                name
                , []() {
                    return new T();
                }
            );
        }
        template <class T>
        T *getInSharedMemChain(
            typename App::EnvironmentType *env
            , std::string const &name
            , std::optional<ByteDataHookPair> hookPair
            , std::string const &memoryName
            , std::size_t memorySize
            , bool useNotification
        ) {
            return getChain<T>(
                name 
                , [env,hookPair,memoryName,memorySize,useNotification]() {
                    return new T(memoryName, memorySize, DefaultHookFactory<typename App::EnvironmentType>::template supplyFacilityHookPair_SingleType<typename T::DataType>(
                        env, hookPair
                    ), useNotification);
                }
            );
        }
        
        template <class ChainData, bool ForceSeparateDataStorageIfPossible, class Action>
        typename Action::Result dispatch(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , std::optional<ByteDataHookPair> hookPair
            , Action &&action
        )
        {
            switch (protocol) {
            case SharedChainProtocol::Etcd:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, etcd_shared_chain::EtcdChainComponent *>) {
                        auto conf = shared_chain_utils::etcdChainConfigurationFromConnectionLocator(locator);
                        if constexpr (ForceSeparateDataStorageIfPossible) {
                            conf.SaveDataOnSeparateStorage(true);
                        }
                        auto *chain = getEtcdChain<ChainData>(
                            env 
                            , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , hookPair
                            , conf
                        );
                        if constexpr (std::is_same_v<typename Action::Result, void>) {
                            std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                        } else {
                            return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                        }
                    } else {
                        throw new std::runtime_error("sharedChainCreator::dispatch: Etcd chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::Redis:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, redis_shared_chain::RedisChainComponent *>) {
                        auto conf = shared_chain_utils::redisChainConfigurationFromConnectionLocator(locator);
                        auto *chain = getRedisChain<ChainData>(
                            env 
                            , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , hookPair
                            , conf
                        );
                        if constexpr (std::is_same_v<typename Action::Result, void>) {
                            std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                        } else {
                            return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                        }
                    } else {
                        throw new std::runtime_error("sharedChainCreator::dispatch: Redis chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::InMemoryWithLock:
                {
                    auto *chain = getInMemoryChain<basic::simple_shared_chain::InMemoryWithLockChain<ChainData>>(
                        env 
                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                    );
                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                    } else {
                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                    }
                }
                break;
            case SharedChainProtocol::InMemoryLockFree:
                {
                    auto *chain = getInMemoryChain<basic::simple_shared_chain::InMemoryLockFreeChain<ChainData>>(
                        env 
                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                    );
                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                    } else {
                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                    }
                }
                break;
            case SharedChainProtocol::InSharedMemoryWithLock:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, lock_free_in_memory_shared_chain::SharedMemoryChainComponent *>) {
                        auto memoryName = locator.identifier();
                        std::size_t memorySize = static_cast<std::size_t>(4*1024LL*1024LL*1024LL);
                        auto sizeStr = locator.query("size","");
                        if (sizeStr != "") {
                            memorySize = static_cast<std::size_t>(std::stoull(sizeStr));
                        }
                        bool useName = (locator.query("useName", "false") == "true");                       
                        bool dataLockIsNamedMutex = (locator.query("dataLock", "named") == "named");
                        std::string extraDataLockType = locator.query("extraDataLock", "named");
                        bool useNotification = (locator.query("useNotification", "false") == "true");
                        if (useName) {
                            if (dataLockIsNamedMutex) {
                                if (extraDataLockType == "spin") {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::NamedMutex
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                } else if (extraDataLockType == "none") {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::NamedMutex
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                } else {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::NamedMutex
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                }
                            } else {
                                if (extraDataLockType == "spin") {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::SpinLock
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                } else if (extraDataLockType == "none") {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::SpinLock
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                } else {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::SpinLock
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                }
                            }
                        } else {
                            if (dataLockIsNamedMutex) {
                                if (extraDataLockType == "spin") {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::NamedMutex
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                } else if (extraDataLockType == "none") {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::NamedMutex
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                } else {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::NamedMutex
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                }
                            } else {
                                if (extraDataLockType == "spin") {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::SpinLockProtected
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::SpinLock
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                } else if (extraDataLockType == "none") {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::SpinLock
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                } else {
                                    using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                        ChainData
                                        , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                        )
                                        , (ForceSeparateDataStorageIfPossible || (
                                            DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                            && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                        ))
                                        , (
                                            App::PossiblyMultiThreaded
                                            ?
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::SpinLock
                                            :
                                            lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                                        )
                                    >;
                                    auto *chain = getInSharedMemChain<C>(
                                        env 
                                        , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                        , hookPair
                                        , memoryName
                                        , memorySize
                                        , useNotification
                                    );
                                    if constexpr (std::is_same_v<typename Action::Result, void>) {
                                        std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    } else {
                                        return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                                    }
                                }
                            }
                        }
                    } else {
                        throw new std::runtime_error("sharedChainCreator::dispatch: Shared memory chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::InSharedMemoryLockFree:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, lock_free_in_memory_shared_chain::SharedMemoryChainComponent *>) {
                        auto memoryName = locator.identifier();
                        std::size_t memorySize = static_cast<std::size_t>(4*1024LL*1024LL*1024LL);
                        auto sizeStr = locator.query("size","");
                        if (sizeStr != "") {
                            memorySize = static_cast<std::size_t>(std::stoull(sizeStr));
                        }
                        bool useName = (locator.query("useName", "false") == "true");  
                        bool useNotification = (locator.query("useNotification", "false") == "true");                     
                        if (useName) {
                            using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                ChainData
                                , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                , (
                                    App::PossiblyMultiThreaded
                                    ?
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory
                                    :
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                )
                                , (ForceSeparateDataStorageIfPossible || (
                                    DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                    && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                ))
                                , lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                            >;
                            auto *chain = getInSharedMemChain<C>(
                                env 
                                , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , hookPair
                                , memoryName
                                , memorySize
                                , useNotification
                            );
                            if constexpr (std::is_same_v<typename Action::Result, void>) {
                                std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                            } else {
                                return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                            }
                        } else {
                            using C = lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                ChainData
                                , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                , (
                                    App::PossiblyMultiThreaded
                                    ?
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::LockFreeAndWasteMemory
                                    :
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                )
                                , (ForceSeparateDataStorageIfPossible || (
                                    DefaultHookFactory<typename App::EnvironmentType>::template HasOutgoingHookFactory<ChainData>()
                                    && DefaultHookFactory<typename App::EnvironmentType>::template HasIncomingHookFactory<ChainData>()
                                ))
                                , lock_free_in_memory_shared_chain::BoostSharedMemoryChainDataLockStrategy::None
                            >;
                            auto *chain = getInSharedMemChain<C>(
                                env 
                                , shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , hookPair
                                , memoryName
                                , memorySize
                                , useNotification
                            );
                            if constexpr (std::is_same_v<typename Action::Result, void>) {
                                std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                            } else {
                                return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                            }
                        }
                    } else {
                        throw new std::runtime_error("sharedChainCreator::dispatch: Shared memory chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::Empty:
                {
                    bool supportsExtraData = (locator.query("supprtsExtraData", "false") == "true");
                    if (supportsExtraData) {
                        auto *chain = getChain<basic::simple_shared_chain::EmptyChain<ChainData,true>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new basic::simple_shared_chain::EmptyChain<ChainData,true>();
                            }
                        );
                        if constexpr (std::is_same_v<typename Action::Result, void>) {
                            std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                        } else {
                            return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                        }
                    } else {
                        auto *chain = getChain<basic::simple_shared_chain::EmptyChain<ChainData,false>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new basic::simple_shared_chain::EmptyChain<ChainData,false>();
                            }
                        );
                        if constexpr (std::is_same_v<typename Action::Result, void>) {
                            std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                        } else {
                            return std::move(action).template invoke<std::remove_pointer_t<decltype(chain)>>(env, chain);
                        }
                    }
                }
                break;
            default:
                throw std::runtime_error(std::string("sharedChainCreator::dispatch: unknown protocol")+std::to_string(static_cast<int>(protocol)));
                break;
            }
        }
    public:
        SharedChainCreator() : mutex_(), chains_() {}
        ~SharedChainCreator() {
            std::lock_guard<std::mutex> _(mutex_);
            for (auto &item : chains_) {
                std::get<1>(item.second)(std::get<0>(item.second));
            }
            chains_.clear();
        }

    private:
        template <class ChainItemFolder, class TriggerT, class ResultTransformer>
        class ReaderAction {
        private:
            basic::simple_shared_chain::ChainPollingPolicy pollingPolicy_;
            ChainItemFolder folder_;
            std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer> resultTransformer_;
        public:
            using Result = shared_chain_utils::ImporterOrAction<App,ChainItemFolder,TriggerT,ResultTransformer>;
            ReaderAction(
                basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy
                , ChainItemFolder &&folder
                , std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer> &&resultTransformer
            ) 
            : pollingPolicy_(pollingPolicy)
            , folder_(std::move(folder))
            , resultTransformer_(std::move(resultTransformer))
            {}

            template <class Chain>
            Result invoke(typename App::EnvironmentType *env, Chain *chain) && {
                return shared_chain_utils::chainReaderHelper<App,ChainItemFolder,TriggerT,ResultTransformer>(
                    env
                    , chain
                    , pollingPolicy_
                    , std::move(folder_)
                    , std::move(resultTransformer_)
                );
            }
        };

    public:
        template <class ChainData, class ChainItemFolder, class TriggerT=void, class ResultTransformer=void, bool ForceSeparateDataStorageIfPossible=false>
        auto reader(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
            , ChainItemFolder &&folder = ChainItemFolder {}
            , std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer> &&resultTransformer = std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer>()
        )
            -> typename ReaderAction<
                ChainItemFolder, TriggerT, ResultTransformer
            >::Result
        {
            return dispatch<ChainData, ForceSeparateDataStorageIfPossible, ReaderAction<
                    ChainItemFolder, TriggerT, ResultTransformer
                >>(
                env, protocol, locator, hookPair, ReaderAction<
                    ChainItemFolder, TriggerT, ResultTransformer
                >(pollingPolicy, std::move(folder), std::move(resultTransformer))
            );
        }

        template <class ChainData, class ChainItemFolder, class TriggerT=void, class ResultTransformer=void, bool ForceSeparateDataStorageIfPossible=false>
        auto reader(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
            , ChainItemFolder &&folder = ChainItemFolder {}
            , std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer> &&resultTransformer = std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer>()
        )
            -> shared_chain_utils::ImporterOrAction<App,ChainItemFolder,TriggerT,ResultTransformer>
        {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                return reader<ChainData,ChainItemFolder,TriggerT,ResultTransformer,ForceSeparateDataStorageIfPossible>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), pollingPolicy, hookPair, std::move(folder),std::move(resultTransformer)
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::reader: malformed connection locator string '")+locatorStr+"'");
            }
        }

    private:
        template <class ChainItemFolder, class InputHandler, class IdleLogic>
        class WriterAction {
        private:
            basic::simple_shared_chain::ChainPollingPolicy pollingPolicy_;
            ChainItemFolder folder_;
            InputHandler inputHandler_;
            std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            > idleLogic_;
        public:
            using Result = std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , std::shared_ptr<typename App::template OnOrderFacility<typename InputHandler::InputType, typename InputHandler::ResponseType>>
                , std::shared_ptr<typename App::template OnOrderFacilityWithExternalEffects<typename InputHandler::InputType, typename InputHandler::ResponseType, typename basic::simple_shared_chain::OffChainUpdateTypeExtractor<IdleLogic>::T>> 
            >;
            WriterAction(
                basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy
                , ChainItemFolder &&folder
                , InputHandler &&inputHandler
                , std::conditional_t<
                    std::is_same_v<IdleLogic, void>
                    , basic::VoidStruct
                    , IdleLogic
                > &&idleLogic
            ) 
            : pollingPolicy_(pollingPolicy)
            , folder_(std::move(folder))
            , inputHandler_(std::move(inputHandler))
            , idleLogic_(std::move(idleLogic))
            {}

            template <class Chain>
            Result invoke(typename App::EnvironmentType *env, Chain *chain) && {
                return shared_chain_utils::chainWriterHelper<App,ChainItemFolder,InputHandler,IdleLogic>(
                    chain
                    , pollingPolicy_
                    , std::move(folder_)
                    , std::move(inputHandler_)
                    , std::move(idleLogic_)
                );
            }
        };

    public:
        template <class ChainData, class ChainItemFolder, class InputHandler, class IdleLogic=void, bool ForceSeparateDataStorageIfPossible=false>
        auto writer(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
            , ChainItemFolder &&folder = ChainItemFolder {}
            , InputHandler &&inputHandler = InputHandler()
            , std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            > &&idleLogic = std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            >()
        )
            -> typename WriterAction<ChainItemFolder, InputHandler, IdleLogic>::Result
        {
            return dispatch<ChainData, ForceSeparateDataStorageIfPossible, WriterAction<
                ChainItemFolder, InputHandler, IdleLogic
            >>(
                env, protocol, locator, hookPair, WriterAction<
                    ChainItemFolder, InputHandler, IdleLogic
                >(pollingPolicy, std::move(folder), std::move(inputHandler), std::move(idleLogic))
            );
        }

        template <class ChainData, class ChainItemFolder, class InputHandler, class IdleLogic=void, bool ForceSeparateDataStorageIfPossible=false>
        auto writer(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
            , ChainItemFolder &&folder = ChainItemFolder {}
            , InputHandler &&inputHandler = InputHandler()
            , std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            > &&idleLogic = std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            >()
        )
            -> std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , std::shared_ptr<typename App::template OnOrderFacility<typename InputHandler::InputType, typename InputHandler::ResponseType>>
                , std::shared_ptr<typename App::template OnOrderFacilityWithExternalEffects<typename InputHandler::InputType, typename InputHandler::ResponseType, typename basic::simple_shared_chain::OffChainUpdateTypeExtractor<IdleLogic>::T>> 
            >
        {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                return writer<ChainData,ChainItemFolder,InputHandler,IdleLogic,ForceSeparateDataStorageIfPossible>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), pollingPolicy, hookPair, std::move(folder), std::move(inputHandler), std::move(idleLogic)
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::writer: malformed connection locator string '")+locatorStr+"'");
            }
        }

    private:
        template <class ChainItemFolder, class TriggerT, class ResultTransformer>
        static auto ImporterOrActionFactoryTypeHelper() {
            if constexpr (std::is_same_v<TriggerT, void>) {
                return basic::simple_shared_chain::ChainReaderImporterFactory<App, ChainItemFolder, ResultTransformer>();
            } else {
                return basic::simple_shared_chain::ChainReaderActionFactory<App, ChainItemFolder, TriggerT, ResultTransformer>();
            }
        }
        template <class ChainItemFolder, class TriggerT, class ResultTransformer>
        using ImporterOrActionFactory= decltype(
            ImporterOrActionFactoryTypeHelper<ChainItemFolder,TriggerT,ResultTransformer>()
        );

    public:
        template <class ChainData, class ChainItemFolder, class TriggerT=void, class ResultTransformer=void, bool ForceSeparateDataStorageIfPossible=false>
        auto readerFactoryWithHook(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
            , ChainItemFolder &&folder = ChainItemFolder {}
            , std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer> &&resultTransformer = std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer>()
        )
            -> ImporterOrActionFactory<ChainItemFolder,TriggerT,ResultTransformer>
        {
            auto f = std::make_shared<std::decay_t<decltype(folder)>>(std::move(folder));
            auto t = std::make_shared<std::decay_t<decltype(resultTransformer)>>(std::move(resultTransformer));
            return [this,env,locatorStr,pollingPolicy,hookPair,f,t]() {
                return reader<ChainData,ChainItemFolder,TriggerT,ResultTransformer,ForceSeparateDataStorageIfPossible>(
                    env, locatorStr, pollingPolicy, hookPair, std::move(*f), std::move(*t)
                );
            };
        }

        template <class ChainData, class ChainItemFolder, class TriggerT=void, class ResultTransformer=void, bool ForceSeparateDataStorageIfPossible=false>
        auto readerFactory(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
            , ChainItemFolder &&folder = ChainItemFolder {}
            , std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer> &&resultTransformer = std::conditional_t<std::is_same_v<ResultTransformer, void>, bool, ResultTransformer>()
        )
            -> ImporterOrActionFactory<ChainItemFolder,TriggerT,ResultTransformer>
        {
            return readerFactoryWithHook<ChainData, ChainItemFolder, TriggerT, ResultTransformer, ForceSeparateDataStorageIfPossible>(
                env, locatorStr, pollingPolicy, std::nullopt, std::move(folder), std::move(resultTransformer)
            );
        }

        template <class ChainData, class ChainItemFolder, class InputHandler, class IdleLogic=void, bool ForceSeparateDataStorageIfPossible=false>
        auto writerFactoryWithHook(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
            , ChainItemFolder &&folder = ChainItemFolder {}
            , InputHandler &&inputHandler = InputHandler()
            , std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            > &&idleLogic = std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            >()
        )
            -> std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::simple_shared_chain::ChainWriterOnOrderFacilityFactory<App, ChainItemFolder, InputHandler>
                , basic::simple_shared_chain::ChainWriterOnOrderFacilityWithExternalEffectsFactory<App, ChainItemFolder, InputHandler, IdleLogic>
            >
        {
            auto f = std::make_shared<std::decay_t<decltype(folder)>>(std::move(folder));
            auto h = std::make_shared<std::decay_t<decltype(inputHandler)>>(std::move(inputHandler));
            auto l = std::make_shared<std::decay_t<decltype(idleLogic)>>(std::move(idleLogic));
            return [this,env,locatorStr,pollingPolicy,hookPair,f,h,l]() {
                return writer<ChainData,ChainItemFolder,InputHandler,IdleLogic,ForceSeparateDataStorageIfPossible>(
                    env, locatorStr, pollingPolicy, hookPair, std::move(*f), std::move(*h), std::move(*l)
                );
            };
        }

        template <class ChainData, class ChainItemFolder, class InputHandler, class IdleLogic=void, bool ForceSeparateDataStorageIfPossible=false>
        auto writerFactory(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
            , ChainItemFolder &&folder = ChainItemFolder {}
            , InputHandler &&inputHandler = InputHandler()
            , std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            > &&idleLogic = std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            >()
        )
            -> std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::simple_shared_chain::ChainWriterOnOrderFacilityFactory<App, ChainItemFolder, InputHandler>
                , basic::simple_shared_chain::ChainWriterOnOrderFacilityWithExternalEffectsFactory<App, ChainItemFolder, InputHandler, IdleLogic>
            >
        {
            return writerFactoryWithHook<ChainData, ChainItemFolder, InputHandler, IdleLogic, ForceSeparateDataStorageIfPossible>(
                env, locatorStr, pollingPolicy, std::nullopt, std::move(folder), std::move(inputHandler), std::move(idleLogic)
            );
        }
    
    private:
        template <class ChainItemFolder, class F>
        class OneShotWriteAction {
        private:
            ChainItemFolder folder_;
            F const *f_;
        public:
            using Result = bool;
            OneShotWriteAction(
                ChainItemFolder &&folder
                , F const *f
            ) 
            : folder_(std::move(folder))
            , f_(f)
            {}

            template <class Chain>
            Result invoke(typename App::EnvironmentType *env, Chain *chain) && {
                return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,Chain>::write(
                    env
                    , chain
                    , *f_ 
                    , std::move(folder_)
                );
            }
        };

    public:
        template <class ChainData, class ChainItemFolder, class F, bool ForceSeparateDataStorageIfPossible=false>
        bool oneShotWrite(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , F const &f
            , ChainItemFolder &&folder = ChainItemFolder {}
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) {
            return dispatch<ChainData, ForceSeparateDataStorageIfPossible, OneShotWriteAction<
                ChainItemFolder, F
            >>(
                env, protocol, locator, hookPair, OneShotWriteAction<
                    ChainItemFolder, F
                >(std::move(folder), &f)
            );
        }

        template <class ChainData, class ChainItemFolder, class F, bool ForceSeparateDataStorageIfPossible=false>
        bool oneShotWrite(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , F const &f
            , ChainItemFolder &&folder = ChainItemFolder {}
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                return oneShotWrite<ChainData,ChainItemFolder,F,ForceSeparateDataStorageIfPossible>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), f, std::move(folder), hookPair
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::oneShotWrite: malformed connection locator string '")+locatorStr+"'");
            }
        }
    
    private:
        template <class ChainData>
        class TryOneShotWriteConstValueAction {
        private:
            std::string const *id_;
            ChainData data_;
        public:
            using Result = bool;
            TryOneShotWriteConstValueAction(
                std::string const *id
                , ChainData &&data
            ) 
            : id_(id)
            , data_(std::move(data))
            {}

            template <class Chain>
            Result invoke(typename App::EnvironmentType *env, Chain *chain) && {
                return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,Chain>::tryWriteConstValue(
                    env
                    , chain
                    , *id_
                    , std::move(data_)
                );
            }
        };

    public:
        template <class ChainData, bool ForceSeparateDataStorageIfPossible=false>
        bool tryOneShotWriteConstValue(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , std::string const &id
            , ChainData &&data
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) {
            return dispatch<ChainData, ForceSeparateDataStorageIfPossible, TryOneShotWriteConstValueAction<
                ChainData
            >>(
                env, protocol, locator, hookPair, TryOneShotWriteConstValueAction<
                    ChainData
                >(&id, std::move(data))
            );
        }

        template <class ChainData, bool ForceSeparateDataStorageIfPossible=false>
        bool tryOneShotWriteConstValue(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , std::string const &id
            , ChainData &&data
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                return tryOneShotWriteConstValue<ChainData, ForceSeparateDataStorageIfPossible>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), id, std::move(data), hookPair
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::tryOneShotWriteConstValue: malformed connection locator string '")+locatorStr+"'");
            }
        }
    
    private:
        template <class ChainData>
        class BlockingOneShotWriteConstValueAction {
        private:
            std::string const *id_;
            ChainData data_;
        public:
            using Result = void;
            BlockingOneShotWriteConstValueAction(
                std::string const *id
                , ChainData &&data
            ) 
            : id_(id)
            , data_(std::move(data))
            {}

            template <class Chain>
            Result invoke(typename App::EnvironmentType *env, Chain *chain) && {
                basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,Chain>::blockingWriteConstValue(
                    env
                    , chain
                    , *id_ 
                    , std::move(data_)
                );
            }
        };

    public:
        template <class ChainData, bool ForceSeparateDataStorageIfPossible=false>
        void blockingOneShotWriteConstValue(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , std::string const &id
            , ChainData &&data
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) {
            dispatch<ChainData, ForceSeparateDataStorageIfPossible, BlockingOneShotWriteConstValueAction<
                ChainData
            >>(
                env, protocol, locator, hookPair, BlockingOneShotWriteConstValueAction<
                    ChainData
                >(&id, std::move(data))
            );
        }

        template <class ChainData, bool ForceSeparateDataStorageIfPossible=false>
        void blockingOneShotWriteConstValue(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , std::string const &id
            , ChainData &&data
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                blockingOneShotWriteConstValue<ChainData, ForceSeparateDataStorageIfPossible>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), id, std::move(data), hookPair
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::tryOneShotWriteConstValue: malformed connection locator string '")+locatorStr+"'");
            }
        }
    
    private:
        template <class ExtraData>
        class WriteExtraDataAction {
        private:
            std::string const *key_;
            ExtraData const *extraData_;
        public:
            using Result = void;
            WriteExtraDataAction(
                std::string const *key
                , ExtraData const *extraData
            ) 
            : key_(key)
            , extraData_(extraData)
            {}

            template <class Chain>
            Result invoke(typename App::EnvironmentType *env, Chain *chain) && {
                shared_chain_utils::writeExtraDataHelper<ExtraData>(
                    chain
                    , *key_
                    , *extraData_
                );
            }
        };

    public:
        template <class ChainData, class ExtraData, bool ForceSeparateDataStorageIfPossible=false>
        void writeExtraData(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , std::string const &key
            , ExtraData const &extraData
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) {
            dispatch<ChainData, ForceSeparateDataStorageIfPossible, WriteExtraDataAction<
                ExtraData
            >>(
                env, protocol, locator, hookPair, WriteExtraDataAction<
                    ExtraData
                >(&key, &extraData)
            );
        }

        template <class ChainData, class ExtraData, bool ForceSeparateDataStorageIfPossible=false>
        void writeExtraData(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , std::string const &key
            , ExtraData const &extraData
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                writeExtraData<ChainData, ExtraData, ForceSeparateDataStorageIfPossible>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), key, extraData, hookPair
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::writeExtraData: malformed connection locator string '")+locatorStr+"'");
            }
        }
    
    private:
        template <class ExtraData>
        class ReadExtraDataAction {
        private:
            std::string const *key_;
        public:
            using Result = std::optional<ExtraData>;
            ReadExtraDataAction(
                std::string const *key
            ) 
            : key_(key)
            {}

            template <class Chain>
            Result invoke(typename App::EnvironmentType *env, Chain *chain) && {
                return shared_chain_utils::readExtraDataHelper<ExtraData>(
                    chain
                    , *key_
                );
            }
        };

    public:
        template <class ChainData, class ExtraData, bool ForceSeparateDataStorageIfPossible=false>
        std::optional<ExtraData> readExtraData(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , std::string const &key
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) {
            return dispatch<ChainData, ForceSeparateDataStorageIfPossible, ReadExtraDataAction<
                ExtraData
            >>(
                env, protocol, locator, hookPair, ReadExtraDataAction<
                    ExtraData
                >(&key)
            );
        }

        template <class ChainData, class ExtraData, bool ForceSeparateDataStorageIfPossible=false>
        std::optional<ExtraData> readExtraData(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , std::string const &key
            , std::optional<ByteDataHookPair> hookPair = std::nullopt
        ) {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                return readExtraData<ChainData, ExtraData, ForceSeparateDataStorageIfPossible>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), key, hookPair
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::readExtraData: malformed connection locator string '")+locatorStr+"'");
            }
        }
    };

} } } }

#endif