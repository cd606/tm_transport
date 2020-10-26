#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_SHARED_CHAIN_CREATOR_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_SHARED_CHAIN_CREATOR_HPP_

#include <tm_kit/basic/simple_shared_chain/ChainReader.hpp>
#include <tm_kit/basic/simple_shared_chain/ChainWriter.hpp>
#include <tm_kit/basic/simple_shared_chain/OneShotChainWriter.hpp>

#include <tm_kit/transport/etcd_shared_chain/EtcdChain.hpp>
#include <tm_kit/transport/redis_shared_chain/RedisChain.hpp>
#include <tm_kit/transport/lock_free_in_memory_shared_chain/LockFreeInMemoryChain.hpp>
#include <tm_kit/transport/lock_free_in_memory_shared_chain/LockFreeInBoostSharedMemoryChain.hpp>

#include <tm_kit/transport/ConnectionLocator.hpp>

#include <unordered_map>
#include <mutex>

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    struct AllChainComponents :
        public etcd_shared_chain::EtcdChainComponent
        , public redis_shared_chain::RedisChainComponent
        , public lock_free_in_memory_shared_chain::SharedMemoryChainComponent
    {};

    enum class SharedChainProtocol {
        Etcd 
        , Redis 
        , InMemoryWithLock
        , InMemoryLockFree
        , InSharedMemory
    };
    inline const std::array<std::string,5> SHARED_CHAIN_PROTOCOL_STR = {
        "etcd"
        , "redis"
        , "in_memory_with_lock"
        , "in_memory_lock_free"
        , "in_shared_memory"
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
            return std::nullopt;
        }

        inline std::string makeSharedChainLocator(SharedChainProtocol protocol, ConnectionLocator const &locator) {
            return SHARED_CHAIN_PROTOCOL_STR[static_cast<int>(protocol)] + "://" + locator.toSerializationFormat();
        }

        inline etcd_shared_chain::EtcdChainConfiguration etcdChainConfigurationFromConnectionLocator(ConnectionLocator const &l) {
            return etcd_shared_chain::EtcdChainConfiguration()
                .InsecureEtcdServerAddr(l.host()+":"+std::to_string(l.port()))
                .HeadKey(l.query("headKey", "head"))
                .SaveDataOnSeparateStorage(l.query("saveDataOnSeparateStorage","false") == "true")
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
        
        template <class App, class ChainItemFolder, class TriggerT, class Chain>
        inline auto chainReaderHelper(
            typename App::EnvironmentType *env
            , Chain *chain 
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy
        ) 
            -> std::conditional_t<
                std::is_same_v<TriggerT, void>
                , std::shared_ptr<typename App::template Importer<typename ChainItemFolder::ResultType>>
                , std::shared_ptr<typename App::template Action<TriggerT, typename ChainItemFolder::ResultType>> 
            >
        {
            if constexpr (std::is_same_v<TriggerT, void>) {
                return basic::simple_shared_chain::ChainReader<
                    App
                    , Chain
                    , ChainItemFolder
                    , void
                >::importer(
                    chain
                    , pollingPolicy
                );
            } else {
                return basic::simple_shared_chain::ChainReader<
                    App
                    , Chain
                    , ChainItemFolder
                    , TriggerT
                >::action(
                    env
                    , chain
                );
            }
        }

        template <class App, class ChainItemFolder, class InputHandler, class IdleLogic, class Chain>
        inline auto chainWriterHelper(
            Chain *chain 
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy
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
                    , std::move(inputHandler)
                    , std::move(idleLogic)
                );
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
    public:
        SharedChainCreator() : mutex_(), chains_() {}
        ~SharedChainCreator() {
            std::lock_guard<std::mutex> _(mutex_);
            for (auto &item : chains_) {
                std::get<1>(item.second)(std::get<0>(item.second));
            }
            chains_.clear();
        }

        template <class ChainData, class ChainItemFolder, class TriggerT=void>
        auto reader(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
        )
            -> std::conditional_t<
                std::is_same_v<TriggerT, void>
                , std::shared_ptr<typename App::template Importer<typename ChainItemFolder::ResultType>>
                , std::shared_ptr<typename App::template Action<TriggerT, typename ChainItemFolder::ResultType>> 
            >
        {
            switch (protocol) {
            case SharedChainProtocol::Etcd:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, etcd_shared_chain::EtcdChainComponent *>) {
                        auto conf = shared_chain_utils::etcdChainConfigurationFromConnectionLocator(locator);
                        return shared_chain_utils::chainReaderHelper<App,ChainItemFolder,TriggerT>(
                            env
                            , getChain<etcd_shared_chain::EtcdChain<ChainData>>(
                                shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , [&conf]() {
                                    return new etcd_shared_chain::EtcdChain<ChainData>(conf);
                                }
                            )
                            , pollingPolicy
                        );
                    } else {
                        throw new std::runtime_error("sharedChainCreator::reader: Etcd chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::Redis:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, redis_shared_chain::RedisChainComponent *>) {
                        auto conf = shared_chain_utils::redisChainConfigurationFromConnectionLocator(locator);
                        return shared_chain_utils::chainReaderHelper<App,ChainItemFolder,TriggerT>(
                            env
                            , getChain<redis_shared_chain::RedisChain<ChainData>>(
                                shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , [&conf]() {
                                    return new redis_shared_chain::RedisChain<ChainData>(conf);
                                }
                            )
                            , pollingPolicy
                        );
                    } else {
                        throw new std::runtime_error("sharedChainCreator::reader: Redis chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::InMemoryWithLock:
                {
                    return shared_chain_utils::chainReaderHelper<App,ChainItemFolder,TriggerT>(
                        env
                        , getChain<etcd_shared_chain::InMemoryChain<ChainData>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new etcd_shared_chain::InMemoryChain<ChainData>();
                            }
                        )
                        , pollingPolicy
                    );
                }
                break;
            case SharedChainProtocol::InMemoryLockFree:
                {
                    return shared_chain_utils::chainReaderHelper<App,ChainItemFolder,TriggerT>(
                        env
                        , getChain<lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>();
                            }
                        )
                        , pollingPolicy
                    );
                }
                break;
            case SharedChainProtocol::InSharedMemory:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, lock_free_in_memory_shared_chain::SharedMemoryChainComponent *>) {
                        auto memoryName = locator.identifier();
                        std::size_t memorySize = static_cast<std::size_t>(4*1024LL*1024LL*1024LL);
                        auto sizeStr = locator.query("size","");
                        if (sizeStr != "") {
                            memorySize = static_cast<std::size_t>(std::stoull(sizeStr));
                        }
                        bool useName = (locator.query("useName", "false") == "true");                       
                        if (useName) {
                            return shared_chain_utils::chainReaderHelper<App,ChainItemFolder,TriggerT>(
                                env
                                , getChain<lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                    ChainData
                                    , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                    , (
                                        App::PossiblyMultiThreaded
                                        ?
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                        :
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                    )
                                >>(
                                    shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                    , [memoryName,memorySize]() {
                                        return new lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                            ChainData
                                            , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                            , (
                                                App::PossiblyMultiThreaded
                                                ?
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                                :
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                            )
                                        >(memoryName, memorySize);
                                    }
                                )
                                , pollingPolicy
                            );
                        } else {
                            return shared_chain_utils::chainReaderHelper<App,ChainItemFolder,TriggerT>(
                                env 
                                , getChain<lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                    ChainData
                                    , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                    , (
                                        App::PossiblyMultiThreaded
                                        ?
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                        :
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                    )
                                >>(
                                    shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                    , [memoryName,memorySize]() {
                                        return new lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                            ChainData
                                            , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                            , (
                                                App::PossiblyMultiThreaded
                                                ?
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                                :
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                            )
                                        >(memoryName, memorySize);
                                    }
                                )
                                , pollingPolicy
                            );
                        }
                    } else {
                        throw new std::runtime_error("sharedChainCreator::reader: Shared memory chain is not supported by the environment");
                    }
                }
                break;
            default:
                throw std::runtime_error(std::string("sharedChainCreator::reader: unknown protocol")+std::to_string(static_cast<int>(protocol)));
                break;
            }
        }

        template <class ChainData, class ChainItemFolder, class TriggerT=void>
        auto reader(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
        )
            -> std::conditional_t<
                std::is_same_v<TriggerT, void>
                , std::shared_ptr<typename App::template Importer<typename ChainItemFolder::ResultType>>
                , std::shared_ptr<typename App::template Action<TriggerT, typename ChainItemFolder::ResultType>> 
            >
        {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                return reader<ChainData,ChainItemFolder,TriggerT>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), pollingPolicy
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::reader: malformed connection locator string '")+locatorStr+"'");
            }
        }

        template <class ChainData, class ChainItemFolder, class InputHandler, class IdleLogic=void>
        auto writer(
            SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
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
            switch (protocol) {
            case SharedChainProtocol::Etcd:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, etcd_shared_chain::EtcdChainComponent *>) {
                        auto conf = shared_chain_utils::etcdChainConfigurationFromConnectionLocator(locator);
                        return shared_chain_utils::chainWriterHelper<App,ChainItemFolder,InputHandler,IdleLogic>(
                            getChain<etcd_shared_chain::EtcdChain<ChainData>>(
                                shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , [&conf]() {
                                    return new etcd_shared_chain::EtcdChain<ChainData>(conf);
                                }
                            )
                            , pollingPolicy
                            , std::move(inputHandler)
                            , std::move(idleLogic)
                        );
                    } else {
                        throw new std::runtime_error("sharedChainCreator::writer: Etcd chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::Redis:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, redis_shared_chain::RedisChainComponent *>) {
                        auto conf = shared_chain_utils::redisChainConfigurationFromConnectionLocator(locator);
                        return shared_chain_utils::chainWriterHelper<App,ChainItemFolder,InputHandler,IdleLogic>(
                            getChain<redis_shared_chain::RedisChain<ChainData>>(
                                shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , [&conf]() {
                                    return new redis_shared_chain::RedisChain<ChainData>(conf);
                                }
                            )
                            , pollingPolicy
                            , std::move(inputHandler)
                            , std::move(idleLogic)
                        );
                    } else {
                        throw new std::runtime_error("sharedChainCreator::writer: Redis chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::InMemoryWithLock:
                {
                    return shared_chain_utils::chainWriterHelper<App,ChainItemFolder,InputHandler,IdleLogic>(
                        getChain<etcd_shared_chain::InMemoryChain<ChainData>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new etcd_shared_chain::InMemoryChain<ChainData>();
                            }
                        )
                        , pollingPolicy
                        , std::move(inputHandler)
                        , std::move(idleLogic)
                    );
                }
                break;
            case SharedChainProtocol::InMemoryLockFree:
                {
                    return shared_chain_utils::chainWriterHelper<App,ChainItemFolder,InputHandler,IdleLogic>(
                        getChain<lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>();
                            }
                        )
                        , pollingPolicy
                        , std::move(inputHandler)
                        , std::move(idleLogic)
                    );
                }
                break;
            case SharedChainProtocol::InSharedMemory:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, lock_free_in_memory_shared_chain::SharedMemoryChainComponent *>) {
                        auto memoryName = locator.identifier();
                        std::size_t memorySize = static_cast<std::size_t>(4*1024LL*1024LL*1024LL);
                        auto sizeStr = locator.query("size","");
                        if (sizeStr != "") {
                            memorySize = static_cast<std::size_t>(std::stoull(sizeStr));
                        }
                        bool useName = (locator.query("useName", "false") == "true");
                        if (useName) {
                            return shared_chain_utils::chainWriterHelper<App,ChainItemFolder,InputHandler,IdleLogic>(
                                getChain<lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                    ChainData
                                    , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                    , (
                                        App::PossiblyMultiThreaded
                                        ?
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                        :
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                    )
                                >>(
                                    shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                    , [memoryName,memorySize]() {
                                        return new lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                            ChainData
                                            , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                            , (
                                                App::PossiblyMultiThreaded
                                                ?
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                                :
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                            )
                                        >(memoryName, memorySize);
                                    }
                                )
                                , pollingPolicy
                                , std::move(inputHandler)
                                , std::move(idleLogic)
                            );
                        } else {
                            return shared_chain_utils::chainWriterHelper<App,ChainItemFolder,InputHandler,IdleLogic>(
                                getChain<lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                    ChainData
                                    , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                    , (
                                        App::PossiblyMultiThreaded
                                        ?
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                        :
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                    )
                                >>(
                                    shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                    , [memoryName,memorySize]() {
                                        return new lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                            ChainData
                                            , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                            , (
                                                App::PossiblyMultiThreaded
                                                ?
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                                :
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                            )
                                        >(memoryName, memorySize);
                                    }
                                )
                                , pollingPolicy
                                , std::move(inputHandler)
                                , std::move(idleLogic)
                            );
                        }
                    } else {
                        throw new std::runtime_error("sharedChainCreator::writer: Shared memory chain is not supported by the environment");
                    }
                }
                break;
            default:
                throw std::runtime_error(std::string("sharedChainCreator::writer: unknown protocol")+std::to_string(static_cast<int>(protocol)));
                break;
            }
        }

        template <class ChainData, class ChainItemFolder, class InputHandler, class IdleLogic=void>
        auto writer(
            std::string const &locatorStr
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
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
                return writer<ChainData,ChainItemFolder,InputHandler,IdleLogic>(
                    std::get<0>(*parsed), std::get<1>(*parsed), pollingPolicy, std::move(inputHandler), std::move(idleLogic)
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::writer: malformed connection locator string '")+locatorStr+"'");
            }
        }

        template <class ChainData, class ChainItemFolder, class TriggerT=void>
        auto readerFactory(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
        )
            -> std::conditional_t<
                std::is_same_v<TriggerT, void>
                , basic::simple_shared_chain::ChainReaderImporterFactory<App, ChainItemFolder>
                , basic::simple_shared_chain::ChainReaderActionFactory<App, ChainItemFolder, TriggerT>
            >
        {
            return [this,env,locatorStr,pollingPolicy]() {
                return reader<ChainData,ChainItemFolder,TriggerT>(
                    env, locatorStr, pollingPolicy
                );
            };
        }

        template <class ChainData, class ChainItemFolder, class InputHandler, class IdleLogic=void>
        auto writerFactory(
            std::string const &locatorStr
            , basic::simple_shared_chain::ChainPollingPolicy const &pollingPolicy = basic::simple_shared_chain::ChainPollingPolicy()
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
            InputHandler h = std::move(inputHandler);
            std::conditional_t<
                std::is_same_v<IdleLogic, void>
                , basic::VoidStruct
                , IdleLogic
            > l = std::move(idleLogic);
            return [this,locatorStr,pollingPolicy,h=std::move(h),l=std::move(l)]() {
                InputHandler h1 = std::move(h);
                std::conditional_t<
                    std::is_same_v<IdleLogic, void>
                    , basic::VoidStruct
                    , IdleLogic
                > l1 = std::move(l);
                return writer<ChainData,ChainItemFolder,InputHandler,IdleLogic>(
                    locatorStr, pollingPolicy, std::move(h1), std::move(l1)
                );
            };
        }

        template <class ChainData, class ChainItemFolder, class F>
        bool oneShotWrite(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , F const &f
            , ChainItemFolder &&folder = ChainItemFolder {}
        ) {
            switch (protocol) {
            case SharedChainProtocol::Etcd:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, etcd_shared_chain::EtcdChainComponent *>) {
                        auto conf = shared_chain_utils::etcdChainConfigurationFromConnectionLocator(locator);
                        return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,etcd_shared_chain::EtcdChain<ChainData>>::write(
                            env
                            , getChain<etcd_shared_chain::EtcdChain<ChainData>>(
                                shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , [&conf]() {
                                    return new etcd_shared_chain::EtcdChain<ChainData>(conf);
                                }
                            )
                            , f 
                            , std::move(folder)
                        );
                    } else {
                        throw new std::runtime_error("sharedChainCreator::oneShotWrite: Etcd chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::Redis:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, redis_shared_chain::RedisChainComponent *>) {
                        auto conf = shared_chain_utils::redisChainConfigurationFromConnectionLocator(locator);
                        return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,redis_shared_chain::RedisChain<ChainData>>::write(
                            env
                            , getChain<redis_shared_chain::RedisChain<ChainData>>(
                                shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , [&conf]() {
                                    return new redis_shared_chain::RedisChain<ChainData>(conf);
                                }
                            )
                            , f 
                            , std::move(folder)
                        );
                    } else {
                        throw new std::runtime_error("sharedChainCreator::oneShotWrite: Redis chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::InMemoryWithLock:
                {
                    return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,etcd_shared_chain::InMemoryChain<ChainData>>::write(
                        env
                        , getChain<etcd_shared_chain::InMemoryChain<ChainData>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new etcd_shared_chain::InMemoryChain<ChainData>();
                            }
                        )
                        , f 
                        , std::move(folder)
                    );
                }
                break;
            case SharedChainProtocol::InMemoryLockFree:
                {
                    return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>>::write(
                        env
                        , getChain<lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>();
                            }
                        )
                        , f 
                        , std::move(folder)
                    );
                }
                break;
            case SharedChainProtocol::InSharedMemory:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, lock_free_in_memory_shared_chain::SharedMemoryChainComponent *>) {
                        auto memoryName = locator.identifier();
                        std::size_t memorySize = static_cast<std::size_t>(4*1024LL*1024LL*1024LL);
                        auto sizeStr = locator.query("size","");
                        if (sizeStr != "") {
                            memorySize = static_cast<std::size_t>(std::stoull(sizeStr));
                        }
                        bool useName = (locator.query("useName", "false") == "true");
                        if (useName) {
                            return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                ChainData
                                , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                , (
                                    App::PossiblyMultiThreaded
                                    ?
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                    :
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                )
                            >>::write(
                                env
                                , getChain<lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                    ChainData
                                    , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                    , (
                                        App::PossiblyMultiThreaded
                                        ?
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                        :
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                    )
                                >>(
                                    shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                    , [memoryName,memorySize]() {
                                        return new lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                            ChainData
                                            , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                            , (
                                                App::PossiblyMultiThreaded
                                                ?
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                                :
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                            )
                                        >(memoryName, memorySize);
                                    }
                                )
                                , f 
                                , std::move(folder)
                            );
                        } else {
                            return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                ChainData
                                , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                , (
                                    App::PossiblyMultiThreaded
                                    ?
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                    :
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                )
                            >>::write(
                                env
                                , getChain<lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                    ChainData
                                    , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                    , (
                                        App::PossiblyMultiThreaded
                                        ?
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                        :
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                    )
                                >>(
                                    shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                    , [memoryName,memorySize]() {
                                        return new lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                            ChainData
                                            , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                            , (
                                                App::PossiblyMultiThreaded
                                                ?
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                                :
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                            )
                                        >(memoryName, memorySize);
                                    }
                                )
                                , f 
                                , std::move(folder)
                            );
                        }
                    } else {
                        throw new std::runtime_error("sharedChainCreator::oneShotWrite: Shared memory chain is not supported by the environment");
                    }
                }
                break;
            default:
                throw std::runtime_error(std::string("sharedChainCreator::oneShotWrite: unknown protocol")+std::to_string(static_cast<int>(protocol)));
                break;
            }
        }

        template <class ChainData, class ChainItemFolder, class F>
        bool oneShotWrite(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , F const &f
            , ChainItemFolder &&folder = ChainItemFolder {}
        ) {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                return oneShotWrite<ChainData,ChainItemFolder,F>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), f, std::move(folder)
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::oneShotWrite: malformed connection locator string '")+locatorStr+"'");
            }
        }

        template <class ChainData>
        bool tryOneShotWriteConstValue(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , std::string const &id
            , ChainData &&data
        ) {
            switch (protocol) {
            case SharedChainProtocol::Etcd:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, etcd_shared_chain::EtcdChainComponent *>) {
                        auto conf = shared_chain_utils::etcdChainConfigurationFromConnectionLocator(locator);
                        return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,etcd_shared_chain::EtcdChain<ChainData>>::tryWriteConstValue(
                            env
                            , getChain<etcd_shared_chain::EtcdChain<ChainData>>(
                                shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , [&conf]() {
                                    return new etcd_shared_chain::EtcdChain<ChainData>(conf);
                                }
                            )
                            , id 
                            , std::move(data)
                        );
                    } else {
                        throw new std::runtime_error("sharedChainCreator::tryOneShotWriteConstValue: Etcd chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::Redis:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, redis_shared_chain::RedisChainComponent *>) {
                        auto conf = shared_chain_utils::redisChainConfigurationFromConnectionLocator(locator);
                        return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,redis_shared_chain::RedisChain<ChainData>>::tryWriteConstValue(
                            env
                            , getChain<redis_shared_chain::RedisChain<ChainData>>(
                                shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , [&conf]() {
                                    return new redis_shared_chain::RedisChain<ChainData>(conf);
                                }
                            )
                            , id 
                            , std::move(data)
                        );
                    } else {
                        throw new std::runtime_error("sharedChainCreator::tryOneShotWriteConstValue: Redis chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::InMemoryWithLock:
                {
                    return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,etcd_shared_chain::InMemoryChain<ChainData>>::tryWriteConstValue(
                        env
                        , getChain<etcd_shared_chain::InMemoryChain<ChainData>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new etcd_shared_chain::InMemoryChain<ChainData>();
                            }
                        )
                        , id 
                        , std::move(data)
                    );
                }
                break;
            case SharedChainProtocol::InMemoryLockFree:
                {
                    return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>>::tryWriteConstValue(
                        env
                        , getChain<lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>();
                            }
                        )
                        , id 
                        , std::move(data)
                    );
                }
                break;
            case SharedChainProtocol::InSharedMemory:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, lock_free_in_memory_shared_chain::SharedMemoryChainComponent *>) {
                        auto memoryName = locator.identifier();
                        std::size_t memorySize = static_cast<std::size_t>(4*1024LL*1024LL*1024LL);
                        auto sizeStr = locator.query("size","");
                        if (sizeStr != "") {
                            memorySize = static_cast<std::size_t>(std::stoull(sizeStr));
                        }
                        bool useName = (locator.query("useName", "false") == "true");
                        if (useName) {
                            return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                ChainData
                                , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                , (
                                    App::PossiblyMultiThreaded
                                    ?
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                    :
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                )
                            >>::tryWriteConstValue(
                                env
                                , getChain<lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                    ChainData
                                    , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                    , (
                                        App::PossiblyMultiThreaded
                                        ?
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                        :
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                    )
                                >>(
                                    shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                    , [memoryName,memorySize]() {
                                        return new lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                            ChainData
                                            , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                            , (
                                                App::PossiblyMultiThreaded
                                                ?
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                                :
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                            )
                                        >(memoryName, memorySize);
                                    }
                                )
                                , id 
                                , std::move(data)
                            );
                        } else {
                            return basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                ChainData
                                , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                , (
                                    App::PossiblyMultiThreaded
                                    ?
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                    :
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                )
                            >>::tryWriteConstValue(
                                env
                                , getChain<lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                    ChainData
                                    , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                    , (
                                        App::PossiblyMultiThreaded
                                        ?
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                        :
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                    )
                                >>(
                                    shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                    , [memoryName,memorySize]() {
                                        return new lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                            ChainData
                                            , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                            , (
                                                App::PossiblyMultiThreaded
                                                ?
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                                :
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                            )
                                        >(memoryName, memorySize);
                                    }
                                )
                                , id 
                                , std::move(data)
                            );
                        }
                    } else {
                        throw new std::runtime_error("sharedChainCreator::tryOneShotWriteConstValue: Shared memory chain is not supported by the environment");
                    }
                }
                break;
            default:
                throw std::runtime_error(std::string("sharedChainCreator::tryOneShotWriteConstValue: unknown protocol")+std::to_string(static_cast<int>(protocol)));
                break;
            }
        }

        template <class ChainData>
        bool tryOneShotWriteConstValue(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , std::string const &id
            , ChainData &&data
        ) {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                return tryOneShotWriteConstValue<ChainData>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), id, std::move(data)
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::tryOneShotWriteConstValue: malformed connection locator string '")+locatorStr+"'");
            }
        }

        template <class ChainData>
        void blockingOneShotWriteConstValue(
            typename App::EnvironmentType *env
            , SharedChainProtocol protocol
            , ConnectionLocator const &locator
            , std::string const &id
            , ChainData &&data
        ) {
            switch (protocol) {
            case SharedChainProtocol::Etcd:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, etcd_shared_chain::EtcdChainComponent *>) {
                        auto conf = shared_chain_utils::etcdChainConfigurationFromConnectionLocator(locator);
                        basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,etcd_shared_chain::EtcdChain<ChainData>>::blockingWriteConstValue(
                            env
                            , getChain<etcd_shared_chain::EtcdChain<ChainData>>(
                                shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , [&conf]() {
                                    return new etcd_shared_chain::EtcdChain<ChainData>(conf);
                                }
                            )
                            , id 
                            , std::move(data)
                        );
                    } else {
                        throw new std::runtime_error("sharedChainCreator::tryOneShotWriteConstValue: Etcd chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::Redis:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, redis_shared_chain::RedisChainComponent *>) {
                        auto conf = shared_chain_utils::redisChainConfigurationFromConnectionLocator(locator);
                        basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,redis_shared_chain::RedisChain<ChainData>>::blockingWriteConstValue(
                            env
                            , getChain<redis_shared_chain::RedisChain<ChainData>>(
                                shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                , [&conf]() {
                                    return new redis_shared_chain::RedisChain<ChainData>(conf);
                                }
                            )
                            , id 
                            , std::move(data)
                        );
                    } else {
                        throw new std::runtime_error("sharedChainCreator::tryOneShotWriteConstValue: Redis chain is not supported by the environment");
                    }
                }
                break;
            case SharedChainProtocol::InMemoryWithLock:
                {
                    basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,etcd_shared_chain::InMemoryChain<ChainData>>::blockingWriteConstValue(
                        env
                        , getChain<etcd_shared_chain::InMemoryChain<ChainData>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new etcd_shared_chain::InMemoryChain<ChainData>();
                            }
                        )
                        , id 
                        , std::move(data)
                    );
                }
                break;
            case SharedChainProtocol::InMemoryLockFree:
                {
                    basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>>::blockingWriteConstValue(
                        env
                        , getChain<lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>>(
                            shared_chain_utils::makeSharedChainLocator(protocol, locator)
                            , []() {
                                return new lock_free_in_memory_shared_chain::LockFreeInMemoryChain<ChainData>();
                            }
                        )
                        , id 
                        , std::move(data)
                    );
                }
                break;
            case SharedChainProtocol::InSharedMemory:
                {
                    if constexpr (std::is_convertible_v<typename App::EnvironmentType *, lock_free_in_memory_shared_chain::SharedMemoryChainComponent *>) {
                        auto memoryName = locator.identifier();
                        std::size_t memorySize = static_cast<std::size_t>(4*1024LL*1024LL*1024LL);
                        auto sizeStr = locator.query("size","");
                        if (sizeStr != "") {
                            memorySize = static_cast<std::size_t>(std::stoull(sizeStr));
                        }
                        bool useName = (locator.query("useName", "false") == "true");
                        if (useName) {
                            basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                ChainData
                                , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                , (
                                    App::PossiblyMultiThreaded
                                    ?
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                    :
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                )
                            >>::blockingWriteConstValue(
                                env
                                , getChain<lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                    ChainData
                                    , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                    , (
                                        App::PossiblyMultiThreaded
                                        ?
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                        :
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                    )
                                >>(
                                    shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                    , [memoryName,memorySize]() {
                                        return new lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                            ChainData
                                            , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByName
                                            , (
                                                App::PossiblyMultiThreaded
                                                ?
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                                :
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                            )
                                        >(memoryName, memorySize);
                                    }
                                )
                                , id 
                                , std::move(data)
                            );
                        } else {
                            basic::simple_shared_chain::OneShotChainWriter<typename App::EnvironmentType,lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                ChainData
                                , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                , (
                                    App::PossiblyMultiThreaded
                                    ?
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                    :
                                    lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                )
                            >>::blockingWriteConstValue(
                                env
                                , getChain<lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                    ChainData
                                    , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                    , (
                                        App::PossiblyMultiThreaded
                                        ?
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                        :
                                        lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                    )
                                >>(
                                    shared_chain_utils::makeSharedChainLocator(protocol, locator)
                                    , [memoryName,memorySize]() {
                                        return new lock_free_in_memory_shared_chain::LockFreeInBoostSharedMemoryChain<
                                            ChainData
                                            , lock_free_in_memory_shared_chain::BoostSharedMemoryChainFastRecoverSupport::ByOffset
                                            , (
                                                App::PossiblyMultiThreaded
                                                ?
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::MutexProtected
                                                :
                                                lock_free_in_memory_shared_chain::BoostSharedMemoryChainExtraDataProtectionStrategy::Unsafe
                                            )
                                        >(memoryName, memorySize);
                                    }
                                )
                                , id 
                                , std::move(data)
                            );
                        }
                    } else {
                        throw new std::runtime_error("sharedChainCreator::tryOneShotWriteConstValue: Shared memory chain is not supported by the environment");
                    }
                }
                break;
            default:
                throw std::runtime_error(std::string("sharedChainCreator::tryOneShotWriteConstValue: unknown protocol")+std::to_string(static_cast<int>(protocol)));
                break;
            }
        }

        template <class ChainData>
        void blockingOneShotWriteConstValue(
            typename App::EnvironmentType *env
            , std::string const &locatorStr
            , std::string const &id
            , ChainData &&data
        ) {
            auto parsed = shared_chain_utils::parseSharedChainLocator(locatorStr);
            if (parsed) {
                blockingOneShotWriteConstValue<ChainData>(
                    env, std::get<0>(*parsed), std::get<1>(*parsed), id, std::move(data)
                );
            } else {
                throw std::runtime_error(std::string("sharedChainCreator::tryOneShotWriteConstValue: malformed connection locator string '")+locatorStr+"'");
            }
        }
    };

} } } }

#endif