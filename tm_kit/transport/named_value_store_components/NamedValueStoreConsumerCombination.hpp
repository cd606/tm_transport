#ifndef TM_KIT_TRANSPORT_ETCD_NAMED_VALUE_STORE_CONSUMER_COMBINATION_HPP_
#define TM_KIT_TRANSPORT_ETCD_NAMED_VALUE_STORE_CONSUMER_COMBINATION_HPP_

#include <tm_kit/infra/GenericLift.hpp>
#include <tm_kit/basic/SerializationHelperMacros.hpp>
#include <tm_kit/basic/transaction/named_value_store/DataModel.hpp>
#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>
#include <tm_kit/transport/MultiTransportRemoteFacilityManagingUtils.hpp>

#define NAMED_VALUE_STORE_CONSUMER_DELETE_KEY_FIELDS \
    ((std::string, key))
#define NAMED_VALUE_STORE_CONSUMER_INSERT_OR_UPDATE_KEY_FIELDS \
    ((std::string, key)) \
    ((Data, data))

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace named_value_store_components {
    TM_BASIC_CBOR_CAPABLE_STRUCT(ConsumerDeleteKey, NAMED_VALUE_STORE_CONSUMER_DELETE_KEY_FIELDS);
    TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT(((typename, Data)), ConsumerInsertOrUpdateKey, NAMED_VALUE_STORE_CONSUMER_INSERT_OR_UPDATE_KEY_FIELDS);

    template <typename Data>
    using ConsumerUpdate = basic::SingleLayerWrapper<std::variant<
        ConsumerDeleteKey
        , ConsumerInsertOrUpdateKey<Data>
    >>;

    template <
        class R
        , template <class...> class ProtocolWrapper
        , typename Data
        , typename ExitSourceType=basic::VoidStruct
        , typename=std::enable_if_t<
            std::is_convertible_v<
                typename R::AppType::EnvironmentType *
                , ClientSideAbstractIdentityAttacherComponent<
                    std::string
                    , typename basic::transaction::named_value_store::GS<typename R::AppType::EnvironmentType::IDType, Data>::Input
                > *
            >
        >
    >
    auto consumerCombination(
        R &r
        , std::string const &prefix
        , std::variant<
            typename R::template ConvertibleToSourceoid<HeartbeatMessage>
            , typename R::template ConvertibleToSourceoid<std::shared_ptr<HeartbeatMessage const>>
        > &&heartbeatSource
        , std::regex const &serverNameRE
        , std::string const &facilityRegistrationName
        , std::optional<typename R::template Source<ExitSourceType>> &&exitSource
        , std::chrono::system_clock::duration ttl = std::chrono::seconds(3)
        , std::chrono::system_clock::duration checkPeriod = std::chrono::seconds(5)
    ) -> typename R::template Source<ConsumerUpdate<Data>>
    {
        using M = typename R::AppType;
        using Env = typename M::EnvironmentType;

        using DI = basic::transaction::named_value_store::DI<Data>;
        using GS = basic::transaction::named_value_store::GS<typename Env::IDType, Data>;

        auto facilityInfo =  transport::MultiTransportRemoteFacilityManagingUtils<R>
        ::template setupOneDistinguishedRemoteFacilityWithProtocol<
            ProtocolWrapper, typename GS::Input, typename GS::Output
        >(
            r
            , std::move(heartbeatSource)
            , serverNameRE
            , facilityRegistrationName
            , []() -> typename GS::Input {
                return typename GS::Input {
                    typename GS::Subscription {
                        std::vector<typename DI::Key> {
                            typename DI::Key {}
                        }
                    }
                };
            }
            , [](typename GS::Input const &, typename GS::Output const &) {
                return true;
            }
            , std::nullopt
            , ttl
            , checkPeriod
        );
        using FacilityKey = std::tuple<ConnectionLocator, typename GS::Input>;
        auto idRecord = std::make_shared<std::optional<typename Env::IDType>>(std::nullopt);
        r.preservePointer(idRecord);
        bool hasExitDataSource = (bool) exitSource;
        auto *env = r.environment();
        auto handler = infra::GenericLift<M>::liftMulti(
            [idRecord,hasExitDataSource,env](typename M::template KeyedData<FacilityKey, typename GS::Output> &&input)
                -> std::vector<ConsumerUpdate<Data>>
            {
                auto const &key = input.key;
                return std::visit([idRecord,hasExitDataSource,env,&key](auto &&x) -> std::vector<ConsumerUpdate<Data>> {
                    using T = std::decay_t<decltype(x)>;
                    if constexpr (std::is_same_v<T, typename GS::SubscriptionUpdate>) {
                        std::vector<ConsumerUpdate<Data>> result;
                        for (auto &item : x.data) {
                            std::visit([&result](auto &&y) {
                                using U = std::decay_t<decltype(y)>;
                                if constexpr (std::is_same_v<U, typename DI::OneFullUpdateItem>) {
                                    if (y.data) {
                                        for (auto &&dataPiece : *(y.data)) {
                                            result.push_back({{
                                                ConsumerInsertOrUpdateKey<Data> {
                                                    .key = dataPiece.first
                                                    , .data = std::move(dataPiece.second)
                                                }
                                            }});
                                        }
                                    }
                                } else if constexpr (std::is_same_v<U, typename DI::OneDeltaUpdateItem>) {
                                    for (auto const &deletedKey : std::get<2>(y).deletes) {
                                        result.push_back({{
                                            ConsumerDeleteKey {
                                                .key = deletedKey
                                            }
                                        }});
                                    }
                                    for (auto &&update : std::get<2>(y).inserts_updates) {
                                        result.push_back({{
                                            ConsumerInsertOrUpdateKey<Data> {
                                                .key = std::move(std::get<0>(update))
                                                , .data = std::move(std::get<1>(update))
                                            }
                                        }});
                                    }
                                }
                            }, std::move(item.value));
                        }
                        return result;
                    } else if constexpr (std::is_same_v<T, typename GS::Subscription>) {
                        if (hasExitDataSource) {
                            *idRecord = key.id();
                        }
                        return std::vector<ConsumerUpdate<Data>> {};
                    } else if constexpr (std::is_same_v<T, typename GS::Unsubscription>) {
                        if (hasExitDataSource) {
                            *idRecord = std::nullopt;
                            env->exit();
                        }
                        return std::vector<ConsumerUpdate<Data>> {};
                    } else {
                        return std::vector<ConsumerUpdate<Data>> {};
                    }
                }, std::move(input.data.value));
            }
        );
        r.registerAction(prefix+"/handler", handler);
        facilityInfo.feedOrderResults(r, r.actionAsSink(handler));

        if (exitSource) {
            auto locatorRecord = std::make_shared<std::optional<ConnectionLocator>>(std::nullopt);
            r.preservePointer(locatorRecord);

            auto saveLocator = infra::GenericLift<M>::lift(
                [locatorRecord](std::tuple<ConnectionLocator, bool> &&data) {
                    if (std::get<1>(data)) {
                        *locatorRecord = {std::move(std::get<0>(data))};
                    } else {
                        *locatorRecord = std::nullopt;
                    }
                }
            );
            r.registerExporter(prefix+"/saveLocator", saveLocator);
            facilityInfo.feedConnectionChanges(r, r.exporterAsSink(saveLocator));

            auto createUnsubscription = infra::GenericLift<M>::lift(
                [locatorRecord, idRecord, env](ExitSourceType &&) 
                    -> std::optional<typename M::template Key<std::tuple<ConnectionLocator, typename GS::Input>>>
                {
                    if (*locatorRecord && *idRecord) {
                        return {{{
                            **locatorRecord 
                            , typename GS::Input {
                                typename GS::Unsubscription {
                                    **idRecord
                                }
                            }
                        }}};
                    } else {
                        env->exit();
                        return std::nullopt;
                    }
                }
            );
            r.registerAction(prefix+"/createUnsubscription", createUnsubscription);
            facilityInfo.orderReceiver(r, r.execute(createUnsubscription, exitSource->clone()));
        }

        return r.actionAsSource(handler);
    }

} } } } }

TM_BASIC_CBOR_CAPABLE_STRUCT_SERIALIZE(dev::cd606::tm::transport::named_value_store_components::ConsumerDeleteKey, NAMED_VALUE_STORE_CONSUMER_DELETE_KEY_FIELDS);
TM_BASIC_CBOR_CAPABLE_TEMPLATE_STRUCT_SERIALIZE(((typename, Data)), dev::cd606::tm::transport::named_value_store_components::ConsumerInsertOrUpdateKey, NAMED_VALUE_STORE_CONSUMER_INSERT_OR_UPDATE_KEY_FIELDS);

#endif