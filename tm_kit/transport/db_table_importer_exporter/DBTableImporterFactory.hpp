#ifndef TM_KIT_TRANSPORT_DB_TABLE_IMPORTER_EXPORTER_DB_TABLE_IMPORTER_FACTORY_HPP_
#define TM_KIT_TRANSPORT_DB_TABLE_IMPORTER_EXPORTER_DB_TABLE_IMPORTER_FACTORY_HPP_

#include <tm_kit/infra/AppClassifier.hpp>
#include <tm_kit/basic/StructFieldInfoUtils.hpp>

#include <soci/soci.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace db_table_importer_exporter {
    template <class M>
    class DBTableImporterFactory {
    public:
        template <class T, typename = std::enable_if_t<basic::StructFieldInfo<T>::HasGeneratedStructFieldInfo>>
        static auto createImporter(std::shared_ptr<soci::session> const &session, std::string const &tableName)
            -> std::shared_ptr<typename M::template Importer<std::vector<T>>>
        {
            return M::template uniformSimpleImporter<std::vector<T>>(
                [session,tableName](
                    typename M::EnvironmentType *env
                ) -> std::tuple<bool, typename M::template Data<std::vector<T>>> {
                    using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<T>;
                    std::vector<T> value;
                    soci::rowset<soci::row> queryRes = 
                        session->prepare << ("SELECT "+DF::commaSeparatedFieldNames()+" FROM "+tableName);
                    for (auto const &r : queryRes) {
                        value.push_back(DF::retrieveData(r,0));
                    }
                    return {
                        false 
                        , typename M::template InnerData<std::vector<T>> {
                            env
                            , {
                                env->resolveTime()
                                , std::move(value)
                                , true
                            }
                        }
                    };
                }
            );
        }
        template <class T, typename = std::enable_if_t<basic::StructFieldInfo<T>::HasGeneratedStructFieldInfo>>
        static auto createRepeatedImporter(std::shared_ptr<soci::session> const &session, std::string const &tableName, std::chrono::system_clock::duration const &interval)
            -> std::shared_ptr<typename M::template Importer<std::vector<T>>>
        {
            static_assert((infra::app_classification_v<M> == infra::AppClassification::RealTime), "repeated db table importer is only supported in real time mode");
            return M::template uniformSimpleImporter<std::vector<T>>(
                [session,tableName,interval](
                    typename M::EnvironmentType *env
                ) -> std::tuple<bool, typename M::template Data<std::vector<T>>> {
                    using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<T>;
                    static bool first = true;
                    if (!first) {
                        std::this_thread::sleep_for(interval);
                    }
                    first = false;
                    try {
                        std::vector<T> value;
                        soci::rowset<soci::row> queryRes = 
                            session->prepare << ("SELECT "+DF::commaSeparatedFieldNames()+" FROM "+tableName);
                        for (auto const &r : queryRes) {
                            value.push_back(DF::retrieveData(r,0));
                        }
                        return {
                            true 
                            , typename M::template InnerData<std::vector<T>> {
                                env
                                , {
                                    env->resolveTime()
                                    , std::move(value)
                                    , false
                                }
                            }
                        };
                    } catch (std::exception const &) {
                        return {true, std::nullopt};
                    }
                }
            );
        }
        template <class T, class KeyExtractor, typename = std::enable_if_t<basic::StructFieldInfo<T>::HasGeneratedStructFieldInfo>>
        static auto createRepeatedImporterWithKeyCheck(std::shared_ptr<soci::session> const &session, std::string const &tableName, std::chrono::system_clock::duration const &interval, KeyExtractor &&keyExtractor)
            -> std::shared_ptr<typename M::template Importer<std::vector<T>>>
        {
            static_assert((infra::app_classification_v<M> == infra::AppClassification::RealTime), "repeated db table importer with key check is only supported in real time mode");
            return M::template uniformSimpleImporter<std::vector<T>>(
                [session,tableName,interval,keyExtractor=std::move(keyExtractor)](
                    typename M::EnvironmentType *env
                ) mutable -> std::tuple<bool, typename M::template Data<std::vector<T>>> {
                    using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<T>;
                    using Key = std::decay_t<decltype(keyExtractor(std::declval<T>()))>;
                    static bool first = true;
                    static std::unordered_set<Key> seen;
                    
                    if (!first) {
                        std::this_thread::sleep_for(interval);
                    }
                    first = false;
                    try {
                        std::vector<T> value;
                        soci::rowset<soci::row> queryRes = 
                            session->prepare << ("SELECT "+DF::commaSeparatedFieldNames()+" FROM "+tableName);
                        for (auto const &r : queryRes) {
                            auto d = DF::retrieveData(r, 0);
                            auto k = keyExtractor(d);
                            if (seen.find(k) == seen.end()) {
                                value.push_back(std::move(d));
                                seen.insert(k);
                            }
                        }
                        return {
                            true 
                            , typename M::template InnerData<std::vector<T>> {
                                env
                                , {
                                    env->resolveTime()
                                    , std::move(value)
                                    , false
                                }
                            }
                        };
                    } catch (std::exception const &) {
                        return {true, std::nullopt};
                    }
                }
            );
        }
    };
} } } } }

#endif