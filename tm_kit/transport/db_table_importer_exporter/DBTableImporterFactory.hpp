#ifndef TM_KIT_TRANSPORT_DB_TABLE_IMPORTER_EXPORTER_DB_TABLE_IMPORTER_FACTORY_HPP_
#define TM_KIT_TRANSPORT_DB_TABLE_IMPORTER_EXPORTER_DB_TABLE_IMPORTER_FACTORY_HPP_

#include <tm_kit/infra/AppClassifier.hpp>
#include <tm_kit/basic/StructFieldInfoUtils.hpp>

#include <boost/algorithm/string.hpp>

#include <soci/soci.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace db_table_importer_exporter {
    template <class M>
    class DBTableImporterFactory {
    private:
        template <class T>
        static std::string selectStatement(std::string const &input) {
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<T>;
            std::string s = boost::trim_copy(input);
            std::string s1 = boost::to_upper_copy(s);
            if (boost::starts_with(s1, "SELECT ")) {
                return s;
            }
            if (boost::starts_with(s1, "FROM ")) {
                return ("SELECT "+DF::commaSeparatedFieldNamesForSelect()+" "+s);
            }
            return ("SELECT "+DF::commaSeparatedFieldNamesForSelect()+" FROM "+s);
        }
    public:
        template <class T, typename = std::enable_if_t<basic::StructFieldInfo<T>::HasGeneratedStructFieldInfo>>
        static auto getTableData(std::shared_ptr<soci::session> const &session, std::string const &importerInput)
            -> std::vector<T>
        {
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<T>;
            std::vector<T> value;
            soci::rowset<soci::row> queryRes = 
                session->prepare << selectStatement<T>(importerInput);
            for (auto const &r : queryRes) {
                value.push_back(DF::retrieveData(r,0));
            }
            return value;
        }
    private:
        template <class FirstT, class... RemainingTs>
        static constexpr bool allHaveGeneratedStructFieldInfo() {
            if constexpr (sizeof...(RemainingTs) == 0) {
                return true;
            } else {
                if constexpr (basic::StructFieldInfo<FirstT>::HasGeneratedStructFieldInfo) {
                    return allHaveGeneratedStructFieldInfo<RemainingTs...>();
                } else {
                    return false;
                }
            }
        }
        template <std::size_t Idx, class FirstT, class... RemainingTs>
        static void commaSeparatedFieldNamesForSelect_combined_internal(std::ostringstream &oss) {
            if constexpr (Idx > 0) {
                oss << ", ";
            }
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<FirstT>;
            oss << DF::commaSeparatedFieldNamesForSelect();
            if constexpr (sizeof...(RemainingTs) == 0) {
                return;
            } else {
                commaSeparatedFieldNamesForSelect_combined_internal<Idx+1, RemainingTs...>(oss);
            }
        }
        template <class... Ts>
        static std::string commaSeparatedFieldNamesForSelect_combined() {
            std::ostringstream oss;
            commaSeparatedFieldNamesForSelect_combined_internal<0, Ts...>(oss);
            return oss.str();
        }
        template <class... Ts>
        static std::string selectStatementForCombined(std::string const &input) {
            std::string s = boost::trim_copy(input);
            std::string s1 = boost::to_upper_copy(s);
            if (boost::starts_with(s1, "SELECT ")) {
                return s;
            }
            if (boost::starts_with(s1, "FROM ")) {
                return ("SELECT "+commaSeparatedFieldNamesForSelect_combined<Ts...>()+" "+s);
            }
            return ("SELECT "+commaSeparatedFieldNamesForSelect_combined<Ts...>()+" FROM "+s);
        }
        template <std::size_t Idx1, std::size_t Idx2, class Tuple, class FirstT, class... RemainingTs>
        static void retrieveDataForCombined_internal(Tuple &output, soci::row const &r) {
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<FirstT>;
            std::get<Idx1>(output) = DF::retrieveData(r, Idx2);
            if constexpr (sizeof...(RemainingTs) == 0) {
                return;
            } else {
                retrieveDataForCombined_internal<Idx1+1,Idx2+basic::StructFieldInfo<FirstT>::FIELD_NAMES.size(),Tuple,RemainingTs...>(output, r);
            }
        }
        template <class... Ts>
        static void retrieveDataForCombined(std::tuple<Ts...> &output, soci::row const &r) {
            retrieveDataForCombined_internal<0, 0, std::tuple<Ts...>, Ts...>(output, r);
        }
    public:
        template <class... Ts>
        static auto getTableDataForCombined(std::shared_ptr<soci::session> const &session, std::string const &importerInput)
            -> std::vector<std::tuple<Ts...>>
        {
            static_assert(allHaveGeneratedStructFieldInfo<Ts...>(), "the types must all have generated struct field info");
            std::vector<std::tuple<Ts...>> value;
            soci::rowset<soci::row> queryRes = 
                session->prepare << selectStatementForCombined<Ts...>(importerInput);
            for (auto const &r : queryRes) {
                std::tuple<Ts...> v;
                retrieveDataForCombined<Ts...>(v, r);
                value.push_back(std::move(v));
            }
            return value;
        }
    public:
        template <class T, typename = std::enable_if_t<basic::StructFieldInfo<T>::HasGeneratedStructFieldInfo>>
        static auto createImporter(std::shared_ptr<soci::session> const &session, std::string const &importerInput)
            -> std::shared_ptr<typename M::template Importer<std::vector<T>>>
        {
            return M::template uniformSimpleImporter<std::vector<T>>(
                [session,importerInput](
                    typename M::EnvironmentType *env
                ) -> std::tuple<bool, typename M::template Data<std::vector<T>>> {
                    using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<T>;
                    std::vector<T> value;
                    soci::rowset<soci::row> queryRes = 
                        session->prepare << selectStatement<T>(importerInput);
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
        static auto createRepeatedImporter(std::shared_ptr<soci::session> const &session, std::string const &importerInput, std::chrono::system_clock::duration const &interval)
            -> std::shared_ptr<typename M::template Importer<std::vector<T>>>
        {
            static_assert((infra::app_classification_v<M> == infra::AppClassification::RealTime), "repeated db table importer is only supported in real time mode");
            return M::template uniformSimpleImporter<std::vector<T>>(
                [session,importerInput,interval](
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
                            session->prepare << selectStatement<T>(importerInput);
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
        static auto createRepeatedImporterWithKeyCheck(std::shared_ptr<soci::session> const &session, std::string const &importerInput, std::chrono::system_clock::duration const &interval, KeyExtractor &&keyExtractor)
            -> std::shared_ptr<typename M::template Importer<std::vector<T>>>
        {
            static_assert((infra::app_classification_v<M> == infra::AppClassification::RealTime), "repeated db table importer with key check is only supported in real time mode");
            return M::template uniformSimpleImporter<std::vector<T>>(
                [session,importerInput,interval,keyExtractor=std::move(keyExtractor)](
                    typename M::EnvironmentType *env
                ) mutable -> std::tuple<bool, typename M::template Data<std::vector<T>>> {
                    using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<T>;
                    using Key = std::decay_t<decltype(keyExtractor(std::declval<T>()))>;
                    static bool first = true;
                    static std::unordered_set<Key, basic::struct_field_info_utils::StructFieldInfoBasedHash<Key>> seen;
                    
                    if (!first) {
                        std::this_thread::sleep_for(interval);
                    }
                    first = false;
                    try {
                        std::vector<T> value;
                        soci::rowset<soci::row> queryRes = 
                            session->prepare << selectStatement<T>(importerInput);
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
