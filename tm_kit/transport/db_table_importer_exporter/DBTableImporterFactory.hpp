#ifndef TM_KIT_TRANSPORT_DB_TABLE_IMPORTER_EXPORTER_DB_TABLE_IMPORTER_FACTORY_HPP_
#define TM_KIT_TRANSPORT_DB_TABLE_IMPORTER_EXPORTER_DB_TABLE_IMPORTER_FACTORY_HPP_

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
                    auto t = std::chrono::steady_clock::now();
                    std::vector<T> value;
                    soci::rowset<soci::row> queryRes = 
                        session->prepare << ("SELECT "+DF::commaSeparatedFieldNames()+" FROM "+tableName);
                    for (auto const &r : queryRes) {
                        value.push_back(DF::retrieveData(r,0));
                    }
                    auto t1 = std::chrono::steady_clock::now();
                    std::cerr << "Time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t1-t).count() << '\n';
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
    };
} } } } }

#endif