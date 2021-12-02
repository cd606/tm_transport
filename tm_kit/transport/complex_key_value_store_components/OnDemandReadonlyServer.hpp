#ifndef TM_KIT_TRANSPORT_COMPLEX_KEY_VALUE_STORE_COMPONENTS_ON_DEMAND_READ_ONLY_SERVER_HPP_
#define TM_KIT_TRANSPORT_COMPLEX_KEY_VALUE_STORE_COMPONENTS_ON_DEMAND_READ_ONLY_SERVER_HPP_

#include <tm_kit/basic/CalculationsOnInit.hpp>
#include <tm_kit/basic/StructFieldInfoUtils.hpp>
#include <tm_kit/basic/transaction/complex_key_value_store/VersionlessDataModel.hpp>

#include <soci/soci.h>

#include <iostream>
#include <sstream>

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace complex_key_value_store_components {

    template <class M>
    class OnDemandReadonlyServer {
    private:
        template <class ItemKey, int FieldCount, int FieldIndex>
        static std::string sociWhereClause_internal() {
            if constexpr (FieldIndex>=0 && FieldIndex<FieldCount) {
                std::ostringstream oss;
                if constexpr (FieldIndex > 0) {
                    oss << " AND ";
                }
                oss << basic::StructFieldInfo<ItemKey>::FIELD_NAMES[FieldIndex] 
                    << " = :"
                    << basic::StructFieldInfo<ItemKey>::FIELD_NAMES[FieldIndex];
                if constexpr (FieldIndex < FieldCount-1) {
                    oss << sociWhereClause_internal<ItemKey,FieldCount,FieldIndex+1>();
                }
                return oss.str();
            } else {
                return "";
            }
        }
        template <class ItemKey>
        static std::string sociWhereClause() {
            return sociWhereClause_internal<ItemKey, basic::StructFieldInfo<ItemKey>::FIELD_NAMES.size(), 0>();
        }
        template <class ItemKey, int FieldCount, int FieldIndex>
        static void sociBindWhereClause_internal(soci::statement &stmt, ItemKey const &k) {
            if constexpr (FieldIndex>=0 && FieldIndex<FieldCount) {
                stmt.exchange(soci::use(k.*(basic::StructFieldTypeInfo<ItemKey,FieldIndex>::fieldPointer()), std::string(basic::StructFieldInfo<ItemKey>::FIELD_NAMES[FieldIndex])));
                if constexpr (FieldIndex < FieldCount-1) {
                    sociBindWhereClause_internal<ItemKey,FieldCount,FieldIndex+1>(stmt, k);
                }
            }
        }
        template <class ItemKey>
        static void sociBindWhereClause(soci::statement &stmt, ItemKey const &k) {
            sociBindWhereClause_internal<ItemKey, basic::StructFieldInfo<ItemKey>::FIELD_NAMES.size(), 0>(stmt, k);
        }
    public:
        template <class ItemKey, class ItemData>
        static auto keyBasedQueryFunc(
            std::shared_ptr<soci::session> const &session
            , std::function<std::string(std::string const &)> selectMainPartFromCriteria
        )
        {
            using DBDataStorage = basic::transaction::complex_key_value_store::as_collection::Collection<ItemKey,ItemData>;
            using KF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemKey>;
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemData>;
            auto x = selectMainPartFromCriteria(OnDemandReadonlyServer<M>::template sociWhereClause<ItemKey>());
            if (!boost::starts_with(boost::to_upper_copy(boost::trim_copy(x)), "FROM ")) {
                x = "FROM "+x;
            }
            std::string query = "SELECT "+DF::commaSeparatedFieldNames()+" "+x;
            return [query,session](ItemKey &&k) -> basic::transaction::complex_key_value_store::KeyBasedQueryResult<ItemData>
            {
                try {
                    soci::row r;
                    soci::statement stmt(*session);
                    stmt.alloc();
                    stmt.prepare(query);
                    stmt.exchange(soci::into(r));
                    OnDemandReadonlyServer<M>::template sociBindWhereClause<ItemKey>(stmt, k);
                    stmt.define_and_bind();
                    stmt.execute(false);
                    if (stmt.fetch()) {
                        return {DF::retrieveData(r)};
                    } else {
                        return {std::nullopt};
                    }
                } catch (soci::soci_error const &) {
                    return {std::nullopt};
                }
            };
        }

        template <class ItemKey, class ItemData>
        static auto keyBasedQueryFacility(
            std::shared_ptr<soci::session> const &session
            , std::function<std::string(std::string const &)> selectMainPartFromCriteria
        ) -> std::shared_ptr<typename M::template OnOrderFacility<ItemKey, basic::transaction::complex_key_value_store::KeyBasedQueryResult<ItemData>>>
        {
            return M::template liftPureOnOrderFacility<ItemKey>(
                keyBasedQueryFunc<ItemKey,ItemData>(
                    session, selectMainPartFromCriteria
                )
            );
        }

        template <class ItemKey, class ItemData, class QueryType=basic::VoidStruct>
        static auto fullDataQueryFunc(
            std::shared_ptr<soci::session> const &session
            , std::string const &selectMainPart
        ) 
        {
            using KF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemKey>;
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemData>;
            auto x = selectMainPart;
            if (!boost::starts_with(boost::to_upper_copy(boost::trim_copy(x)), "FROM ")) {
                x = "FROM "+x;
            }
            std::string query = ("SELECT "+KF::commaSeparatedFieldNames()+", "+DF::commaSeparatedFieldNames()+" "+x);
            return [query,session](QueryType &&) 
                -> basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData>
            {
                try {
                    soci::rowset<soci::row> res = 
                        session->prepare << query;
                    basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData> ret;
                    for (auto const &r : res) {
                        ret.insert({KF::retrieveData(r,0), DF::retrieveData(r,KF::FieldCount)});
                    }
                    return ret;
                } catch (soci::soci_error const &) {
                    return {};
                }
            };
        }

        template <class ItemKey, class ItemData, class QueryType=basic::VoidStruct>
        static auto fullDataQueryFacility(
            std::shared_ptr<soci::session> const &session
            , std::string const &selectMainPart
        ) -> std::shared_ptr<typename M::template OnOrderFacility<QueryType, basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData>>>
        {
            return M::template liftPureOnOrderFacility<QueryType>(
                fullDataQueryFunc<ItemKey,ItemData,QueryType>(
                    session, selectMainPart
                )
            );
        }

        template <class ItemKey, class ItemData, class QueryType>
        static auto dynamicDataQueryFunc(
            std::shared_ptr<soci::session> const &session
            , std::string const &selectMainPart
            , std::function<std::string(QueryType &&)> dynamicWhereFunc
        ) 
        {
            using KF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemKey>;
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemData>;
            auto x = selectMainPart;
            
            std::string query;
            if (boost::starts_with(boost::to_upper_copy(boost::trim_copy(x)), "SELECT ")) {
                query = x;
            } else {
                if (!boost::starts_with(boost::to_upper_copy(boost::trim_copy(x)), "FROM ")) {
                    x = "FROM "+x;
                }
                query = ("SELECT "+KF::commaSeparatedFieldNames()+", "+DF::commaSeparatedFieldNames()+" "+x);
            }
            return [query,session,dynamicWhereFunc](QueryType &&q) 
                -> basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData>
            {
                try {
                    std::string fullQuery = query+" WHERE "+dynamicWhereFunc(std::move(q));
                    soci::rowset<soci::row> res = 
                        session->prepare << fullQuery;
                    basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData> ret;
                    for (auto const &r : res) {
                        ret.insert({KF::retrieveData(r,0), DF::retrieveData(r,KF::FieldCount)});
                    }
                    return ret;
                } catch (soci::soci_error const &) {
                    return {};
                }
            };
        }

        template <class ItemKey, class ItemData, class QueryType>
        static auto dynamicDataQueryFacility(
            std::shared_ptr<soci::session> const &session
            , std::string const &selectMainPart
            , std::function<std::string(QueryType &&)> dynamicWhereFunc
        ) -> std::shared_ptr<typename M::template OnOrderFacility<QueryType, basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData>>>
        {
            return M::template liftPureOnOrderFacility<QueryType>(
                dynamicDataQueryFunc<ItemKey,ItemData,QueryType>(
                    session, selectMainPart, dynamicWhereFunc
                )
            );
        }

        template <class RowType, class QueryType>
        static auto dynamicDataQueryFunc_VectorVersion(
            std::shared_ptr<soci::session> const &session
            , std::string const &selectMainPart
            , std::function<std::string(QueryType &&)> dynamicWhereFunc
        ) 
        {
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<RowType>;
            auto x = selectMainPart;
            
            std::string query;
            if (boost::starts_with(boost::to_upper_copy(boost::trim_copy(x)), "SELECT ")) {
                query = x;
            } else {
                if (!boost::starts_with(boost::to_upper_copy(boost::trim_copy(x)), "FROM ")) {
                    x = "FROM "+x;
                }
                query = ("SELECT "+DF::commaSeparatedFieldNames()+" "+x);
            }
            return [query,session,dynamicWhereFunc](QueryType &&q) 
                -> std::vector<RowType>
            {
                try {
                    std::string fullQuery = query+" WHERE "+dynamicWhereFunc(std::move(q));
                    soci::rowset<soci::row> res = 
                        session->prepare << fullQuery;
                    std::vector<RowType> ret;
                    for (auto const &r : res) {
                        ret.push_back(DF::retrieveData(r,0));
                    }
                    return ret;
                } catch (soci::soci_error const &) {
                    return {};
                }
            };
        }

        template <class RowType, class QueryType>
        static auto dynamicDataQueryFacility_VectorVersion(
            std::shared_ptr<soci::session> const &session
            , std::string const &selectMainPart
            , std::function<std::string(QueryType &&)> dynamicWhereFunc
        ) -> std::shared_ptr<typename M::template OnOrderFacility<QueryType, std::vector<RowType>>>
        {
            return M::template liftPureOnOrderFacility<QueryType>(
                dynamicDataQueryFunc_VectorVersion<RowType,QueryType>(
                    session, selectMainPart, dynamicWhereFunc
                )
            );
        }
    };

}}}}}

#endif