#ifndef TM_KIT_TRANSPORT_COMPLEX_KEY_VALUE_STORE_COMPONENTS_PRELOAD_ALL_READ_ONLY_SERVER_HPP_
#define TM_KIT_TRANSPORT_COMPLEX_KEY_VALUE_STORE_COMPONENTS_PRELOAD_ALL_READ_ONLY_SERVER_HPP_

#include <tm_kit/basic/CalculationsOnInit.hpp>
#include <tm_kit/basic/StructFieldInfoUtils.hpp>
#include <tm_kit/basic/transaction/complex_key_value_store/VersionlessDataModel.hpp>

#include <soci/soci.h>

#include <iostream>
#include <sstream>

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
            sociBindWhereClause_internal<ItemKey, basic::StructFieldInfo<ItemKey>::FIELD_NAMES.size(), 0>(stmt, std::move(k));
        }
    public:
        template <class ItemKey, class ItemData>
        static auto keyBasedQueryFacility(
            std::shared_ptr<soci::session> const &session
            , std::function<std::string(std::string const &)> selectMainPartFromCriteria
        ) -> std::shared_ptr<typename M::template OnOrderFacility<ItemKey, basic::transaction::complex_key_value_store::KeyBasedQueryResult<ItemData>>>
        {
            using DBDataStorage = basic::transaction::complex_key_value_store::Collection<ItemKey,ItemData>;
            using KF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemKey>;
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemData>;
            std::string query = "SELECT "+DF::commaSeparatedFieldNames()+" "+selectMainPartFromCriteria(OnDemandReadonlyServer<M>::template sociWhereClause<ItemKey>());
            return M::template liftPureOnOrderFacility<ItemKey>(
                [query,session](ItemKey &&k) -> basic::transaction::complex_key_value_store::KeyBasedQueryResult<ItemData>
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
                }
            );
        }

        template <class ItemKey, class ItemData, class QueryType=basic::CBOR<basic::VoidStruct>>
        static auto fullDataQueryFacility(
            std::shared_ptr<soci::session> const &session
            , std::string const &selectMainPart
        ) -> std::shared_ptr<typename M::template OnOrderFacility<QueryType, basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData>>>
        {
            using KF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemKey>;
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemData>;
            std::string query = ("SELECT "+KF::commaSeparatedFieldNames()+", "+DF::commaSeparatedFieldNames()+" "+selectMainPart);
            return M::template liftPureOnOrderFacility<QueryType>(
                [query,session](QueryType &&) 
                    -> basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData>
                {
                    try {
                        soci::rowset<soci::row> res = 
                            session->prepare << query;
                        basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData> ret;
                        for (auto const &r : res) {
                            ret.value.insert({KF::retrieveData(r,0), DF::retrieveData(r,KF::FieldCount)});
                        }
                        return ret;
                    } catch (soci::soci_error const &) {
                        return {};
                    }
                }
            );
        }
    };

}}}}}

#endif