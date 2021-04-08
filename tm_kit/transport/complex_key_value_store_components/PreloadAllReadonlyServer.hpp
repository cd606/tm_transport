#ifndef TM_KIT_TRANSPORT_COMPLEX_KEY_VALUE_STORE_COMPONENTS_ON_DEMAND_READ_ONLY_SERVER_HPP_
#define TM_KIT_TRANSPORT_COMPLEX_KEY_VALUE_STORE_COMPONENTS_ON_DEMAND_READ_ONLY_SERVER_HPP_

#include <tm_kit/basic/CalculationsOnInit.hpp>
#include <tm_kit/basic/StructFieldInfoUtils.hpp>
#include <tm_kit/basic/transaction/complex_key_value_store/VersionlessDataModel.hpp>

#include <soci/soci.h>

#include <iostream>
#include <sstream>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace complex_key_value_store_components {

    template <class M>
    class PreloadAllReadonlyServer {
    public:
        template <class ItemKey, class ItemData>
        static auto keyBasedQueryFacility(
            std::shared_ptr<soci::session> const &session
            , std::string const &selectMainPart
        ) -> std::shared_ptr<typename M::template OnOrderFacility<ItemKey, basic::transaction::complex_key_value_store::KeyBasedQueryResult<ItemData>>>
        {
            using DBDataStorage = basic::transaction::complex_key_value_store::Collection<ItemKey,ItemData>;
            return basic::onOrderFacilityUsingInternallyPreCalculatedValue<M,ItemKey>(
                [session,selectMainPart](std::function<void(infra::LogLevel, std::string const &)> logger)
                    -> DBDataStorage
                {
                    using KF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemKey>;
                    using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemData>;
                    soci::rowset<soci::row> res = 
                        session->prepare << ("SELECT "+KF::commaSeparatedFieldNames()+", "+DF::commaSeparatedFieldNames()+" "+selectMainPart);
                    DBDataStorage ret;
                    for (auto const &r : res) {
                        ret.insert({KF::retrieveData(r,0), DF::retrieveData(r,KF::FieldCount)});
                    }
                    if (logger) {
                        std::ostringstream oss;
                        oss << "[PreloadAllReadonlyServer::keyBasedQueryFacility::initFunc] loaded " << ret.size() << " rows";
                        logger(infra::LogLevel::Info, oss.str());
                    }
                    return ret;
                }
                , [](DBDataStorage const &storage, ItemKey const &key) -> basic::transaction::complex_key_value_store::KeyBasedQueryResult<ItemData> {
                    auto iter = storage.find(key);
                    if (iter == storage.end()) {
                        return {std::nullopt};
                    } else {
                        return {iter->second};
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
            return basic::onOrderFacilityReturningInternallyPreCalculatedValue<M,QueryType>(
                [session,selectMainPart](std::function<void(infra::LogLevel, std::string const &)> logger)
                    -> basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData>
                {
                    using KF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemKey>;
                    using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemData>;
                    soci::rowset<soci::row> res = 
                        session->prepare << ("SELECT "+KF::commaSeparatedFieldNames()+", "+DF::commaSeparatedFieldNames()+" "+selectMainPart);
                    basic::transaction::complex_key_value_store::FullDataResult<ItemKey,ItemData> ret;
                    for (auto const &r : res) {
                        ret.value.insert({KF::retrieveData(r,0), DF::retrieveData(r,KF::FieldCount)});
                    }
                    if (logger) {
                        std::ostringstream oss;
                        oss << "[PreloadAllReadonlyServer::keyBasedQueryFacility::initFunc] loaded " << ret.value.size() << " rows";
                        logger(infra::LogLevel::Info, oss.str());
                    }
                    return ret;
                }
            );
        }
    };

}}}}}

#endif