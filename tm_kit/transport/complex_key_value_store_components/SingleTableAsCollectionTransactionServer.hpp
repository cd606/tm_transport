#ifndef TM_KIT_TRANSPORT_COMPLEX_KEY_VALUE_STORE_COMPONENTS_SINGLE_TABLE_AS_COLLECTION_TRANSACTION_SERVER_HPP_
#define TM_KIT_TRANSPORT_COMPLEX_KEY_VALUE_STORE_COMPONENTS_SINGLE_TABLE_AS_COLLECTION_TRANSACTION_SERVER_HPP_

#include <tm_kit/basic/CalculationsOnInit.hpp>
#include <tm_kit/basic/StructFieldInfoUtils.hpp>
#include <tm_kit/basic/transaction/complex_key_value_store/VersionlessDataModel.hpp>
#include <tm_kit/basic/transaction/v2/TransactionLogicCombination.hpp>

#include <soci/soci.h>

#include <iostream>
#include <sstream>

#include <boost/algorithm/string.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace complex_key_value_store_components {

    template <class ItemKey, class ItemData>
    class SingleTableAsCollectionTransactionServer {
    public:
        using DI = basic::transaction::complex_key_value_store::as_collection::DI<ItemKey, ItemData>;
        using TI = basic::transaction::complex_key_value_store::as_collection::TI<ItemKey, ItemData>;
        template <class IDType>
        using GS = basic::transaction::complex_key_value_store::as_collection::GS<IDType, ItemKey, ItemData>;

        using KF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemKey>;
        using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemData>;
                
        class DSComponent : public basic::transaction::v2::DataStreamEnvComponent<DI> {
        private:
            std::shared_ptr<soci::session> session_;
            std::string tableName_;
            std::function<void(std::string)> logger_;
            std::optional<std::string> whereClause_;
            typename basic::transaction::v2::DataStreamEnvComponent<DI>::Callback *cb_;
        public:
            DSComponent() : session_(), tableName_(), logger_(), whereClause_(std::nullopt) {
            }
            DSComponent(std::shared_ptr<soci::session> const &session, std::string const &tableName, std::function<void(std::string)> const &logger, std::optional<std::string> const &whereClause) : session_(session), tableName_(tableName), logger_(logger), whereClause_(whereClause) {
            }
            DSComponent(DSComponent &&c) : session_(std::move(c.session_)), tableName_(std::move(c.tableName_)), logger_(std::move(c.logger_)), whereClause_(std::move(c.whereClause_)) {}
            DSComponent &operator=(DSComponent &&c) {
                if (this != &c) {
                    session_ = std::move(c.session_);
                    tableName_ = std::move(c.tableName_);
                    logger_ = std::move(c.logger_);
                    whereClause_ = std::move(c.whereClause_);
                }
                return *this;
            }
            virtual ~DSComponent() {}
            void initialize(typename basic::transaction::v2::DataStreamEnvComponent<DI>::Callback *cb) {
                cb_ = cb;
                typename DI::Data initialData;

                soci::rowset<soci::row> res = 
                    session_->prepare << 
                        (
                            (whereClause_ && boost::starts_with(boost::to_upper_copy(boost::trim_copy(*whereClause_)), "SELECT "))
                            ?
                            *whereClause_
                            :
                            ("SELECT "+KF::commaSeparatedFieldNames()+", "+DF::commaSeparatedFieldNames()+" FROM "+tableName_+(whereClause_?(" WHERE "+*whereClause_):std::string{}))
                        );
                for (auto const &r : res) {
                    initialData.insert({
                        KF::retrieveData(r,0)
                        , DF::retrieveData(r, KF::FieldCount)
                    });
                }
                std::ostringstream oss;
                oss << "[SingleTableAsCollectionTransactionServer::DSComponent::initialize] loaded " << initialData.size() << " rows";
                logger_(oss.str());
                cb_->onUpdate(typename DI::Update {
                    basic::ConstType<0> {}
                    , std::vector<typename DI::OneUpdateItem> {
                        { typename DI::OneFullUpdateItem {
                            {}
                            , basic::ConstType<0> {}
                            , std::move(initialData)
                        } }
                    }
                });
            }
            typename basic::transaction::v2::DataStreamEnvComponent<DI>::Callback *callback() const {
                return cb_;
            }
        };
        class THComponent : public basic::transaction::v2::TransactionEnvComponent<TI> {
        private:
            std::shared_ptr<soci::session> session_;
            std::string tableName_;
            std::function<void(std::string)> logger_;
            std::function<bool(std::string const &)> accountFilter_;
            DSComponent *dsComponent_;

            void triggerCallback(typename TI::TransactionResponse const &resp, typename TI::Key const &key, typename TI::DataDelta const &dataDelta) {
                dsComponent_->callback()->onUpdate(typename DI::Update {
                    basic::ConstType<0> {}
                    , std::vector<typename DI::OneUpdateItem> {
                        { typename DI::OneDeltaUpdateItem {
                            key
                            , basic::ConstType<0> {}
                            , dataDelta
                        } } 
                    } 
                });
            }
            std::string insertTemplate() const {
                std::ostringstream oss;
                oss << "INSERT INTO " << tableName_ << '(';
                oss << KF::commaSeparatedFieldNames();
                oss << ',';
                oss << DF::commaSeparatedFieldNames();
                oss << ") VALUES (";
                bool begin = true;
                for (auto const &s : basic::StructFieldInfo<ItemKey>::FIELD_NAMES) {
                    if (!begin) {
                        oss << ',';
                    }
                    begin = false;
                    oss << ':' << s;
                }
                for (auto const &s : basic::StructFieldInfo<ItemData>::FIELD_NAMES) {
                    oss << ',';
                    oss << ':' << s;
                }
                oss << ')';
                return oss.str();
            }
            std::string deleteTemplate() const {
                std::ostringstream oss;
                oss << "DELETE FROM " << tableName_ << " WHERE ";
                bool begin = true;
                for (auto const &s : basic::StructFieldInfo<ItemKey>::FIELD_NAMES) {
                    if (!begin) {
                        oss << " AND ";
                    }
                    begin = false;
                    oss << s << " = :" << s;
                }
                return oss.str();
            }
            template <int FieldCount, int FieldIndex>
            static void sociBindKey_internal(soci::statement &stmt, std::vector<ItemKey> const &k, std::vector<std::function<void()>> &deletors) {
                if constexpr (FieldIndex>=0 && FieldIndex<FieldCount) {
                    auto *v = new std::vector<typename basic::StructFieldTypeInfo<ItemKey,FieldIndex>::TheType>();
                    for (auto const &x : k) {
                        v->push_back(x.*(basic::StructFieldTypeInfo<ItemKey,FieldIndex>::fieldPointer()));
                    }
                    stmt.exchange(soci::use(*v, std::string(basic::StructFieldInfo<ItemKey>::FIELD_NAMES[FieldIndex])));
                    deletors.push_back([v]() {delete v;});
                    if constexpr (FieldIndex < FieldCount-1) {
                        sociBindKey_internal<FieldCount,FieldIndex+1>(stmt, k, deletors);
                    }
                }
            }
            static void sociBindKey(soci::statement &stmt, std::vector<ItemKey> const &k, std::vector<std::function<void()>> &deletors) {
                sociBindKey_internal<basic::StructFieldInfo<ItemKey>::FIELD_NAMES.size(), 0>(stmt, k, deletors);
            }
            template <int FieldCount, int FieldIndex>
            static void sociBindData_internal(soci::statement &stmt, std::vector<ItemData> const &d, std::vector<std::function<void()>> &deletors) {
                if constexpr (FieldIndex>=0 && FieldIndex<FieldCount) {
                    auto * v= new std::vector<typename basic::StructFieldTypeInfo<ItemData,FieldIndex>::TheType>();
                    for (auto const &x : d) {
                        v->push_back(x.*(basic::StructFieldTypeInfo<ItemData,FieldIndex>::fieldPointer()));
                    }
                    stmt.exchange(soci::use(*v, std::string(basic::StructFieldInfo<ItemData>::FIELD_NAMES[FieldIndex])));
                    deletors.push_back([v]() {delete v;});
                    if constexpr (FieldIndex < FieldCount-1) {
                        sociBindData_internal<FieldCount,FieldIndex+1>(stmt, d, deletors);
                    }
                }
            }
            static void sociBindData(soci::statement &stmt, std::vector<ItemData> const &d, std::vector<std::function<void()>> &deletors) {
                sociBindData_internal<basic::StructFieldInfo<ItemData>::FIELD_NAMES.size(), 0>(stmt, d, deletors);
            }
        public:
            THComponent() : session_(), tableName_(), logger_(), accountFilter_(), dsComponent_(nullptr) {
            }
            THComponent(std::shared_ptr<soci::session> const &session, std::string const &tableName, std::function<void(std::string)> const &logger, std::function<bool(std::string const &)> const &accountFilter, DSComponent *dsComponent) : session_(session), tableName_(tableName), logger_(logger), accountFilter_(accountFilter), dsComponent_(dsComponent) {
            }
            THComponent(THComponent &&c) : session_(std::move(c.session_)), tableName_(std::move(c.tableName_)), logger_(std::move(c.logger_)), accountFilter_(std::move(c.accountFilter_)), dsComponent_(c.dsComponent_) {}
            THComponent &operator=(THComponent &&c) {
                if (this != &c) {
                    session_ = std::move(c.session_);
                    tableName_ = std::move(c.tableName_);
                    logger_ = std::move(c.logger_);
                    accountFilter_ = std::move(c.accountFilter_);
                    dsComponent_ = c.dsComponent_;
                }
                return *this;
            }
            virtual ~THComponent() {
            }
            typename TI::GlobalVersion acquireLock(std::string const &account, typename TI::Key const &, typename TI::DataDelta const *) override final {
                return basic::ConstType<0> {};
            }
            typename TI::GlobalVersion releaseLock(std::string const &account, typename TI::Key const &, typename TI::DataDelta const *) override final {
                return basic::ConstType<0> {};
            }
            typename TI::TransactionResponse handleInsert(std::string const &account, typename TI::Key const &key, typename TI::Data const &data) override final {
                return {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::FailureConsistency};
            }
            typename TI::TransactionResponse handleUpdate(std::string const &account, typename TI::Key const &key, std::optional<typename TI::VersionSlice> const &updateVersionSlice, typename TI::ProcessedUpdate const &processedUpdate) override final {
                static std::string del_templ = "";
                static std::string ins_templ = "";
                if (accountFilter_) {
                    if (!accountFilter_(account)) {
                        return {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::FailurePermission};
                    }
                }
                if (session_) {
                    std::unordered_set<ItemKey, basic::struct_field_info_utils::StructFieldInfoBasedHash<ItemKey>> keysToDelete;
                    std::unordered_map<ItemKey, ItemData, basic::struct_field_info_utils::StructFieldInfoBasedHash<ItemKey>> valuesToInsert;
                    for (auto const &key : processedUpdate.deletes) {
                        keysToDelete.insert(key);
                    }
                    for (auto const &item : processedUpdate.inserts_updates) {
                        keysToDelete.insert(std::get<0>(item));
                        valuesToInsert[std::get<0>(item)] = std::get<1>(item);
                    }

                    std::vector<ItemKey> keysToDeleteVec {keysToDelete.begin(), keysToDelete.end()};
                    std::vector<ItemKey> keysToInsertVec;
                    std::vector<ItemData> datasToInsertVec;
                    for (auto const &item : valuesToInsert) {
                        keysToInsertVec.push_back(item.first);
                        datasToInsertVec.push_back(item.second);
                    }
                    keysToDelete.clear();
                    valuesToInsert.clear();

                    if (!keysToDeleteVec.empty()) {
                        std::vector<std::function<void()>> deletors;
                        soci::statement stmt(*session_);
                        stmt.alloc();
                        if (del_templ == "") {
                            del_templ = deleteTemplate();
                        }
                        stmt.prepare(del_templ);
                        sociBindKey(stmt, keysToDeleteVec, deletors);
                        stmt.define_and_bind();
                        stmt.execute(true);
                        for (auto const &d : deletors) {
                            d();
                        }
                    }
                    if (!keysToInsertVec.empty()) {
                        std::vector<std::function<void()>> deletors;
                        soci::statement stmt(*session_);
                        stmt.alloc();
                        if (ins_templ == "") {
                            ins_templ = insertTemplate();
                        }
                        stmt.prepare(ins_templ);
                        sociBindKey(stmt, keysToInsertVec, deletors);
                        sociBindData(stmt, datasToInsertVec, deletors);
                        stmt.define_and_bind();
                        stmt.execute(true);
                        for (auto const &d : deletors) {
                            d();
                        }
                    }
                    typename TI::TransactionResponse resp {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::Success};
                    triggerCallback(resp, key, processedUpdate);
                    return resp;
                } else {
                    return {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::FailurePermission};
                }
            }
            typename TI::TransactionResponse handleDelete(std::string const &account, typename TI::Key const &key, std::optional<typename TI::Version> const &versionToDelete) override final {
                return {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::FailureConsistency};
            }
        };

        template <class Env, typename=std::enable_if_t<
                std::is_convertible_v<Env *, DSComponent *>
                && std::is_convertible_v<Env *, THComponent *>
            >
        >
        static void initializeEnvironment(
            Env *env
            , std::shared_ptr<soci::session> const &session
            , std::string const &tableName
            , std::function<bool(std::string const &)> accountFilter = std::function<bool(std::string const &)>()
            , std::optional<std::string> const &whereClause = std::nullopt
        ) {
            env->DSComponent::operator=(DSComponent {
                session
                , tableName
                , [env](std::string const &s) {
                    env->log(infra::LogLevel::Info, s);
                }
                , whereClause
            });
            env->THComponent::operator=(THComponent {
                session
                , tableName
                , [env](std::string const &s) {
                    env->log(infra::LogLevel::Info, s);
                }
                , accountFilter
                , static_cast<DSComponent *>(env)
            });
        }

        //return type is auto deducted
        template <class R, typename=std::enable_if_t<
                std::is_convertible_v<typename R::AppType::EnvironmentType *, DSComponent *>
                && std::is_convertible_v<typename R::AppType::EnvironmentType *, THComponent *>
            >
        >
        static auto setupTransactionServer(
            R &r 
            , std::string const &prefix
            , bool sealTransactionFacility = false
        ) {
            using TF = basic::transaction::complex_key_value_store::as_collection::TF<typename R::AppType, ItemKey, ItemData>;
            using DM = basic::transaction::complex_key_value_store::as_collection::DM<typename R::AppType, ItemKey, ItemData>;
            auto dataStore = std::make_shared<typename TF::DataStore>();
            r.preservePointer(dataStore);
            return basic::transaction::v2::transactionLogicCombination<
                R, TI, DI, DM, basic::transaction::v2::SubscriptionLoggingLevel::Verbose
            >(
                r
                , prefix
                , new TF(dataStore)
                , sealTransactionFacility
            );
        }
    };

}}}}}

#endif