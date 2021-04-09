#ifndef TM_KIT_TRANSPORT_COMPLEX_KEY_VALUE_STORE_COMPONENTS_SINGLE_TABLE_PER_ITEM_TRANSACTION_SERVER_HPP_
#define TM_KIT_TRANSPORT_COMPLEX_KEY_VALUE_STORE_COMPONENTS_SINGLE_TABLE_PER_ITEM_TRANSACTION_SERVER_HPP_

#include <tm_kit/basic/CalculationsOnInit.hpp>
#include <tm_kit/basic/StructFieldInfoUtils.hpp>
#include <tm_kit/basic/transaction/complex_key_value_store/VersionlessDataModel.hpp>
#include <tm_kit/basic/transaction/v2/TransactionLogicCombination.hpp>

#include <soci/soci.h>

#include <iostream>
#include <sstream>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace complex_key_value_store_components {

    template <class ItemKey, class ItemData>
    class SingleTablePerItemTransactionServer {
    public:
        using DI = basic::transaction::complex_key_value_store::per_item::DI<ItemKey, ItemData>;
        using TI = basic::transaction::complex_key_value_store::per_item::TI<ItemKey, ItemData>;
        template <class IDType>
        using GS = basic::transaction::complex_key_value_store::per_item::GS<IDType, ItemKey, ItemData>;

        using KF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemKey>;
        using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemData>;
                
        class DSComponent : public basic::transaction::v2::DataStreamEnvComponent<DI> {
        private:
            std::shared_ptr<soci::session> session_;
            std::string tableName_;
            std::function<void(std::string)> logger_;
            typename basic::transaction::v2::DataStreamEnvComponent<DI>::Callback *cb_;
        public:
            DSComponent() : session_(), tableName_(), logger_() {
            }
            DSComponent(std::shared_ptr<soci::session> const &session, std::string const &tableName, std::function<void(std::string)> const &logger) : session_(session), tableName_(tableName), logger_(logger) {
            }
            DSComponent(DSComponent &&c) : session_(std::move(c.session_)), tableName_(std::move(c.tableName_)), logger_(std::move(c.logger_)) {}
            DSComponent &operator=(DSComponent &&c) {
                if (this != &c) {
                    session_ = std::move(c.session_);
                    tableName_ = std::move(c.tableName_);
                    logger_ = std::move(c.logger_);
                }
                return *this;
            }
            virtual ~DSComponent() {}
            void initialize(typename basic::transaction::v2::DataStreamEnvComponent<DI>::Callback *cb) {
                cb_ = cb;
                std::vector<typename DI::OneUpdateItem> updates;
                soci::rowset<soci::row> res = 
                    session_->prepare << ("SELECT "+KF::commaSeparatedFieldNames()+", "+DF::commaSeparatedFieldNames()+" FROM "+tableName_);
                for (auto const &r : res) {
                    updates.push_back({
                        typename DI::OneFullUpdateItem {
                            KF::retrieveData(r,0)
                            , basic::ConstType<0> {}
                            , DF::retrieveData(r, KF::FieldCount)
                        }
                    });
                }
                std::ostringstream oss;
                oss << "[SingleTablePerItemTransactionServer::DSComponent::initialize] loaded " << updates.size() << " rows";
                logger_(oss.str());
                cb_->onUpdate(typename DI::Update {
                    basic::ConstType<0> {}
                    , std::move(updates)
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

            void triggerCallback(typename TI::TransactionResponse const &resp, typename TI::Key const &key, std::optional<typename TI::Data> const &data) {
                dsComponent_->callback()->onUpdate(typename DI::Update {
                    basic::ConstType<0> {}
                    , std::vector<typename DI::OneUpdateItem> {
                        { typename DI::OneFullUpdateItem {
                            key
                            , basic::ConstType<0> {}
                            , data
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
            std::string updateTemplate() const {
                std::ostringstream oss;
                oss << "UPDATE " << tableName_ << " SET ";
                bool begin = true;
                for (auto const &s : basic::StructFieldInfo<ItemData>::FIELD_NAMES) {
                    if (!begin) {
                        oss << ", ";
                    }
                    begin = false;
                    oss << s << " = :" << s;
                }
                oss << " WHERE ";
                begin = true;
                for (auto const &s : basic::StructFieldInfo<ItemKey>::FIELD_NAMES) {
                    if (!begin) {
                        oss << " AND ";
                    }
                    begin = false;
                    oss << s << " = :" << s;
                }
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
            static void sociBindKey_internal(soci::statement &stmt, ItemKey const &k) {
                if constexpr (FieldIndex>=0 && FieldIndex<FieldCount) {
                    stmt.exchange(soci::use(k.*(basic::StructFieldTypeInfo<ItemKey,FieldIndex>::fieldPointer()), std::string(basic::StructFieldInfo<ItemKey>::FIELD_NAMES[FieldIndex])));
                    if constexpr (FieldIndex < FieldCount-1) {
                        sociBindKey_internal<FieldCount,FieldIndex+1>(stmt, k);
                    }
                }
            }
            static void sociBindKey(soci::statement &stmt, ItemKey const &k) {
                sociBindKey_internal<basic::StructFieldInfo<ItemKey>::FIELD_NAMES.size(), 0>(stmt, k);
            }
            template <int FieldCount, int FieldIndex>
            static void sociBindData_internal(soci::statement &stmt, ItemData const &d) {
                if constexpr (FieldIndex>=0 && FieldIndex<FieldCount) {
                    stmt.exchange(soci::use(d.*(basic::StructFieldTypeInfo<ItemData,FieldIndex>::fieldPointer()), std::string(basic::StructFieldInfo<ItemData>::FIELD_NAMES[FieldIndex])));
                    if constexpr (FieldIndex < FieldCount-1) {
                        sociBindData_internal<FieldCount,FieldIndex+1>(stmt, d);
                    }
                }
            }
            static void sociBindData(soci::statement &stmt, ItemData const &d) {
                sociBindData_internal<basic::StructFieldInfo<ItemData>::FIELD_NAMES.size(), 0>(stmt, d);
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
                static std::string templ = "";
                if (accountFilter_) {
                    if (!accountFilter_(account)) {
                        return {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::FailurePermission};
                    }
                }
                if (session_) {
                    soci::statement stmt(*session_);
                    stmt.alloc();
                    if (templ == "") {
                        templ = insertTemplate();
                    }
                    stmt.prepare(templ);
                    sociBindKey(stmt, key);
                    sociBindData(stmt, data);
                    stmt.define_and_bind();
                    stmt.execute(true);
                    typename TI::TransactionResponse resp {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::Success};
                    triggerCallback(resp, key, data);
                    return resp;
                } else {
                    return {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::FailurePermission};
                }
            }
            typename TI::TransactionResponse handleUpdate(std::string const &account, typename TI::Key const &key, std::optional<typename TI::VersionSlice> const &updateVersionSlice, typename TI::ProcessedUpdate const &processedUpdate) override final {
                static std::string templ = "";
                if (accountFilter_) {
                    if (!accountFilter_(account)) {
                        return {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::FailurePermission};
                    }
                }
                if (session_) {
                    soci::statement stmt(*session_);
                    stmt.alloc();
                    if (templ == "") {
                        templ = updateTemplate();
                    }
                    stmt.prepare(templ);
                    sociBindKey(stmt, key);
                    sociBindData(stmt, processedUpdate);
                    stmt.define_and_bind();
                    stmt.execute(true);
                    typename TI::TransactionResponse resp {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::Success};
                    triggerCallback(resp, key, processedUpdate);
                    return resp;
                } else {
                    return {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::FailurePermission};
                }
            }
            typename TI::TransactionResponse handleDelete(std::string const &account, typename TI::Key const &key, std::optional<typename TI::Version> const &versionToDelete) override final {
                static std::string templ = "";
                if (accountFilter_) {
                    if (!accountFilter_(account)) {
                        return {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::FailurePermission};
                    }
                }
                if (session_) {
                    soci::statement stmt(*session_);
                    stmt.alloc();
                    if (templ == "") {
                        templ = deleteTemplate();
                    }
                    stmt.prepare(templ);
                    sociBindKey(stmt, key);
                    stmt.define_and_bind();
                    stmt.execute(true);
                    typename TI::TransactionResponse resp {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::Success};
                    triggerCallback(resp, key, std::nullopt);
                    return resp;
                } else {
                    return {basic::ConstType<0> {}, basic::transaction::v2::RequestDecision::FailurePermission};
                }
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
        ) {
            env->DSComponent::operator=(DSComponent {
                session
                , tableName
                , [env](std::string const &s) {
                    env->log(infra::LogLevel::Info, s);
                }
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
        ) {
            using TF = basic::transaction::complex_key_value_store::per_item::TF<typename R::AppType, ItemKey, ItemData>;
            using DM = basic::transaction::complex_key_value_store::per_item::DM<typename R::AppType, ItemKey, ItemData>;
            auto dataStore = std::make_shared<typename TF::DataStore>();
            r.preservePointer(dataStore);
            return basic::transaction::v2::transactionLogicCombination<
                R, TI, DI, DM, basic::transaction::v2::SubscriptionLoggingLevel::Verbose
            >(
                r
                , prefix
                , new TF(dataStore)
            );
        }
    };

}}}}}

#endif