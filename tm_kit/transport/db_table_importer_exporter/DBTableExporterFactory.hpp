#ifndef TM_KIT_TRANSPORT_DB_TABLE_IMPORTER_EXPORTER_DB_TABLE_EXPORTER_FACTORY_HPP_
#define TM_KIT_TRANSPORT_DB_TABLE_IMPORTER_EXPORTER_DB_TABLE_EXPORTER_FACTORY_HPP_

#include <tm_kit/basic/StructFieldInfoUtils.hpp>
#include <tm_kit/basic/DateHolder.hpp>

#include <soci/soci.h>

#include <iostream>
#include <sstream>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace db_table_importer_exporter {
    template <class M>
    class DBTableExporterFactory {
    private:
        template <class T>
        static std::string insertTemplate(std::string const &tableName) {
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<T>;
            std::ostringstream oss;
            oss << "INSERT INTO " << tableName << '(';
            oss << DF::commaSeparatedFieldNames();
            oss << ") VALUES (";
            bool begin = true;
            DF::addValueFieldsToInsertValueList(oss, begin);
            oss << ')';
            return oss.str();
        }
        template <class ItemKey, class ItemData>
        static std::string insertTemplateWithDuplicateCheck(std::string const &tableName) {
            using KF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemKey>;
            using DF = basic::struct_field_info_utils::StructFieldInfoBasedDataFiller<ItemData>;
            std::ostringstream oss;
            oss << "INSERT INTO " << tableName << '(';
            oss << KF::commaSeparatedFieldNames();
            oss << ", ";
            oss << DF::commaSeparatedFieldNames();
            oss << ") VALUES (";
            bool begin = true;
            KF::addValueFieldsToInsertValueList(oss, begin);
            DF::addValueFieldsToInsertValueList(oss, begin);
            oss << ") ON DUPLICATE KEY UPDATE ";
            begin = true;
            for (auto const &s : basic::StructFieldInfo<ItemData>::FIELD_NAMES) {
                if (!begin) {
                    oss << ", ";
                }
                begin = false;
                oss << s << "=VALUES(" << s << ")";
            }
            return oss.str();
        }
        template <class T, int FieldCount, int FieldIndex>
        static void sociBindFields_internal(soci::statement &stmt, T const &data) {
            if constexpr (FieldIndex>=0 && FieldIndex<FieldCount) {
                if constexpr (std::is_same_v<typename basic::StructFieldTypeInfo<T,FieldIndex>::TheType, basic::DateHolder>) {
                    std::tm t;
                    auto const &x = data.*(basic::StructFieldTypeInfo<T,FieldIndex>::fieldPointer());
                    t.tm_year = ((x.year==0)?0:x.year-1900);
                    t.tm_mon = ((x.month==0)?0:x.month-1);
                    t.tm_mday = ((x.day==0)?1:x.day);
                    t.tm_hour = 0;
                    t.tm_min = 0;
                    t.tm_sec = 0;
                    t.tm_isdst = -1;
                    stmt.exchange(soci::use(t, std::string(basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex])));
                } else if constexpr (std::is_same_v<typename basic::StructFieldTypeInfo<T,FieldIndex>::TheType, std::chrono::system_clock::time_point>) {
                    std::string s = infra::withtime_utils::localTimeString(data.*(basic::StructFieldTypeInfo<T,FieldIndex>::fieldPointer()));
                    stmt.exchange(soci::use(s, std::string(basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex])));
                } else {
                    stmt.exchange(soci::use(data.*(basic::StructFieldTypeInfo<T,FieldIndex>::fieldPointer()), std::string(basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex])));
                }
                if constexpr (FieldIndex < FieldCount-1) {
                    sociBindFields_internal<T,FieldCount,FieldIndex+1>(stmt, data);
                }
            }
        }
        template <class T>
        static void sociBindFields(soci::statement &stmt, T const &data) {
            sociBindFields_internal<T, basic::StructFieldInfo<T>::FIELD_NAMES.size(), 0>(stmt, data);
        }
        template <class T, int FieldCount, int FieldIndex>
        static void sociBindFieldsBatch_internal(soci::statement &stmt, std::vector<T> const &data, std::vector<std::function<void()>> &deletors) {            
            if constexpr (FieldIndex>=0 && FieldIndex<FieldCount) {
                if constexpr (std::is_same_v<typename basic::StructFieldTypeInfo<T,FieldIndex>::TheType, basic::DateHolder>) {
                    auto *v = new std::vector<std::tm>();
                    for (auto const &x : data) {
                        auto const &y = x.*(basic::StructFieldTypeInfo<T,FieldIndex>::fieldPointer());
                        std::tm t;
                        t.tm_year = ((y.year==0)?0:y.year-1900);
                        t.tm_mon = ((y.month==0)?0:y.month-1);
                        t.tm_mday = ((y.day==0)?1:y.day);
                        t.tm_hour = 0;
                        t.tm_min = 0;
                        t.tm_sec = 0;
                        t.tm_isdst = -1;
                        v->push_back(t);
                    }
                    stmt.exchange(soci::use(*v, std::string(basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex])));
                    deletors.push_back([v]() {delete v;});
                } else if constexpr (std::is_same_v<typename basic::StructFieldTypeInfo<T,FieldIndex>::TheType, std::chrono::system_clock::time_point>) {
                    auto *v = new std::vector<std::string>();
                    for (auto const &x : data) {
                        auto const &y = x.*(basic::StructFieldTypeInfo<T,FieldIndex>::fieldPointer());
                        v->push_back(infra::withtime_utils::localTimeString(y));
                    }
                    stmt.exchange(soci::use(*v, std::string(basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex])));
                    deletors.push_back([v]() {delete v;});
                } else {
                    auto *v = new std::vector<typename basic::StructFieldTypeInfo<T,FieldIndex>::TheType>();
                    for (auto const &x : data) {
                        v->push_back(x.*(basic::StructFieldTypeInfo<T,FieldIndex>::fieldPointer()));
                    }
                    stmt.exchange(soci::use(*v, std::string(basic::StructFieldInfo<T>::FIELD_NAMES[FieldIndex])));
                    deletors.push_back([v]() {delete v;});
                }
                if constexpr (FieldIndex < FieldCount-1) {
                    sociBindFieldsBatch_internal<T,FieldCount,FieldIndex+1>(stmt, data, deletors);
                }
            }
        }
        template <class T>
        static void sociBindFieldsBatch(soci::statement &stmt, std::vector<T> const &data, std::vector<std::function<void()>> &deletors) {
            sociBindFieldsBatch_internal<T, basic::StructFieldInfo<T>::FIELD_NAMES.size(), 0>(stmt, data, deletors);
        }
    public:
        template <class T, typename = std::enable_if_t<basic::StructFieldInfo<T>::HasGeneratedStructFieldInfo>>
        static auto createExporter(std::shared_ptr<soci::session> const &session, std::string const &tableName)
            -> std::shared_ptr<typename M::template Exporter<T>>
        {
            return M::template pureExporter<T>(
                [session,tableName](
                    T &&data
                ) {
                    static std::string insertStmt = insertTemplate<T>(tableName);
                    
                    soci::statement stmt(*session);
                    stmt.alloc();
                    stmt.prepare(insertStmt);
                    sociBindFields(stmt, data);
                    stmt.define_and_bind();
                    stmt.execute(true);
                }
            );
        }
        template <class ItemKey, class ItemData, typename = std::enable_if_t<basic::StructFieldInfo<ItemKey>::HasGeneratedStructFieldInfo && basic::StructFieldInfo<ItemData>::HasGeneratedStructFieldInfo>>
        static auto createExporterWithDuplicateCheck(std::shared_ptr<soci::session> const &session, std::string const &tableName)
            -> std::shared_ptr<typename M::template Exporter<std::tuple<ItemKey,ItemData>>>
        {
            return M::template pureExporter<std::tuple<ItemKey,ItemData>>(
                [session,tableName](
                    std::tuple<ItemKey,ItemData> &&data
                ) {
                    static std::string insertStmt = insertTemplateWithDuplicateCheck<ItemKey,ItemData>(tableName);
                    
                    soci::statement stmt(*session);
                    stmt.alloc();
                    stmt.prepare(insertStmt);
                    sociBindFields(stmt, std::get<0>(data));
                    sociBindFields(stmt, std::get<1>(data));
                    stmt.define_and_bind();
                    stmt.execute(true);
                }
            );
        }
        template <class T, typename = std::enable_if_t<basic::StructFieldInfo<T>::HasGeneratedStructFieldInfo>>
        static auto createBatchExporter(std::shared_ptr<soci::session> const &session, std::string const &tableName)
            -> std::shared_ptr<typename M::template Exporter<std::vector<T>>>
        {
            return M::template pureExporter<std::vector<T>>(
                [session,tableName](
                    std::vector<T> &&data
                ) {
                    if (data.empty()) {
                        return;
                    }
                    static std::string insertStmt = insertTemplate<T>(tableName);
                    
                    std::vector<std::function<void()>> deletors;
                    soci::statement stmt(*session);
                    stmt.alloc();
                    stmt.prepare(insertStmt);
                    sociBindFieldsBatch(stmt, data, deletors);
                    stmt.define_and_bind();
                    stmt.execute(true);
                    for (auto const &d : deletors) {
                        d();
                    }
                    
                }
            );
        }
        template <class T, typename = std::enable_if_t<basic::StructFieldInfo<T>::HasGeneratedStructFieldInfo>>
        static void writeBatchToTable(std::shared_ptr<soci::session> const &session, std::string const &tableName, std::vector<T> const &batch)
        {
            static std::string insertStmt = insertTemplate<T>(tableName);

            if (batch.empty()) {
                return;
            }
            
            std::vector<std::function<void()>> deletors;
            soci::statement stmt(*session);
            stmt.alloc();
            stmt.prepare(insertStmt);
            sociBindFieldsBatch(stmt, batch, deletors);
            stmt.define_and_bind();
            stmt.execute(true);
            for (auto const &d : deletors) {
                d();
            }
        }
    };
} } } } }

#endif