#ifndef TM_KIT_TRANSPORT_CONNECTION_LOCATOR_HPP_
#define TM_KIT_TRANSPORT_CONNECTION_LOCATOR_HPP_

#include <string>
#include <map>
#include <iostream>
#include <exception>
#include <functional>

#include <tm_kit/basic/ByteData.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    class ConnectionLocatorParseError : public std::runtime_error {
    public:
        ConnectionLocatorParseError(std::string const &msg) : std::runtime_error(msg) {}
    };
    class ConnectionLocator final {
    private:
        std::string host_;
        int port_;
        std::string userName_;
        std::string password_;
        std::string identifier_;
        std::map<std::string, std::string> properties_;
    public:
        ConnectionLocator() = default;
        ~ConnectionLocator() = default;
        ConnectionLocator(ConnectionLocator const &) = default;
        ConnectionLocator(ConnectionLocator &&) = default;
        ConnectionLocator &operator=(ConnectionLocator const &) = default;
        ConnectionLocator &operator=(ConnectionLocator &&) = default;

        ConnectionLocator(std::string const &h, int p) :
            host_(h), port_(p), userName_(), password_(), identifier_(), properties_()
            {}
        ConnectionLocator(std::string const &h, int p, std::string const &u, std::string const &pwd) :
            host_(h), port_(p), userName_(u), password_(pwd), identifier_(), properties_()
            {}
        ConnectionLocator(std::string const &h, int p, std::string const &u, std::string const &pwd, std::string const &id) :
            host_(h), port_(p), userName_(u), password_(pwd), identifier_(id), properties_()
            {}

        static ConnectionLocator parse(std::string const &input);

        ConnectionLocator copyOfBasicPortionWithProperties() const;
        ConnectionLocator copyOfBasicPortionWithoutProperties() const;
        ConnectionLocator copyWithoutProperties() const;

        std::string const &host() const { return host_; }
        int port() const { return port_; }
        std::string const &userName() const { return userName_; }
        std::string const &password() const { return password_; }
        std::string const &identifier() const { return identifier_; }

        std::string query(std::string const &field, std::string const &defaultValue="") const;
        template <class F>
        void for_all_properties(F const &f) const {
            for (auto const &item : properties_) {
                f(item.first, item.second);
            }
        }
        template <class F>
        void for_all_properties(F &f) const {
            for (auto const &item : properties_) {
                f(item.first, item.second);
            }
        }

        std::string toSerializationFormat() const;
        std::string toPrintFormat() const;

        bool operator==(ConnectionLocator const &) const;
        bool operator<(ConnectionLocator const &) const;

        ConnectionLocator modifyHost(std::string const &h) const {
            ConnectionLocator l(*this);
            l.host_ = h;
            return l;
        }
        ConnectionLocator modifyPort(int p) const {
            ConnectionLocator l(*this);
            l.port_ = p;
            return l;
        }
        ConnectionLocator modifyUserName(std::string const &u) const {
            ConnectionLocator l(*this);
            l.userName_ = u;
            return l;
        }
        ConnectionLocator modifyPassword(std::string const &p) const {
            ConnectionLocator l(*this);
            l.password_ = p;
            return l;
        }
        ConnectionLocator modifyIdentifier(std::string const &s) const {
            ConnectionLocator l(*this);
            l.identifier_ = s;
            return l;
        }
        ConnectionLocator clearProperties() const {
            ConnectionLocator l(*this);
            l.properties_.clear();
            return l;
        }
        ConnectionLocator removeProperty(std::string const &name) const {
            ConnectionLocator l(*this);
            l.properties_.erase(name);
            return l;
        }
        ConnectionLocator addProperty(std::string const &name, std::string const &value) const {
            ConnectionLocator l(*this);
            l.properties_[name] = value;
            return l;
        }
        ConnectionLocator addProperties(std::unordered_map<std::string, std::string> const &prop) const {
            ConnectionLocator l(*this);
            for (auto const &item : prop) {
                l.properties_[item.first] = item.second;
            }
            return l;
        }
    };

    inline std::ostream &operator<<(std::ostream &os, ConnectionLocator const &d) {
        os << d.toPrintFormat();
        return os;
    }
} } } }

namespace std {
    template <>
    class hash<dev::cd606::tm::transport::ConnectionLocator> {
    public:
        inline std::size_t operator()(dev::cd606::tm::transport::ConnectionLocator const &d) const {
            return (
                std::hash<std::string>()(d.host())
                ^
                std::hash<int>()(d.port())
                ^
                std::hash<std::string>()(d.userName())
                ^
                std::hash<std::string>()(d.password())
                ^
                std::hash<std::string>()(d.identifier())
            );
        }
    };
}

namespace dev { namespace cd606 { namespace tm { namespace basic { namespace bytedata_utils {

    template <>
    struct RunCBORSerializer<transport::ConnectionLocator, void> {
        static std::string apply(transport::ConnectionLocator const &x) {
            return RunCBORSerializer<std::string>::apply(
                x.toSerializationFormat()
            );
        }
        static std::size_t apply(transport::ConnectionLocator const &x, char *output) {
            return RunCBORSerializer<std::string>::apply(
                x.toSerializationFormat(), output
            );
        }   
        static std::size_t calculateSize(transport::ConnectionLocator const &x) {
            return RunCBORSerializer<std::string>::calculateSize(
                x.toSerializationFormat()
            );
        }
    };
    template <>
    struct RunCBORDeserializer<transport::ConnectionLocator, void> {
        static std::optional<std::tuple<transport::ConnectionLocator,size_t>> apply(std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<std::string>::apply(data, start);
            if (t) {
                try {
                    auto l = transport::ConnectionLocator::parse(std::get<0>(*t));
                    return std::tuple<transport::ConnectionLocator,size_t> {
                        std::move(l)
                        , std::get<1>(*t)
                    };
                } catch (transport::ConnectionLocatorParseError const &) {
                    return std::nullopt;
                }
            } else {
                return std::nullopt;
            }
        }
        static std::optional<size_t> applyInPlace(transport::ConnectionLocator &output, std::string_view const &data, size_t start) {
            auto t = RunCBORDeserializer<std::string>::apply(data, start);
            if (t) {
                try {
                    output = transport::ConnectionLocator::parse(std::get<0>(*t));
                    return std::get<1>(*t);
                } catch (transport::ConnectionLocatorParseError const &) {
                    return std::nullopt;
                }
            } else {
                return std::nullopt;
            }
        }
    };
} } } } }

#endif