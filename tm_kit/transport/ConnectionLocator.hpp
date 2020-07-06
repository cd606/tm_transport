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

        std::string toSerializationFormat() const;
        std::string toPrintFormat() const;

        bool operator==(ConnectionLocator const &) const;
        bool operator<(ConnectionLocator const &) const;
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
        static std::vector<uint8_t> apply(transport::ConnectionLocator const &x) {
            return RunCBORSerializer<std::string>::apply(
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
    };
} } } } }

#endif