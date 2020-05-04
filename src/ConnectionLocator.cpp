#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
#include <sstream>

#include <tm_kit/transport/ConnectionLocator.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    ConnectionLocator ConnectionLocator::parse(std::string const &input) {
        std::vector<std::string> parts;
        std::string x = boost::trim_copy(input);
        std::string mainPortion, propertyPortion;
        if (boost::ends_with(x, "]")) {
            auto propStart = x.rfind('[');
            if (propStart == std::string::npos) {
                throw ConnectionLocatorParseError("'"+input+"' is not a valid remote resource descriptor, it ends with ']' but there is no '['");
            }
            mainPortion = boost::trim_copy(x.substr(0, propStart));
            propertyPortion = boost::trim_copy(x.substr(propStart+1, x.length()-propStart-2));
        } else {
            mainPortion = x;
            propertyPortion = "";
        }
        boost::split(parts, mainPortion, boost::is_any_of(":"));
        if (parts.size() < 1) {
            throw ConnectionLocatorParseError("'"+input+"' is not a valid remote resource descriptor, there are too few parts");
        }
        if (parts.size() > 5) {
            throw ConnectionLocatorParseError("'"+input+"' is not a valid remote resource descriptor, there are too many parts");
        }
        ConnectionLocator ret;
        ret.host_ = boost::trim_copy(parts[0]);
        if (parts.size() >= 2) {
            x = boost::trim_copy(parts[1]);
            if (x == "") {
                ret.port_ = 0;
            } else {
                try {
                    ret.port_ = boost::lexical_cast<int>(x);
                } catch (boost::bad_lexical_cast const &) {
                    throw ConnectionLocatorParseError("'"+input+"' is not a valid remote resource descriptor, '"+parts[1]+"' is not a valid port");
                }
            }
        } else {
            ret.port_ = 0;
        }
        if (parts.size() >= 3) {
            ret.userName_ = boost::trim_copy(parts[2]);
        } else {
            ret.userName_ = "";
        }
        if (parts.size() >= 4) {
            ret.password_ = parts[3];
        } else {
            ret.password_ = "";
        }
        if (parts.size() >= 5) {
            ret.identifier_ = boost::trim_copy(parts[4]);
        } else {
            ret.identifier_ = "";
        }
        if (propertyPortion != "") {
            std::vector<std::string> propertyParts;
            boost::split(propertyParts, propertyPortion, boost::is_any_of(","));
            for (auto const &p : propertyParts) {
                std::vector<std::string> nameAndValue;
                x = p;
                boost::split(nameAndValue, x, boost::is_any_of("="));
                if (nameAndValue.size() > 2) {
                    throw ConnectionLocatorParseError("'"+input+"' is not a valid remote resource descriptor, '"+p+"' is not a valid property definition");
                }
                auto name = boost::trim_copy(nameAndValue[0]);
                std::string value;
                if (nameAndValue.size() > 1) {
                    value = boost::trim_copy(nameAndValue[1]);
                }
                auto nameUC = boost::to_upper_copy(name);
                if (nameUC == "HOST" || nameUC == "PORT" || nameUC == "USERNAME" || nameUC == "USER"
                    || nameUC == "PASSWORD" || nameUC == "PASSWD" || nameUC == "IDENTIFIER") {
                    throw ConnectionLocatorParseError("'"+input+"' is not a valid remote resource descriptor, trying to specify a property with reserved name '"+name+"'");
                }
                if (ret.properties_.find(name) != ret.properties_.end()) {
                    throw ConnectionLocatorParseError("'"+input+"' is not a valid remote resource descriptor, trying to specify duplicate property '"+name+"'");
                }
                ret.properties_.insert({name, value});
            }
        }
        return ret;
    }

    ConnectionLocator ConnectionLocator::copyOfBasicPortionWithProperties() const {
        ConnectionLocator l;
        l.host_ = host_;
        l.port_ = port_;
        l.userName_ = userName_;
        l.password_ = password_;
        l.properties_ = properties_;
        return l;
    }

    ConnectionLocator ConnectionLocator::copyOfBasicPortionWithoutProperties() const {
        ConnectionLocator l;
        l.host_ = host_;
        l.port_ = port_;
        l.userName_ = userName_;
        l.password_ = password_;
        return l;
    }

    ConnectionLocator ConnectionLocator::copyWithoutProperties() const {
        ConnectionLocator l;
        l.host_ = host_;
        l.port_ = port_;
        l.userName_ = userName_;
        l.password_ = password_;
        l.identifier_ = identifier_;
        return l;
    }

    std::string ConnectionLocator::query(std::string const &field, std::string const &defaultValue) const {
        auto iter = properties_.find(field);
        if (iter != properties_.end()) {
            return iter->second;
        }
        if (field == "HOST") {
            return host_;
        }
        if (field == "PORT") {
            return boost::lexical_cast<std::string>(port_);
        }
        if (field == "USERNAME" || field == "USER") {
            return userName_;
        }
        if (field == "PASSWORD" || field == "PASSWD") {
            return password_;
        }
        if (field == "IDENTIFIER") {
            return identifier_;
        }
        return defaultValue;
    }

    std::string ConnectionLocator::toSerializationFormat() const {
        std::ostringstream oss;
        oss << host_ << ':' << port_ << ':' << userName_ << ':' << password_ << ':' << identifier_;
        if (properties_.size() > 0) {
            oss << '[';
            bool first = true;
            for (auto const &item : properties_) {
                if (!first) {
                    oss << ',';
                }
                oss << item.first << '=' << item.second;
                first = false;
            }
            oss << ']';
        }
        return oss.str();
    }
    
    std::string ConnectionLocator::toPrintFormat() const {
        std::ostringstream oss;
        oss << "{host=" << host_ << ",port=" << port_ << ",userName=" << userName_
            << ",password=" << password_ << ",identifier=" << identifier_;
        if (properties_.size() > 0) {
            oss << ",properties={";
            bool first = true;
            for (auto const &item : properties_) {
                if (!first) {
                    oss << ',';
                }
                oss << item.first << '=' << item.second;
                first = false;
            }
            oss << '}';
        }
        oss << '}';
        return oss.str();
    }

    bool ConnectionLocator::operator==(ConnectionLocator const &l) const {
        if (this == &l) {
            return true;
        }
        return (host_ == l.host_ && port_ == l.port_ && userName_ == l.userName_ 
            && password_ == l.password_ && identifier_ == l.identifier_
            && properties_ == l.properties_);
    }
    bool ConnectionLocator::operator<(ConnectionLocator const &l) const{
        if (this == &l) {
            return false;
        }
        if (host_ < l.host_) {
            return true;
        }
        if (host_ > l.host_) {
            return false;
        }
        if (port_ < l.port_) {
            return true;
        }
        if (port_ > l.port_) {
            return false;
        }
        if (userName_ < l.userName_) {
            return true;
        }
        if (userName_ > l.userName_) {
            return false;
        }
        if (password_ < l.password_) {
            return true;
        }
        if (password_ > l.password_) {
            return false;
        }
        return (properties_ < l.properties_);
    }

} } } }
