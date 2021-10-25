#include <tm_kit/transport/json_rest/JsonRESTComponent.hpp>
#include <tm_kit/transport/TLSConfigurationComponent.hpp>
#include <tm_kit/transport/HostNameUtil.hpp>

#include <tm_kit/basic/LoggingComponentBase.hpp>
#include <nlohmann/json.hpp>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>
#include <boost/beast/core/detail/base64.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/socket_base.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/config.hpp>
#include <boost/algorithm/string.hpp>

#include <thread>
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <sstream>
#ifdef _MSC_VER
#include <locale>
#include <codecvt>
#endif

#include <sodium/crypto_pwhash.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    class JsonRESTComponentImpl {
    private:
        using HandlerFunc = std::function<
            bool(std::string const &, std::string const &, std::unordered_map<std::string, std::vector<std::string>> const &, std::function<void(std::string const &)> const &)
        >;
        std::unordered_map<int, std::unordered_map<std::string, HandlerFunc>> handlerMap_;
        mutable std::mutex handlerMapMutex_;
        std::unordered_map<int, std::filesystem::path> docRootMap_;
        mutable std::mutex docRootMapMutex_;
        std::atomic<bool> started_;

        class OneClient : public std::enable_shared_from_this<OneClient> {
        private:
            JsonRESTComponentImpl *parent_;
            ConnectionLocator locator_;
            std::string request_;
            std::function<void(std::string &&)> callback_;

            boost::asio::io_context *svc_;
            std::optional<boost::asio::ssl::context> sslCtx_;
            basic::LoggingComponentBase *logger_;
            bool initializationFailure_;

            boost::asio::ip::tcp::resolver resolver_;
            std::variant<
                std::monostate 
                , boost::beast::tcp_stream
                , boost::beast::ssl_stream<boost::beast::tcp_stream>
            > stream_;
            boost::beast::flat_buffer buffer_; 
            boost::beast::http::request<boost::beast::http::string_body> req_;
            boost::beast::http::response<boost::beast::http::string_body> res_;
            std::atomic<bool> ready_;
        public:
            OneClient(
                JsonRESTComponentImpl *parent
                , ConnectionLocator const &locator
                , std::string &&request
                , std::function<void(std::string &&)> const &callback
                , boost::asio::io_context *svc
                , std::optional<TLSClientInfo> const &sslInfo
                , basic::LoggingComponentBase *logger
            )
                : parent_(parent)
                , locator_(locator)
                , request_(std::move(request))
                , callback_(callback)
                , svc_(svc)
                , sslCtx_(
                    sslInfo
                    ? std::optional<boost::asio::ssl::context>(boost::asio::ssl::context {boost::asio::ssl::context::tlsv12_client})
                    : std::nullopt
                )
                , logger_(logger)
                , initializationFailure_(false)
                , resolver_(boost::asio::make_strand(*svc_))
                , stream_()
                , buffer_()
                , req_()
                , res_()
                , ready_(false)
            {
                if (sslInfo) {
                    std::string caCert;
                    if (sslInfo->caCertificateFile != "") {
                        std::ifstream ifs(sslInfo->caCertificateFile.c_str());
                        caCert = std::string(
                            std::istreambuf_iterator<char>{ifs}, {}
                        );
                        ifs.close();
                    }
                    boost::system::error_code ec;
                    sslCtx_->add_certificate_authority(
                        boost::asio::buffer(caCert.data(), caCert.length())
                        , ec
                    );
                    if (ec) {
                        if (logger_) {
                            logger_->logThroughLoggingComponentBase(
                                infra::LogLevel::Error
                                , "[JsonRESTComponent::OneClient::(constructor)] ASIO error '"+ec.message()+"' for locator '"+locator_.toSerializationFormat()+"'"
                            );
                        }
                        initializationFailure_ = true;
                        return;
                    }
                    stream_.emplace<2>(boost::asio::make_strand(*svc_), *sslCtx_);
                } else {
                    stream_.emplace<1>(boost::asio::make_strand(*svc_));
                }
            }
            ~OneClient() {
            }
            void run() {
                req_.method(boost::beast::http::verb::post);
                std::string target = locator_.identifier();
                if (!boost::starts_with(target, "/")) {
                    target = "/"+target;
                }
                req_.target(target);
                req_.set(boost::beast::http::field::host, locator_.host());
                req_.set(boost::beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
                if (locator_.userName() != "") {
                    std::string authStringOrig = locator_.userName()+":"+locator_.password();
                    std::string authString;
                    authString.resize(boost::beast::detail::base64::encoded_size(authStringOrig.length()));
                    authString.resize(boost::beast::detail::base64::encode(authString.data(), reinterpret_cast<uint8_t const *>(authStringOrig.data()), authStringOrig.length()));

                    req_.set(boost::beast::http::field::authorization, "Basic "+authString);
                }
                req_.set(boost::beast::http::field::content_type, "application/json");
                req_.body() = std::move(request_);
                req_.prepare_payload();

                resolver_.async_resolve(
                    locator_.host()
                    , std::to_string(locator_.port())
                    , boost::beast::bind_front_handler(
                        &OneClient::onResolve
                        , shared_from_this()
                    )
                );
            }
            void onResolve(boost::beast::error_code ec, boost::asio::ip::tcp::resolver::results_type results) {
                if (ec) {
                    if (logger_) {
                        logger_->logThroughLoggingComponentBase(
                            infra::LogLevel::Error
                            , "[JsonRESTComponent::OneClient::onResolve] ASIO error '"+ec.message()+"' for locator '"+locator_.toSerializationFormat()+"'"
                        );
                    }
                    parent_->removeJsonRESTClient(shared_from_this());
                    return;
                }
                if (stream_.index() == 1) {
                    std::get<1>(stream_).expires_after(std::chrono::seconds(30));
                    std::get<1>(stream_).async_connect(
                        results 
                        , boost::beast::bind_front_handler(
                            &OneClient::onConnect 
                            , shared_from_this()
                        )
                    );
                } else {
                    boost::beast::get_lowest_layer(std::get<2>(stream_)).expires_after(std::chrono::seconds(30));
                    boost::beast::get_lowest_layer(std::get<2>(stream_)).async_connect(
                        results
                        , boost::beast::bind_front_handler(
                            &OneClient::onConnect
                            , shared_from_this()
                        )
                    );
                }
            }
            void onConnect(boost::beast::error_code ec, boost::asio::ip::tcp::resolver::results_type::endpoint_type) {
                if (ec) {
                    if (logger_) {
                        logger_->logThroughLoggingComponentBase(
                            infra::LogLevel::Error
                            , "[JsonRESTComponent::OneClient::onConnect] ASIO error '"+ec.message()+"' for locator '"+locator_.toSerializationFormat()+"'"
                        );
                    }
                    parent_->removeJsonRESTClient(shared_from_this());
                    return;
                }
                if (stream_.index() == 1) {
                    std::get<1>(stream_).expires_after(std::chrono::seconds(30));
                    boost::beast::http::async_write(
                        std::get<1>(stream_)
                        , req_
                        , boost::beast::bind_front_handler(
                            &OneClient::onWrite 
                            , shared_from_this()
                        )
                    );
                } else {
                    std::get<2>(stream_).async_handshake(
                        boost::asio::ssl::stream_base::client
                        , boost::beast::bind_front_handler(
                            &OneClient::onSSLHandshake
                            , shared_from_this()
                        )
                    );
                }
            }
            void onSSLHandshake(boost::beast::error_code ec) {
                if (ec) {
                    if (logger_) {
                        logger_->logThroughLoggingComponentBase(
                            infra::LogLevel::Error
                            , "[JsonRESTComponent::OneClient::onSSLHandshake] ASIO error '"+ec.message()+"' for locator '"+locator_.toSerializationFormat()+"'"
                        );
                    }
                    parent_->removeJsonRESTClient(shared_from_this());
                    return;
                }
                boost::beast::get_lowest_layer(std::get<2>(stream_)).expires_after(std::chrono::seconds(30));
                boost::beast::http::async_write(
                    std::get<2>(stream_)
                    , req_
                    , boost::beast::bind_front_handler(
                        &OneClient::onWrite 
                        , shared_from_this()
                    )
                );
            }
            void onWrite(boost::beast::error_code ec, std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);
                if (ec) {
                    if (logger_) {
                        logger_->logThroughLoggingComponentBase(
                            infra::LogLevel::Error
                            , "[JsonRESTComponent::OneClient::onWrite] ASIO error '"+ec.message()+"' for locator '"+locator_.toSerializationFormat()+"'"
                        );
                    }
                    parent_->removeJsonRESTClient(shared_from_this());
                    return;
                }
                if (stream_.index() == 1) {
                    boost::beast::http::async_read(
                        std::get<1>(stream_)
                        , buffer_
                        , res_
                        , boost::beast::bind_front_handler(
                            &OneClient::onRead
                            , shared_from_this()
                        )
                    );
                } else {
                    boost::beast::http::async_read(
                        std::get<2>(stream_)
                        , buffer_
                        , res_
                        , boost::beast::bind_front_handler(
                            &OneClient::onRead
                            , shared_from_this()
                        )
                    );
                }
            }
            void onRead(boost::beast::error_code ec, std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);
                if (ec) {
                    if (logger_) {
                        logger_->logThroughLoggingComponentBase(
                            infra::LogLevel::Error
                            , "[JsonRESTComponent::OneClient::onRead] ASIO error '"+ec.message()+"' for locator '"+locator_.toSerializationFormat()+"'"
                        );
                    }
                    parent_->removeJsonRESTClient(shared_from_this());
                    return;
                }
                callback_(std::move(res_.body()));
                if (stream_.index() == 1) {
                    try {
                        std::get<1>(stream_).socket().shutdown(
                            boost::asio::ip::tcp::socket::shutdown_both
                            , ec
                        );
                    } catch (...) {
                    }
                    parent_->removeJsonRESTClient(shared_from_this());
                } else {
                    boost::beast::get_lowest_layer(std::get<2>(stream_)).expires_after(std::chrono::seconds(30));
                    std::get<2>(stream_).async_shutdown(
                        boost::beast::bind_front_handler(
                            &OneClient::onShutdown 
                            , shared_from_this()
                        )
                    );
                }
            }
            void onShutdown(boost::beast::error_code ec) {
                parent_->removeJsonRESTClient(shared_from_this());
            }
            bool initializationFailure() const {
                return initializationFailure_;
            }
        };
        std::unordered_set<std::shared_ptr<OneClient>> clientSet_;
        std::mutex clientSetMutex_;
        boost::asio::io_context clientSvc_;
        std::thread clientThread_;

        class Acceptor : public std::enable_shared_from_this<Acceptor> {
        private:
            JsonRESTComponentImpl *parent_;
            boost::asio::io_context svc_;
            std::optional<boost::asio::ssl::context> sslCtx_;
            basic::LoggingComponentBase *logger_;
            int port_;
            std::thread th_;
            std::atomic<bool> running_;

            boost::asio::ip::tcp::acceptor acceptor_;
            std::string realm_;

            class OneHandler : public std::enable_shared_from_this<OneHandler> {
            private:
                Acceptor *parent_;
                std::variant<
                    std::monostate
                    , boost::beast::tcp_stream
                    , boost::beast::ssl_stream<boost::beast::tcp_stream>
                > stream_;
                boost::beast::flat_buffer buffer_;
                boost::beast::http::request<boost::beast::http::string_body> req_;
            public:
                OneHandler(
                    Acceptor *parent
                    , boost::asio::ip::tcp::socket &&socket
                    , std::optional<boost::asio::ssl::context> &sslCtx
                )
                    : parent_(parent)
                    , stream_()
                    , buffer_()
                    , req_()
                {
                    if (sslCtx) {
                        stream_.emplace<2>(std::move(socket), *sslCtx);
                    } else {
                        stream_.emplace<1>(std::move(socket));
                    }
                }
                ~OneHandler() {
                }
                void run() {
                    if (stream_.index() == 1) {
                        boost::asio::dispatch(
                            std::get<1>(stream_).get_executor()
                            , boost::beast::bind_front_handler(
                                &OneHandler::doRead
                                , shared_from_this()
                            )
                        );
                    } else {
                        std::get<2>(stream_).async_handshake(
                            boost::asio::ssl::stream_base::server
                            , boost::beast::bind_front_handler(
                                &OneHandler::onHandshake 
                                , shared_from_this()
                            )
                        );
                        
                    }
                }
                void onHandshake(boost::beast::error_code) {
                    doRead();
                }
                void doRead() {
                    req_ = {};
                    if (stream_.index() == 1) {
                        boost::beast::http::async_read(
                            std::get<1>(stream_)
                            , buffer_
                            , req_
                            , boost::beast::bind_front_handler(
                                &OneHandler::onRead
                                , shared_from_this()
                            )
                        );
                    } else {
                        boost::beast::http::async_read(
                            std::get<2>(stream_)
                            , buffer_
                            , req_
                            , boost::beast::bind_front_handler(
                                &OneHandler::onRead
                                , shared_from_this()
                            )
                        );
                    }
                }
                std::string unescape(std::string const &s) {
                    std::ostringstream oss;
                    for (std::size_t ii=0; ii<s.length(); ++ii) {
                        if (s[ii] == '+') {
                            oss << ' ';
                        } else if (s[ii] == '%') {
                            if (ii+2 >= s.length()) {
                                break;
                            }
                            int x = 0;
                            char c = s[ii+1];
                            if (c >= 'a' && c <= 'f') {
                                x += (c-'a'+10);
                            } else if (c >= 'A' && c <= 'F') {
                                x += (c-'A'+10);
                            } else if (c >= '0' && c <= '9') {
                                x += (c-'0');
                            } else {
                                break;
                            }
                            x *= 16;
                            c = s[ii+2];
                            if (c >= 'a' && c <= 'f') {
                                x += (c-'a'+10);
                            } else if (c >= 'A' && c <= 'F') {
                                x += (c-'A'+10);
                            } else if (c >= '0' && c <= '9') {
                                x += (c-'0');
                            } else {
                                break;
                            }
                            ii += 2;
                            oss << (char) x;
                        } else {
                            oss << s[ii];
                        }
                    }
                    return oss.str();
                }
                void onRead(boost::beast::error_code ec, std::size_t bytes_transferred) {
                    if (ec) {
                        if (ec != boost::beast::http::make_error_code(boost::beast::http::error::end_of_stream)) {
                            parent_->log(
                                infra::LogLevel::Error
                                , "[JsonRESTComponent::Acceptor::OneHandler::onRead] ASIO error '"+ec.message()+"' for port "+std::to_string(parent_->port())
                            );
                        }
                        doClose(ec);
                        return;
                    }

                    auto auth = req_[boost::beast::http::field::authorization];
                    std::string authStr {auth.data(), auth.size()};
                    
                    std::string login, password;
                    if (boost::starts_with(authStr, "Basic ")) {
                        std::string inputAuthStr = boost::trim_copy(authStr.substr(std::string_view("Basic ").length()));
                        std::string basicAuthStr;
                        basicAuthStr.resize(boost::beast::detail::base64::decoded_size(inputAuthStr.length()));
                        auto decodeRes = boost::beast::detail::base64::decode(
                            basicAuthStr.data(), inputAuthStr.data(), inputAuthStr.length()
                        );
                        basicAuthStr.resize(decodeRes.first);
                        auto colonPos = basicAuthStr.find(':');
                        login = basicAuthStr.substr(0, colonPos);
                        if (colonPos != std::string::npos) {
                            password = basicAuthStr.substr(colonPos+1);
                        }
                    }
                    if (!parent_->parent()->checkBasicAuthentication(parent_->port(), login, password)) {
                        auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::unauthorized, req_.version()};
                        res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                        res->set(boost::beast::http::field::www_authenticate, "Basic realm=\""+parent_->realm()+"\"");
                        res->prepare_payload();
                        if (stream_.index() == 1) {
                            boost::beast::http::async_write(
                                std::get<1>(stream_)
                                , *res
                                , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                    delete res;
                                    x->doRead();
                                }
                            );
                        } else {
                            boost::beast::http::async_write(
                                std::get<2>(stream_)
                                , *res
                                , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                    delete res;
                                    x->doRead();
                                }
                            );
                        }
                        return;
                    }

                    auto target = req_.target();
                    std::string targetStr {target.data(), target.size()};

                    std::string pathStr;
                    std::unordered_map<std::string, std::vector<std::string>> queryMap;
                    auto pathQuerySepPos = targetStr.find('?');
                    if (pathQuerySepPos != std::string::npos) {
                        pathStr = targetStr.substr(0, pathQuerySepPos);
                        std::string queryStr = targetStr.substr(pathQuerySepPos+1);
                        queryStr = queryStr.substr(0, queryStr.find('#'));
                        std::vector<std::string> queryParts;
                        boost::split(queryParts, queryStr, boost::is_any_of("&"));
                        for (auto &q : queryParts) {
                            std::vector<std::string> nv;
                            boost::split(nv, q, boost::is_any_of("="));
                            if (nv.size() != 2) {
                                continue;
                            }
                            std::string n = unescape(nv[0]);
                            std::string v = unescape(nv[1]);
                            auto iter = queryMap.find(n);
                            if (iter == queryMap.end()) {
                                queryMap.insert({n, {v}});
                            } else {
                                iter->second.push_back(v);
                            }
                        }
                    } else {
                        pathStr = targetStr;
                    }

                    HandlerFunc handler;
                    if (req_.method() == boost::beast::http::verb::post || req_.method() == boost::beast::http::verb::get) {
                        handler = parent_->parent()->getHandler(parent_->port(), pathStr);
                    }
                    if (!handler) {
                        std::optional<std::tuple<std::filesystem::path,std::string>> fileMappingRes = std::nullopt;
                        if (req_.method() == boost::beast::http::verb::get || req_.method() == boost::beast::http::verb::head) {
                            fileMappingRes = parent_->parent()->getDoc(parent_->port(), pathStr);
                        }
                        if (fileMappingRes) {
                            boost::beast::http::file_body::value_type body;
                            boost::beast::error_code fileEc;
#ifdef _MSC_VER
                            std::wstring fileName_w = std::get<0>(*fileMappingRes).wstring();
                            std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t> converter;
                            std::string fileName_mbs = converter.to_bytes(fileName_w);
                            body.open(fileName_mbs.c_str(), boost::beast::file_mode::scan, fileEc);
#else
                            body.open(std::get<0>(*fileMappingRes).c_str(), boost::beast::file_mode::scan, fileEc);
#endif
                            if (fileEc) {
                                auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::not_found, req_.version()};
                                res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                                res->prepare_payload();
                                if (stream_.index() == 1) {
                                    boost::beast::http::async_write(
                                        std::get<1>(stream_)
                                        , *res
                                        , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                            delete res;
                                            x->doClose(boost::beast::error_code());
                                        }
                                    );
                                } else {
                                    boost::beast::http::async_write(
                                        std::get<2>(stream_)
                                        , *res
                                        , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                            delete res;
                                            x->doClose(boost::beast::error_code());
                                        }
                                    );
                                }
                            } else {
                                auto const size = body.size();
                                if(req_.method() == boost::beast::http::verb::head) {
                                    auto *res = new boost::beast::http::response<boost::beast::http::empty_body> {boost::beast::http::status::ok, req_.version()};
                                    res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                                    res->set(boost::beast::http::field::content_type, std::get<1>(*fileMappingRes));
                                    res->content_length(size);
                                    res->keep_alive(req_.keep_alive());
                                    res->prepare_payload();
                                    if (stream_.index() == 1) {
                                        boost::beast::http::async_write(
                                            std::get<1>(stream_)
                                            , *res
                                            , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                                delete res;
                                                x->doClose(boost::beast::error_code());
                                            }
                                        );
                                    } else {
                                        boost::beast::http::async_write(
                                            std::get<2>(stream_)
                                            , *res
                                            , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                                delete res;
                                                x->doClose(boost::beast::error_code());
                                            }
                                        );
                                    }
                                } else {
                                    auto *res = new boost::beast::http::response<boost::beast::http::file_body> {
                                        std::piecewise_construct
                                        , std::make_tuple(std::move(body))
                                        , std::make_tuple(boost::beast::http::status::ok, req_.version())
                                    };
                                    res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                                    res->set(boost::beast::http::field::content_type, std::get<1>(*fileMappingRes));
                                    res->content_length(size);
                                    res->keep_alive(req_.keep_alive());
                                    res->prepare_payload();
                                    if (stream_.index() == 1) {
                                        boost::beast::http::async_write(
                                            std::get<1>(stream_)
                                            , *res
                                            , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                                delete res;
                                                x->doClose(boost::beast::error_code());
                                            }
                                        );
                                    } else {
                                        boost::beast::http::async_write(
                                            std::get<2>(stream_)
                                            , *res
                                            , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                                delete res;
                                                x->doClose(boost::beast::error_code());
                                            }
                                        );
                                    }
                                }
                            }
                        } else {
                            auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::not_found, req_.version()};
                            res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                            res->prepare_payload();
                            if (stream_.index() == 1) {
                                boost::beast::http::async_write(
                                    std::get<1>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                        delete res;
                                        x->doClose(boost::beast::error_code());
                                    }
                                );
                            } else {
                                boost::beast::http::async_write(
                                    std::get<2>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                        delete res;
                                        x->doClose(boost::beast::error_code());
                                    }
                                );
                            }
                        }
                    } else {
                        auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::ok, req_.version()};
                        res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                        res->set(boost::beast::http::field::content_type, "application/json");
                        res->keep_alive(req_.keep_alive());
                        if (!handler(
                            login
                            , req_.body()
                            , queryMap
                            , [res,x=shared_from_this()](std::string const &resp) {
                                x->writeResp(res, resp);
                            }
                        )) {
                            res->result(boost::beast::http::status::not_implemented);
                            res->prepare_payload();
                            if (stream_.index() == 1) {
                                boost::beast::http::async_write(
                                    std::get<1>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                        delete res;
                                        x->doClose(boost::beast::error_code());
                                    }
                                );
                            } else {
                                boost::beast::http::async_write(
                                    std::get<2>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                        delete res;
                                        x->doClose(boost::beast::error_code());
                                    }
                                );
                            }
                        }
                    }
                }
                void writeResp(boost::beast::http::response<boost::beast::http::string_body> *res, std::string const &resp) {
                    res->body() = resp;
                    res->prepare_payload();
                    if (stream_.index() == 1) {
                        boost::beast::http::async_write(
                            std::get<1>(stream_)
                            , *res
                            , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                delete res;
                                x->doClose(boost::beast::error_code());
                            }
                        );
                    } else {
                        boost::beast::http::async_write(
                            std::get<2>(stream_)
                            , *res
                            , [x=shared_from_this(),res](boost::system::error_code const &write_ec, std::size_t bytes_written) {
                                delete res;
                                x->doClose(boost::beast::error_code());
                            }
                        );
                    }
                }
                void doClose(boost::beast::error_code ec) {
                    if (stream_.index() == 1) {
                        std::get<1>(stream_).socket().shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
                    } else {
                        std::get<2>(stream_).async_shutdown(
                            boost::beast::bind_front_handler(
                                &OneHandler::onClose
                                , shared_from_this()
                            )
                        );
                    }
                }
                void onClose(boost::beast::error_code) {}
            };
        public:
            Acceptor(
                JsonRESTComponentImpl *parent
                , int port
                , std::optional<TLSServerInfo> const &sslInfo
                , basic::LoggingComponentBase *logger
            )
                : parent_(parent)
                , svc_()
                , sslCtx_(
                    sslInfo
                    ? std::optional<boost::asio::ssl::context>(boost::asio::ssl::context {boost::asio::ssl::context::tlsv12})
                    : std::nullopt
                )
                , logger_(logger)
                , port_(port)
                , th_()
                , running_(true)
                , acceptor_(svc_)
                , realm_(std::string("tm_kit_json_rest_")+std::to_string(port)+"@"+hostname_util::hostname())
            {
                auto addr = boost::asio::ip::make_address("0.0.0.0");
                if (sslCtx_) {
                    sslCtx_->use_certificate_chain_file(sslInfo->serverCertificateFile);
                    sslCtx_->use_private_key_file(sslInfo->serverKeyFile, boost::asio::ssl::context::file_format::pem);
                }
                boost::asio::ip::tcp::endpoint ep(addr, port_);
                boost::beast::error_code ec;

                acceptor_.open(ep.protocol(), ec);
                if (ec) {
                    throw JsonRESTComponentException("Cannot open acceptor on port "+std::to_string(port_));
                }
                acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
                if (ec) {
                    throw JsonRESTComponentException("Cannot set reuse_addr on port "+std::to_string(port_));
                }
                acceptor_.bind(ep, ec);
                if (ec) {
                    throw JsonRESTComponentException("Cannot bind to port "+std::to_string(port_));
                }
                acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
                if (ec) {
                    throw JsonRESTComponentException("Cannot listen on port "+std::to_string(port_));
                }

                th_ = std::thread([this]() {
                    boost::asio::io_context::work work(svc_);
                    svc_.run();
                });
                th_.detach();
            }
            ~Acceptor() {
                running_ = false;
                try {
                    svc_.stop();
                    th_.join();
                } catch (...) {}
            }
            void run() {
                acceptor_.async_accept(
                    boost::asio::make_strand(svc_)
                    , boost::beast::bind_front_handler(
                        &Acceptor::onAccept
                        , shared_from_this()
                    )
                );
            }
            void onAccept(boost::beast::error_code ec, boost::asio::ip::tcp::socket socket) {
                if (!running_) {
                    return;
                }
                if (!ec) {
                    std::make_shared<OneHandler>(this, std::move(socket), sslCtx_)->run();
                    acceptor_.async_accept(
                        boost::asio::make_strand(svc_)
                        , boost::beast::bind_front_handler(
                            &Acceptor::onAccept
                            , shared_from_this()
                        )
                    );
                } else {
                    log(
                        infra::LogLevel::Error
                        , "[JsonRESTComponent::Acceptor::onAccept] ASIO error '"+ec.message()+"' for port "+std::to_string(port_)
                    );
                    parent_->removeAcceptor(port_);
                }
            }
            int port() const {
                return port_;
            }
            JsonRESTComponentImpl *parent() const {
                return parent_;
            }
            std::string const &realm() const {
                return realm_;
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
            void log(infra::LogLevel l, std::string const &s) {
                if (logger_) {
                    logger_->logThroughLoggingComponentBase(l, s);
                }
            }
        };

        std::unordered_map<int, std::shared_ptr<Acceptor>> acceptorMap_;
        mutable std::mutex acceptorMapMutex_;

        using PasswordMap = std::unordered_map<std::string, std::optional<std::string>>;
        std::unordered_map<int, PasswordMap> allPasswords_;
        mutable std::mutex allPasswordsMutex_;

        void startAcceptor(int port, TLSServerConfigurationComponent const *tlsConfig, basic::LoggingComponentBase *logger) {
            auto sslInfo = (tlsConfig?(tlsConfig->getConfigurationItem(
                TLSServerInfoKey {port}
            )):std::nullopt);
            std::lock_guard<std::mutex> _(acceptorMapMutex_);
            auto iter = acceptorMap_.insert({port, std::make_shared<Acceptor>(
                this
                , port
                , sslInfo
                , logger
            )}).first;
            iter->second->run();
        }
    public:
        JsonRESTComponentImpl() : handlerMap_(), handlerMapMutex_(), docRootMap_(), docRootMapMutex_(), started_(false), clientSet_(), clientSetMutex_(), clientSvc_(), clientThread_(), acceptorMap_(), acceptorMapMutex_(), allPasswords_(), allPasswordsMutex_() {
            clientThread_ = std::thread([this]() {
                boost::asio::io_context::work work(clientSvc_);
                clientSvc_.run();
            });
            clientThread_.detach();
        }
        ~JsonRESTComponentImpl() {
            try {
                clientSvc_.stop();
                clientThread_.join();
            } catch (...) {}
        }
        void addJsonRESTClient(ConnectionLocator const &locator, std::string &&request, std::function<
            void(std::string &&)
        > const &clientCallback
        , TLSClientConfigurationComponent const *config
        , basic::LoggingComponentBase *logger) {
            auto client = std::make_shared<OneClient>(
                this
                , locator
                , std::move(request)
                , clientCallback 
                , &clientSvc_
                , (config?config->getConfigurationItem(
                    TLSClientInfoKey {locator.host(), locator.port()}
                ):std::nullopt)
                , logger
            );
            if (!client->initializationFailure()) {
                {
                    std::lock_guard<std::mutex> _(clientSetMutex_);
                    clientSet_.insert(client);
                }
                client->run();
            }
        }
        void removeJsonRESTClient(std::shared_ptr<OneClient> const &p) {
            std::lock_guard<std::mutex> _(clientSetMutex_);
            clientSet_.erase(p);
        }
        void registerHandler(ConnectionLocator const &locator, HandlerFunc const &handler, TLSServerConfigurationComponent const *tlsConfig, basic::LoggingComponentBase *logger) {
            if (locator.userName() != "") {
                if (locator.password() == "") {
                    addBasicAuthentication(locator.port(), locator.userName(), std::nullopt);
                } else {
                    addBasicAuthentication(locator.port(), locator.userName(), locator.password());
                }
            }
            std::lock_guard<std::mutex> _(handlerMapMutex_);
            bool newPort = false;
            auto iter = handlerMap_.find(locator.port());
            if (iter == handlerMap_.end()) {
                iter = handlerMap_.insert({locator.port(), {}}).first;
                newPort = true;
            }
            std::string path = locator.identifier();
            if (!boost::starts_with(path, "/")) {
                path = std::string("/")+path;
            }
            auto innerIter = iter->second.find(path);
            if (innerIter == iter->second.end()) {
                iter->second.insert({path, handler});
            }
            if (newPort && started_) {
                startAcceptor(iter->first, tlsConfig, logger);
            }
        }
        void addBasicAuthentication(int port, std::string const &login, std::optional<std::string> const &password) {
            std::lock_guard<std::mutex> _(allPasswordsMutex_);
            auto iter = allPasswords_.find(port);
            if (iter == allPasswords_.end()) {
                iter = allPasswords_.insert({port, PasswordMap{}}).first;
            }
            if (!password) {
                iter->second[login] = password;
                return;
            }
            std::string hashed;
            hashed.resize(crypto_pwhash_STRBYTES);
            if (crypto_pwhash_str(
                hashed.data(), password->data(), password->length()
                , crypto_pwhash_OPSLIMIT_MIN, crypto_pwhash_MEMLIMIT_MIN
            ) != 0) {
                throw JsonRESTComponentException("Error hashing password for login '"+login+"' on port "+std::to_string(port));
            }
            iter->second[login] = std::optional<std::string> {hashed};
        }
        void addBasicAuthentication_salted(int port, std::string const &login, std::string const &saltedPassword) {
            std::lock_guard<std::mutex> _(allPasswordsMutex_);
            auto iter = allPasswords_.find(port);
            if (iter == allPasswords_.end()) {
                iter = allPasswords_.insert({port, PasswordMap{}}).first;
            }
            iter->second[login] = std::optional<std::string> {saltedPassword};
        }
        void setDocRoot(int port, std::filesystem::path const &docRoot) {
            std::lock_guard<std::mutex> _(docRootMapMutex_);
            docRootMap_[port] = docRoot;
        }

        void finalizeEnvironment(TLSServerConfigurationComponent const *tlsConfig, basic::LoggingComponentBase *logger) {
            std::lock_guard<std::mutex> _(handlerMapMutex_);
            for (auto const &item : handlerMap_) {
                startAcceptor(item.first, tlsConfig, logger);
            }
            std::lock_guard<std::mutex> _m2(docRootMapMutex_);
            for (auto const &item : docRootMap_) {
                if (handlerMap_.find(item.first) == handlerMap_.end()) {
                    startAcceptor(item.first, tlsConfig, logger);
                }
            }
            started_ = true;
        }
        std::unordered_map<ConnectionLocator, std::thread::native_handle_type> json_rest_threadHandles() {
            std::unordered_map<ConnectionLocator, std::thread::native_handle_type> retVal;
            retVal[ConnectionLocator()] = clientThread_.native_handle();
            {
                std::lock_guard<std::mutex> _(acceptorMapMutex_);
                for (auto &item : acceptorMap_) {
                    ConnectionLocator l {"", item.first, "", ""};
                    retVal[l] = item.second->getThreadHandle();
                }
            }
            return retVal;
        }
        void removeAcceptor(int port) {
            std::lock_guard<std::mutex> _(acceptorMapMutex_);
            acceptorMap_.erase(port);
        }
        HandlerFunc getHandler(int port, std::string const &path) const {
            std::lock_guard<std::mutex> _(handlerMapMutex_);
            auto iter = handlerMap_.find(port);
            if (iter == handlerMap_.end()) {
                return HandlerFunc {};
            }
            auto innerIter = iter->second.find(path);
            if (innerIter == iter->second.end()) {
                return HandlerFunc {};
            }
            return innerIter->second;
        }
        bool checkBasicAuthentication(int port, std::string const &login, std::string const &password) const {
            std::lock_guard<std::mutex> _(allPasswordsMutex_);
            auto iter = allPasswords_.find(port);
            if (iter == allPasswords_.end()) {
                return true;
            }
            auto innerIter = iter->second.find(login);
            if (innerIter == iter->second.end()) {
                return false;
            }
            if (!innerIter->second) {
                return true;
            }
            return (crypto_pwhash_str_verify(
                innerIter->second->c_str(), password.c_str(), password.length()
            ) == 0);
        }
        std::optional<std::tuple<std::filesystem::path, std::string>> getDoc(int port, std::string const &path) const {
            static const std::unordered_map<std::string, std::string> mimeMap {
                {".html", "text/html"}
                , {".htm", "text/html"}
                , {".css", "text/css"}
                , {".txt", "text/plain"}
                , {".js", "application/javascript"}
                , {".json", "application/json"}
                , {".xml", "application/xml"}
                , {".png", "image/png"}
                , {".jpe", "image/jpeg"}
                , {".jpeg", "image/jpeg"}
                , {".jpg", "image/jpeg"}
                , {".gif", "image/gif"}
                , {".bmp", "image/bmp"}
                , {".ico", "image/vnd.microsoft.icon"}
                , {".tiff", "image/tiff"}
                , {".tif", "image/tiff"}
                , {".svg", "image/svg+xml"}
                , {".svgz", "image/svg+xml"}
                , {".pdf", "application/pdf"}
                , {".wav", "audio/wav"}
                , {".mp3", "audio/mpeg"}
                , {".mp4", "video/mp4"}
                , {".mpeg", "video/mpeg"}
                , {".mpg", "video/mpeg"}
                , {".dat", "application/octet-stream"}
                , {".bin", "application/octet-stream"}
            };
            static const std::string DEFAULT_MIME = "application/text";
            if (!boost::starts_with(path, "/") || path.find("..") != std::string::npos) {
                return std::nullopt;
            }
            std::lock_guard<std::mutex> _(docRootMapMutex_);
            auto iter = docRootMap_.find(port);
            if (iter == docRootMap_.end()) {
                return std::nullopt;
            }
            auto fullPath = iter->second / path.substr(1);
            if (fullPath.filename() == "") {
                fullPath = fullPath / "index.html";
            }
            if (!std::filesystem::exists(fullPath)) {
                return std::nullopt;
            }
#ifdef _MSC_VER
            auto suffix_w = fullPath.extension();
            std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t> converter;
            std::string suffix = converter.to_bytes(suffix_w);
#else
            auto suffix = fullPath.extension();
#endif
            auto mimeIter = mimeMap.find(suffix);
            std::string mime = DEFAULT_MIME;
            if (mimeIter != mimeMap.end()) {
                mime = mimeIter->second;
            }
            return std::tuple<std::filesystem::path,std::string> {fullPath, mime};
        }
    };

    JsonRESTComponent::JsonRESTComponent() : impl_(std::make_unique<JsonRESTComponentImpl>()) {}
    JsonRESTComponent::JsonRESTComponent(JsonRESTComponent &&) = default;
    JsonRESTComponent &JsonRESTComponent::operator=(JsonRESTComponent &&) = default;
    JsonRESTComponent::~JsonRESTComponent() = default;
    void JsonRESTComponent::addJsonRESTClient(ConnectionLocator const &locator, std::string &&request, std::function<
        void(std::string &&)
    > const &clientCallback) {
        impl_->addJsonRESTClient(locator, std::move(request), clientCallback, dynamic_cast<TLSClientConfigurationComponent const *>(this), dynamic_cast<basic::LoggingComponentBase *>(this));
    }
    void JsonRESTComponent::registerHandler(ConnectionLocator const &locator, std::function<
        bool(std::string const &, std::string const &, std::unordered_map<std::string, std::vector<std::string>> const &, std::function<void(std::string const &)> const &)
    > const &handler) {
        impl_->registerHandler(locator, handler, dynamic_cast<TLSServerConfigurationComponent const *>(this), dynamic_cast<basic::LoggingComponentBase *>(this));
    }
    void JsonRESTComponent::addBasicAuthentication(int port, std::string const &login, std::optional<std::string> const &password) {
        impl_->addBasicAuthentication(port, login, password);
    }
    void JsonRESTComponent::addBasicAuthentication_salted(int port, std::string const &login, std::string const &saltedPassword) {
        impl_->addBasicAuthentication_salted(port, login, saltedPassword);
    }
    void JsonRESTComponent::setDocRoot(int port, std::filesystem::path const &docRoot) {
        impl_->setDocRoot(port, docRoot);
    }
    void JsonRESTComponent::finalizeEnvironment() {
        impl_->finalizeEnvironment(dynamic_cast<TLSServerConfigurationComponent const *>(this), dynamic_cast<basic::LoggingComponentBase *>(this));
    }
    std::unordered_map<ConnectionLocator, std::thread::native_handle_type> JsonRESTComponent::json_rest_threadHandles() {
        return impl_->json_rest_threadHandles();
    }

} } } } }