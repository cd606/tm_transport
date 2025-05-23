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
#include <boost/asio/system_timer.hpp>
#include <boost/config.hpp>
#include <boost/algorithm/string.hpp>

#include <openssl/ssl.h>

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
#include <sodium/crypto_generichash.h>
#include <sodium/randombytes.h>

#include "../BoostCertifyAdaptor.hpp"

#include <curlpp/cURLpp.hpp>
#include <curlpp/Easy.hpp>
#include <curlpp/Options.hpp>
#include <curlpp/Infos.hpp>
#include <curlpp/Exception.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    class JsonRESTComponentImpl {
    private:
        curlpp::Cleanup cleaner_;
        curlpp::Easy curlppEasy_;
        std::mutex curlppEasyMutex_;

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
            int port_;
            std::string urlQueryPart_;
            std::string request_;
            std::function<void(unsigned, std::string &&, std::unordered_map<std::string,std::string> &&)> callback_;
            std::string contentType_;
            std::optional<std::string> method_;
            bool parseHeader_;

            boost::asio::io_context *svc_;
            std::optional<boost::asio::ssl::context> sslCtx_;
            basic::LoggingComponentBase *logger_;
            bool noVerify_;
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
        public:
            OneClient(
                JsonRESTComponentImpl *parent
                , ConnectionLocator const &locator
                , int port
                , std::string &&urlQueryPart
                , std::string &&request
                , std::function<void(unsigned, std::string &&, std::unordered_map<std::string,std::string> &&)> const &callback
                , std::string const &contentType
                , std::optional<std::string> const &method
                , boost::asio::io_context *svc
                , std::optional<TLSClientInfo> const &sslInfo
                , basic::LoggingComponentBase *logger
                , bool noVerify
            )
                : parent_(parent)
                , locator_(locator)
                , port_(port)
                , urlQueryPart_(std::move(urlQueryPart))
                , request_(std::move(request))
                , callback_(callback)
                , contentType_(contentType)
                , method_(method)
                , parseHeader_(locator.query("parse_header", "false") == "true")
                , svc_(svc)
                , sslCtx_(
                    sslInfo
                    ? std::optional<boost::asio::ssl::context>(boost::asio::ssl::context {boost::asio::ssl::context::tlsv12_client})
                    : std::nullopt
                )
                , logger_(logger)
                , noVerify_(noVerify)
                , initializationFailure_(false)
                , resolver_(boost::asio::make_strand(*svc_))
                , stream_()
                , buffer_()
                , req_()
                , res_()
            {
                if (sslInfo) {
                    std::string caCert;
                    if (sslInfo->caCertificateFile != "") {
                        std::ifstream ifs(sslInfo->caCertificateFile.c_str());
                        caCert = std::string(
                            std::istreambuf_iterator<char>{ifs}, {}
                        );
                        ifs.close();
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
                    } else {
                        boost_certify_adaptor::initializeSslCtx(*sslCtx_, noVerify_);
                    }
                    
                    stream_.emplace<2>(boost::asio::make_strand(*svc_), *sslCtx_);
                } else {
                    stream_.emplace<1>(boost::asio::make_strand(*svc_));
                }
            }
            ~OneClient() {
            }
            void run() {
                if (method_) {
                    req_.method(boost::beast::http::string_to_verb(*method_));
                } else {
                    bool useGet = (urlQueryPart_.length() > 0 && request_.length() == 0);
                    req_.method(
                        useGet
                        ?
                        boost::beast::http::verb::get
                        :
                        boost::beast::http::verb::post
                    );
                }
                std::string target = locator_.identifier();
                if (!boost::starts_with(target, "/")) {
                    target = "/"+target;
                }
                if (urlQueryPart_.length() > 0) {
                    target = target+"?"+urlQueryPart_;
                }
                req_.target(target);
                req_.set(boost::beast::http::field::host, locator_.host());
                req_.set(boost::beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
                locator_.for_all_properties([this](std::string const &key, std::string const &value) {
                    if (boost::starts_with(key, "header/")) {
                        req_.set(key.substr(std::string_view("header/").length()), value);
                    } else if (boost::starts_with(key, "http_header/")) {
                        req_.set(key.substr(std::string_view("http_header/").length()), value);
                    }
                });
                auto tokenStr = locator_.query("auth_token", "");
                if (tokenStr != "") {
                    req_.set(boost::beast::http::field::authorization, "Bearer "+tokenStr);
                } else if (locator_.userName() != "") {
                    std::string authStringOrig = locator_.userName()+":"+locator_.password();
                    std::string authString;
                    authString.resize(boost::beast::detail::base64::encoded_size(authStringOrig.length()));
                    authString.resize(boost::beast::detail::base64::encode(authString.data(), reinterpret_cast<uint8_t const *>(authStringOrig.data()), authStringOrig.length()));

                    req_.set(boost::beast::http::field::authorization, "Basic "+authString);
                }
                if (locator_.query("no_content_type", "false") != "true") {
                    req_.set(boost::beast::http::field::content_type, contentType_);
                }
                req_.body() = std::move(request_);
                req_.prepare_payload();

                if (stream_.index() == 2) {
                    if (!boost_certify_adaptor::setHostName(std::get<2>(stream_), locator_.host())) {
                        if (logger_) {
                            logger_->logThroughLoggingComponentBase(
                                infra::LogLevel::Error
                                , "[JsonRESTComponent::OneClient::run] set_server_hostname error for locator '"+locator_.toSerializationFormat()+"'"
                            );
                        }
                        parent_->removeJsonRESTClient(shared_from_this());
                        return;
                    }
                    if(!SSL_set_tlsext_host_name(std::get<2>(stream_).native_handle(), locator_.host().data())) {
                        if (logger_) {
                            logger_->logThroughLoggingComponentBase(
                                infra::LogLevel::Error
                                , "[JsonRESTComponent::OneClient::run] SSL set tlsext host name error for locator '"+locator_.toSerializationFormat()+"'"
                            );
                        }
                        parent_->removeJsonRESTClient(shared_from_this());
                        return;
                    }
                }

                resolver_.async_resolve(
                    locator_.host()
                    , std::to_string(port_)
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
                std::unordered_map<std::string,std::string> headerFields;
                if (parseHeader_) {
                    for (auto &h : res_.base()) {
                        headerFields.insert(std::make_pair(h.name_string(), h.value()));
                    }
                }
                callback_(static_cast<unsigned>(res_.result()), std::move(res_.body()), std::move(headerFields));
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
            void onShutdown(boost::beast::error_code) {
                parent_->removeJsonRESTClient(shared_from_this());
            }
            bool initializationFailure() const {
                return initializationFailure_;
            }
        };
        class OneKeepAliveClient : public std::enable_shared_from_this<OneKeepAliveClient> {
        public:
            struct OneRequest {
                ConnectionLocator reqLocator;
                std::string urlQueryPart;
                std::string request;
                std::function<void(unsigned, std::string &&, std::unordered_map<std::string,std::string> &&)> callback;
                std::optional<std::string> method;
                std::string contentType;
            };
        private:
            JsonRESTComponentImpl *parent_;
            std::string host_;
            int port_;
            std::mutex requestsMutex_;
            std::deque<std::unique_ptr<OneRequest>> requests_;

            struct HTTPRequest {
                boost::beast::http::request<boost::beast::http::string_body> httpReq;
                boost::beast::flat_buffer buffer; 
                boost::beast::http::response<boost::beast::http::string_body> res;
                std::function<void(unsigned, std::string &&, std::unordered_map<std::string,std::string> &&)> callback;
            };
            std::deque<std::unique_ptr<HTTPRequest>> processingQueue_;

            boost::asio::io_context *svc_;
            std::optional<TLSClientInfo> sslInfo_;
            std::optional<boost::asio::ssl::context> sslCtx_;
            basic::LoggingComponentBase *logger_;
            bool noVerify_;
            bool initializationFailure_;

            boost::asio::ip::tcp::resolver resolver_;
            using StreamVariant = std::variant<
                std::monostate 
                , boost::beast::tcp_stream
                , boost::beast::ssl_stream<boost::beast::tcp_stream>
            >;
            std::unique_ptr<StreamVariant> stream_;

            std::atomic<bool> running_;
            std::mutex writeMutex_;

            void buildRequest(
                boost::beast::http::request<boost::beast::http::string_body> &req
                , OneRequest &&input
            ) {
                req.method(boost::beast::http::verb::post);
                if (input.method) {
                    req.method(boost::beast::http::string_to_verb(*(input.method)));
                } else {
                    bool useGet = (input.urlQueryPart.length() > 0 && input.request.length() == 0);
                    req.method(
                        useGet
                        ?
                        boost::beast::http::verb::get
                        :
                        boost::beast::http::verb::post
                    );
                }
                std::string target = input.reqLocator.identifier();
                if (!boost::starts_with(target, "/")) {
                    target = "/"+target;
                }
                if (input.urlQueryPart.length() > 0) {
                    target = target+"?"+input.urlQueryPart;
                }
                req.target(target);
                req.set(boost::beast::http::field::host, host_);
                req.set(boost::beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
                input.reqLocator.for_all_properties([&req](std::string const &key, std::string const &value) {
                    if (boost::starts_with(key, "header/")) {
                        req.set(key.substr(std::string_view("header/").length()), value);
                    } else if (boost::starts_with(key, "http_header/")) {
                        req.set(key.substr(std::string_view("http_header/").length()), value);
                    }
                });
                auto tokenStr = input.reqLocator.query("auth_token", "");
                if (tokenStr != "") {
                    req.set(boost::beast::http::field::authorization, "Bearer "+tokenStr);
                } else if (input.reqLocator.userName() != "") {
                    std::string authStringOrig = input.reqLocator.userName()+":"+input.reqLocator.password();
                    std::string authString;
                    authString.resize(boost::beast::detail::base64::encoded_size(authStringOrig.length()));
                    authString.resize(boost::beast::detail::base64::encode(authString.data(), reinterpret_cast<uint8_t const *>(authStringOrig.data()), authStringOrig.length()));

                    req.set(boost::beast::http::field::authorization, "Basic "+authString);
                }
                if (input.contentType != "") {
                    req.set(boost::beast::http::field::content_type, input.contentType);
                }
                req.body() = std::move(input.request);
                req.keep_alive(true);
                req.prepare_payload();
            }
        public:
            OneKeepAliveClient(
                JsonRESTComponentImpl *parent
                , std::string const &host
                , int port
                , boost::asio::io_context *svc
                , std::optional<TLSClientInfo> const &sslInfo
                , basic::LoggingComponentBase *logger
                , std::unique_ptr<OneRequest> &&initialRequest
                , bool noVerify
            )
                : parent_(parent)
                , host_(host)
                , port_(port)
                , requestsMutex_()
                , requests_()
                , processingQueue_()
                , svc_(svc)
                , sslInfo_(sslInfo)
                , sslCtx_(
                    sslInfo
                    ? std::optional<boost::asio::ssl::context>(boost::asio::ssl::context {boost::asio::ssl::context::tlsv12_client})
                    : std::nullopt
                )
                , logger_(logger)
                , noVerify_(noVerify)
                , initializationFailure_(false)
                , resolver_(boost::asio::make_strand(*svc_))
                , stream_()
                , running_(true)
                , writeMutex_()
            {
                {
                    std::lock_guard<std::mutex> _(requestsMutex_);
                    requests_.push_back(std::move(initialRequest));
                }
                if (sslInfo) {
                    std::string caCert;
                    if (sslInfo->caCertificateFile != "") {
                        std::ifstream ifs(sslInfo->caCertificateFile.c_str());
                        caCert = std::string(
                            std::istreambuf_iterator<char>{ifs}, {}
                        );
                        ifs.close();
                        boost::system::error_code ec;
                        sslCtx_->add_certificate_authority(
                            boost::asio::buffer(caCert.data(), caCert.length())
                            , ec
                        );
                        if (ec) {
                            if (logger_) {
                                logger_->logThroughLoggingComponentBase(
                                    infra::LogLevel::Error
                                    , "[JsonRESTComponent::OneKeepAliveClient::(constructor)] ASIO error '"+ec.message()+"' for locator '"+locator().toSerializationFormat()+"'"
                                );
                            }
                            initializationFailure_ = true;
                            return;
                        }
                    } else {
                        boost_certify_adaptor::initializeSslCtx(*sslCtx_, noVerify_);
                    }
                    
                    stream_ = std::make_unique<StreamVariant>(
                        std::in_place_index<2>, boost::asio::make_strand(*svc_), *sslCtx_
                    );
                } else {
                    stream_ = std::make_unique<StreamVariant>(
                        std::in_place_index<1>, boost::asio::make_strand(*svc_)
                    );
                }
            }
            ~OneKeepAliveClient() {
                running_ = false;
                if (stream_->index() == 1) {
                    try {
                        std::get<1>(*stream_).socket().shutdown(
                            boost::asio::ip::tcp::socket::shutdown_both
                        );
                    } catch (...) {
                    }
                } else {
                    try {
                        std::get<2>(*stream_).shutdown();
                    } catch (...) {
                    }
                }
            }
            void run() {
                if (stream_->index() == 2) {
                    if (!boost_certify_adaptor::setHostName(std::get<2>(*stream_), host_)) {
                        if (logger_) {
                            logger_->logThroughLoggingComponentBase(
                                infra::LogLevel::Error
                                , "[JsonRESTComponent::OneKeepAliveClient::run] set_server_hostname error for locator '"+locator().toSerializationFormat()+"'"
                            );
                        }
                        parent_->removeKeepAliveJsonRESTClient(shared_from_this());
                        return;
                    }
                    if(!SSL_set_tlsext_host_name(std::get<2>(*stream_).native_handle(), host_.data())) {
                        if (logger_) {
                            logger_->logThroughLoggingComponentBase(
                                infra::LogLevel::Error
                                , "[JsonRESTComponent::OneKeepAliveClient::run] SSL set tlsext host name error for locator '"+locator().toSerializationFormat()+"'"
                            );
                        }
                        parent_->removeKeepAliveJsonRESTClient(shared_from_this());
                        return;
                    }
                }

                resolver_.async_resolve(
                    host_
                    , std::to_string(port_)
                    , boost::beast::bind_front_handler(
                        &OneKeepAliveClient::onResolve
                        , shared_from_this()
                    )
                );
            }
            void onResolve(boost::beast::error_code ec, boost::asio::ip::tcp::resolver::results_type results) {
                if (ec) {
                    if (logger_) {
                        logger_->logThroughLoggingComponentBase(
                            infra::LogLevel::Error
                            , "[JsonRESTComponent::OneKeepAliveClient::onResolve] ASIO error '"+ec.message()+"' for locator '"+locator().toSerializationFormat()+"'"
                        );
                    }
                    parent_->removeKeepAliveJsonRESTClient(shared_from_this());
                    return;
                }
                if (stream_->index() == 1) {
                    std::get<1>(*stream_).expires_after(std::chrono::seconds(30));
                    std::get<1>(*stream_).async_connect(
                        results 
                        , boost::beast::bind_front_handler(
                            &OneKeepAliveClient::onConnect 
                            , shared_from_this()
                        )
                    );
                } else {
                    boost::beast::get_lowest_layer(std::get<2>(*stream_)).expires_after(std::chrono::seconds(30));
                    boost::beast::get_lowest_layer(std::get<2>(*stream_)).async_connect(
                        results
                        , boost::beast::bind_front_handler(
                            &OneKeepAliveClient::onConnect
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
                            , "[JsonRESTComponent::OneKeepAliveClient::onConnect] ASIO error '"+ec.message()+"' for locator '"+locator().toSerializationFormat()+"'"
                        );
                    }
                    parent_->removeKeepAliveJsonRESTClient(shared_from_this());
                    return;
                }
                if (stream_->index() == 1) {
                    std::lock_guard<std::mutex> _(requestsMutex_);
                    while (!requests_.empty()) {
                        processingQueue_.push_back(std::make_unique<HTTPRequest>());
                        processingQueue_.back()->callback = requests_.front()->callback;
                        buildRequest(processingQueue_.back()->httpReq, std::move(*requests_.front()));
                        requests_.pop_front();
                    }
                    if (!processingQueue_.empty()) {
                        {
                            std::lock_guard<std::mutex> _(writeMutex_);
                            std::get<1>(*stream_).expires_after(std::chrono::seconds(30));
                            boost::beast::http::write(
                                std::get<1>(*stream_)
                                , processingQueue_.front()->httpReq
                            );
                        }
                        boost::beast::http::async_read(
                            std::get<1>(*stream_)
                            , processingQueue_.front()->buffer
                            , processingQueue_.front()->res
                            , boost::beast::bind_front_handler(
                                &OneKeepAliveClient::onRead
                                , shared_from_this()
                            )
                        );
                    }
                } else {
                    std::get<2>(*stream_).async_handshake(
                        boost::asio::ssl::stream_base::client
                        , boost::beast::bind_front_handler(
                            &OneKeepAliveClient::onSSLHandshake
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
                            , "[JsonRESTComponent::OneKeepAliveClient::onSSLHandshake] ASIO error '"+ec.message()+"' for locator '"+locator().toSerializationFormat()+"'"
                        );
                    }
                    parent_->removeKeepAliveJsonRESTClient(shared_from_this());
                    return;
                }
                {
                    std::lock_guard<std::mutex> _(requestsMutex_);
                    while (!requests_.empty()) {
                        processingQueue_.push_back(std::make_unique<HTTPRequest>());
                        processingQueue_.back()->callback = requests_.front()->callback;
                        buildRequest(processingQueue_.back()->httpReq, std::move(*requests_.front()));
                        requests_.pop_front();
                    }
                    if (!processingQueue_.empty()) {
                        {
                            std::lock_guard<std::mutex> _(writeMutex_);
                            boost::beast::get_lowest_layer(std::get<2>(*stream_)).expires_after(std::chrono::seconds(30));
                            boost::beast::http::write(
                                std::get<2>(*stream_)
                                , processingQueue_.front()->httpReq
                            );
                        }
                        boost::beast::http::async_read(
                            std::get<2>(*stream_)
                            , processingQueue_.front()->buffer
                            , processingQueue_.front()->res
                            , boost::beast::bind_front_handler(
                                &OneKeepAliveClient::onRead
                                , shared_from_this()
                            )
                        );
                    }
                }
            }
            void onRead(boost::beast::error_code ec, std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);
                if (ec) {
                    if (logger_) {
                        logger_->logThroughLoggingComponentBase(
                            infra::LogLevel::Error
                            , "[JsonRESTComponent::OneKeepAliveClient::onRead] ASIO error '"+ec.message()+"' for locator '"+locator().toSerializationFormat()+"'"
                        );
                    }
                    if (stream_->index() == 1) {
                        try {
                            std::get<1>(*stream_).socket().shutdown(
                                boost::asio::ip::tcp::socket::shutdown_both
                            );
                        } catch (...) {
                        }
                        onShutdown(ec);
                    } else {
                        try {
                            boost::beast::get_lowest_layer(std::get<2>(*stream_)).socket().shutdown(
                                boost::asio::ip::tcp::socket::shutdown_both
                            );
                        } catch (...) {
                        }
                        onShutdown(ec);
                    }
                    return;
                }
                {
                    std::lock_guard<std::mutex> _(requestsMutex_);
                    if (!processingQueue_.empty()) {
                        std::unordered_map<std::string,std::string> headerFields;
                        for (auto &h : processingQueue_.front()->res.base()) {
                            headerFields.insert(std::make_pair(h.name_string(), h.value()));
                        }
                        processingQueue_.front()->callback(
                            static_cast<unsigned>(processingQueue_.front()->res.result()), std::move(processingQueue_.front()->res.body()), std::move(headerFields)
                        );
                        processingQueue_.pop_front();
                    }
                }
                auto timer = std::make_shared<boost::asio::system_timer>(*svc_, std::chrono::system_clock::now()+std::chrono::milliseconds(1));
                timer->async_wait([this,timer](boost::system::error_code const &) {
                    checkAndSend();
                });
            }
            void checkAndSend() {
                std::lock_guard<std::mutex> _(requestsMutex_);
                while (!requests_.empty()) {
                    processingQueue_.push_back(std::make_unique<HTTPRequest>());
                    processingQueue_.back()->callback = requests_.front()->callback;
                    buildRequest(processingQueue_.back()->httpReq, std::move(*requests_.front()));
                    requests_.pop_front();
                }
                if (!processingQueue_.empty()) {
                    std::lock_guard<std::mutex> _(writeMutex_);
                    if (stream_->index() == 1) {
                        boost::beast::get_lowest_layer(std::get<1>(*stream_)).expires_after(std::chrono::seconds(30));
                        boost::beast::http::write(
                            std::get<1>(*stream_)
                            , processingQueue_.front()->httpReq
                        );
                        boost::beast::http::async_read(
                            std::get<1>(*stream_)
                            , processingQueue_.front()->buffer
                            , processingQueue_.front()->res
                            , boost::beast::bind_front_handler(
                                &OneKeepAliveClient::onRead
                                , shared_from_this()
                            )
                        );
                    } else {
                        boost::beast::get_lowest_layer(std::get<2>(*stream_)).expires_after(std::chrono::seconds(30));
                        boost::beast::http::write(
                            std::get<2>(*stream_)
                            , processingQueue_.front()->httpReq
                        );
                        boost::beast::http::async_read(
                            std::get<2>(*stream_)
                            , processingQueue_.front()->buffer
                            , processingQueue_.front()->res
                            , boost::beast::bind_front_handler(
                                &OneKeepAliveClient::onRead
                                , shared_from_this()
                            )
                        );
                    }
                } else {
                    auto timer = std::make_shared<boost::asio::system_timer>(*svc_, std::chrono::system_clock::now()+std::chrono::milliseconds(1));
                    timer->async_wait([this,timer](boost::system::error_code const &) {
                        checkAndSend();
                    });
                }
            }
            void onShutdown(boost::beast::error_code) {
                if (running_) {
                    logger_->logThroughLoggingComponentBase(
                        infra::LogLevel::Info
                        , "[JsonRESTComponent::OneKeepAliveClient::onShutdown] Restarting for locator '"+locator().toSerializationFormat()+"'"
                    );
                    if (sslCtx_) {
                        stream_ = std::make_unique<StreamVariant>(
                            std::in_place_index<2>, boost::asio::make_strand(*svc_), *sslCtx_
                        );
                    } else {
                        stream_ = std::make_unique<StreamVariant>(
                            std::in_place_index<1>, boost::asio::make_strand(*svc_)
                        );
                    }
                    run();
                }
            }
            bool initializationFailure() const {
                return initializationFailure_;
            }
            ConnectionLocator locator() const {
                return ConnectionLocator(host_, port_);
            }
            void addRequest(OneRequest &&req) {
                std::lock_guard<std::mutex> _(requestsMutex_);
                requests_.push_back(std::make_unique<OneRequest>(std::move(req)));
            }
        };
        
        std::unordered_set<std::shared_ptr<OneClient>> clientSet_;
        std::mutex clientSetMutex_;
        std::unordered_map<ConnectionLocator, std::shared_ptr<OneKeepAliveClient>> keepAliveClientMap_;
        std::mutex keepAliveClientMapMutex_;
        boost::asio::io_context *clientSvc_;
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
                void onRead(boost::beast::error_code ec, std::size_t) {
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

                    if (pathStr == JsonRESTComponent::TOKEN_AUTHENTICATION_REQUEST) {
                        if (req_.method() != boost::beast::http::verb::post) {
                            auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::bad_request, req_.version()};
                            res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                            res->prepare_payload();
                            if (stream_.index() == 1) {
                                boost::beast::http::async_write(
                                    std::get<1>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                        delete res;
                                        x->doRead();
                                    }
                                );
                            } else {
                                boost::beast::http::async_write(
                                    std::get<2>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                        delete res;
                                        x->doRead();
                                    }
                                );
                            }
                            return;
                        }
                        
                        parent_->parent()->scheduleCreateAuthToken(
                            parent_->port()
                            , req_.body()
                            , req_.version()
                            , req_.keep_alive()
                            , [x=shared_from_this()](boost::beast::http::response<boost::beast::http::string_body> *p, std::string const &resp) {
                                if (resp == "") {
                                    x->writeEmptyRespWithoutClose(p);
                                } else {
                                    x->writeResp(p, resp);
                                }
                            }
                        );
                        return;
                    }
                    
                    bool isSimplePost = false;
                    if (req_[boost::beast::http::field::content_type] == "x-www-form-urlencoded") {
                        isSimplePost = true;
                        std::string queryStr = req_.body();
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
                    }

                    HandlerFunc handler;
                    if (req_.method() == boost::beast::http::verb::post || req_.method() == boost::beast::http::verb::get) {
                        handler = parent_->parent()->getHandler(parent_->port(), pathStr);
                    }

                    auto auth = req_[boost::beast::http::field::authorization];
                    std::string authStr {auth.data(), auth.size()};
                    
                    std::string login, password;
                    std::string authToken;
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
                    } else if (boost::starts_with(authStr, "Bearer ")) {
                        authToken = boost::trim_copy(authStr.substr(std::string_view("Bearer ").length()));
                    }
                    if (authToken != "") {
                        auto checkResLogin = parent_->parent()->checkTokenAuthentication(parent_->port(), authToken);
                        if (!checkResLogin) {
                            auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::unauthorized, req_.version()};
                            res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                            res->set(boost::beast::http::field::www_authenticate, "Bearer realm=\""+parent_->realm()+"\"");
                            res->prepare_payload();
                            if (stream_.index() == 1) {
                                boost::beast::http::async_write(
                                    std::get<1>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                        delete res;
                                        x->doRead();
                                    }
                                );
                            } else {
                                boost::beast::http::async_write(
                                    std::get<2>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                        delete res;
                                        x->doRead();
                                    }
                                );
                            }
                            return;
                        }
                        login = *checkResLogin;
                    } else if (parent_->parent()->requiresTokenAuthentication(parent_->port())) {
                        //if token authentication is needed, but the request is for a static file
                        //, we don't enforce authentication.
                        //on the other hand, for dynamic content, we do enforce that.
                        if (handler) {
                            auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::unauthorized, req_.version()};
                            res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                            res->set(boost::beast::http::field::www_authenticate, "Bearer realm=\""+parent_->realm()+"\"");
                            res->prepare_payload();
                            if (stream_.index() == 1) {
                                boost::beast::http::async_write(
                                    std::get<1>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                        delete res;
                                        x->doRead();
                                    }
                                );
                            } else {
                                boost::beast::http::async_write(
                                    std::get<2>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                        delete res;
                                        x->doRead();
                                    }
                                );
                            }
                            return;
                        }
                    } else if (!parent_->parent()->checkBasicAuthentication(parent_->port(), login, password)) {
                        //if basic authentication is needed, then we want it even for
                        //static files
                        auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::unauthorized, req_.version()};
                        res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                        res->set(boost::beast::http::field::www_authenticate, "Basic realm=\""+parent_->realm()+"\"");
                        res->prepare_payload();
                        if (stream_.index() == 1) {
                            boost::beast::http::async_write(
                                std::get<1>(stream_)
                                , *res
                                , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                    delete res;
                                    x->doRead();
                                }
                            );
                        } else {
                            boost::beast::http::async_write(
                                std::get<2>(stream_)
                                , *res
                                , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                    delete res;
                                    x->doRead();
                                }
                            );
                        }
                        return;
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
                                        , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                            delete res;
                                            x->doClose(boost::beast::error_code());
                                        }
                                    );
                                } else {
                                    boost::beast::http::async_write(
                                        std::get<2>(stream_)
                                        , *res
                                        , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
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
                                            , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                                delete res;
                                                x->doClose(boost::beast::error_code());
                                            }
                                        );
                                    } else {
                                        boost::beast::http::async_write(
                                            std::get<2>(stream_)
                                            , *res
                                            , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
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
                                            , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                                delete res;
                                                x->doClose(boost::beast::error_code());
                                            }
                                        );
                                    } else {
                                        boost::beast::http::async_write(
                                            std::get<2>(stream_)
                                            , *res
                                            , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
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
                                    , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                        delete res;
                                        x->doClose(boost::beast::error_code());
                                    }
                                );
                            } else {
                                boost::beast::http::async_write(
                                    std::get<2>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
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
                            , (isSimplePost?"":req_.body())
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
                                    , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                        delete res;
                                        x->doClose(boost::beast::error_code());
                                    }
                                );
                            } else {
                                boost::beast::http::async_write(
                                    std::get<2>(stream_)
                                    , *res
                                    , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
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
                            , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                delete res;
                                x->doClose(boost::beast::error_code());
                            }
                        );
                    } else {
                        boost::beast::http::async_write(
                            std::get<2>(stream_)
                            , *res
                            , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                delete res;
                                x->doClose(boost::beast::error_code());
                            }
                        );
                    }
                }
                void writeEmptyRespWithoutClose(boost::beast::http::response<boost::beast::http::string_body> *res) {
                    res->prepare_payload();
                    if (stream_.index() == 1) {
                        boost::beast::http::async_write(
                            std::get<1>(stream_)
                            , *res
                            , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                delete res;
                                x->doRead();
                            }
                        );
                    } else {
                        boost::beast::http::async_write(
                            std::get<2>(stream_)
                            , *res
                            , [x=shared_from_this(),res](boost::system::error_code const &, std::size_t) {
                                delete res;
                                x->doRead();
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
#if BOOST_VERSION >= 108700
                    auto work_guard = boost::asio::make_work_guard(svc_);
#else
                    boost::asio::io_context::work work(svc_);
#endif
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
        struct TokenPasswordInfo {
            uint8_t secret[64];
            std::unordered_map<std::string, std::string> saltedPasswords;

            TokenPasswordInfo() : secret(), saltedPasswords() {
                randombytes_buf(secret, 64);
            }
        };
        std::unordered_map<int, TokenPasswordInfo> tokenPasswords_;
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

        struct OneTokenReq {
            std::string authReqBody;
            unsigned version;
            bool keepAlive;
            std::function<void(
                boost::beast::http::response<boost::beast::http::string_body> *
                , std::string const &
            )> writer;
        };

        class OneTokenThreadData {
        private:
            JsonRESTComponentImpl *parent_;
            int port_;
            std::thread th_;
            std::atomic<bool> running_;
            std::list<std::unique_ptr<OneTokenReq>> incoming_, processing_;
            std::condition_variable cond_;
            std::mutex mutex_;

            void run() {
                running_ = true;
                while (running_) {
                    std::unique_lock<std::mutex> lock(mutex_);
                    cond_.wait_for(lock, std::chrono::milliseconds(10));
                    if (!running_) {
                        lock.unlock();
                        break;
                    }
                    if (incoming_.empty()) {
                        lock.unlock();
                        continue;
                    }
                    processing_.splice(processing_.end(), incoming_);
                    lock.unlock();
                    while (!processing_.empty()) {
                        auto &x = processing_.front();
                        parent_->createAuthToken(
                            port_, x->authReqBody, x->version, x->keepAlive, x->writer
                        );
                        processing_.pop_front();
                    }
                }
            }
        public:
            OneTokenThreadData(JsonRESTComponentImpl *parent, int port) : parent_(parent), port_(port), th_(), running_(false), incoming_(), processing_(), cond_(), mutex_() {
                th_ = std::thread([this]() {
                    run();
                });
                th_.detach();
            }
            ~OneTokenThreadData() {
                running_ = false;
                try {
                    if (th_.joinable()) {
                        th_.join();
                    }
                } catch (...) {}
            }
            void addRequest(OneTokenReq &&request) {
                {
                    std::lock_guard<std::mutex> _(mutex_);
                    incoming_.push_back(std::make_unique<OneTokenReq>(std::move(request)));
                }
                cond_.notify_one();
            }
        };
        std::unordered_map<int, std::unique_ptr<OneTokenThreadData>> tokenThreads_;
        std::mutex tokenThreadMutex_;
        
    public:
        JsonRESTComponentImpl() : cleaner_(), curlppEasy_(), curlppEasyMutex_(), handlerMap_(), handlerMapMutex_(), docRootMap_(), docRootMapMutex_(), started_(false), clientSet_(), clientSetMutex_(), keepAliveClientMap_(), keepAliveClientMapMutex_(), clientSvc_(new boost::asio::io_context), clientThread_(), acceptorMap_(), acceptorMapMutex_(), allPasswords_(), tokenPasswords_(), allPasswordsMutex_(), tokenThreads_(), tokenThreadMutex_() {
            clientThread_ = std::thread([this]() {
#if BOOST_VERSION >= 108700
                auto work_guard = boost::asio::make_work_guard(*clientSvc_);
#else
                boost::asio::io_context::work work(*clientSvc_);
#endif
                clientSvc_->run();
            });
            clientThread_.detach();
        }
        ~JsonRESTComponentImpl() {
            try {
                clientSvc_->stop();
                clientThread_.join();
            } catch (...) {}
        }
        void addJsonRESTClient(ConnectionLocator const &locator, std::string &&urlQueryPart, std::string &&request, std::function<
            void(unsigned, std::string &&, std::unordered_map<std::string,std::string> &&)
        > const &clientCallback
        , std::string const &contentType
        , std::optional<std::string> const &method
        , TLSClientConfigurationComponent const *config
        , basic::LoggingComponentBase *logger) {
            if (locator.query("use_curlpp", "false") == "true") {
                addJsonRESTClientViaCurlpp(locator, std::move(urlQueryPart), std::move(request), clientCallback, contentType, method, config, logger);
                return;
            }
            int port = locator.port();
            if (port == 0) {
                if (config) {
                    port = 443;
                } else {
                    port = 80;
                }
            }
            bool noVerify = (locator.query("no_verify", "false") == "true");
            if (locator.query("use_keep_alive_client", "false") == "true") {
                std::lock_guard<std::mutex> _(keepAliveClientMapMutex_);
                ConnectionLocator hostAndPort {locator.host(), port};
                auto iter = keepAliveClientMap_.find(hostAndPort);
                if (iter == keepAliveClientMap_.end()) {
                    auto client = std::make_shared<OneKeepAliveClient>(
                        this
                        , locator.host()
                        , port
                        , clientSvc_
                        , (config?config->getConfigurationItem(
                            TLSClientInfoKey {locator.host(), port}
                        ):std::nullopt)
                        , logger
                        , std::make_unique<OneKeepAliveClient::OneRequest>(OneKeepAliveClient::OneRequest {
                            locator
                            , std::move(urlQueryPart)
                            , std::move(request)
                            , clientCallback 
                            , method
                            , contentType
                        }
                        )
                        , noVerify
                    );
                    if (!client->initializationFailure()) {
                        keepAliveClientMap_.insert({hostAndPort, client});
                        client->run();
                    }
                } else {
                    iter->second->addRequest(OneKeepAliveClient::OneRequest {
                        locator
                        , std::move(urlQueryPart)
                        , std::move(request)
                        , clientCallback 
                        , method
                        , contentType
                    });
                }
            } else {
                auto client = std::make_shared<OneClient>(
                    this
                    , locator
                    , port
                    , std::move(urlQueryPart)
                    , std::move(request)
                    , clientCallback 
                    , contentType
                    , method
                    , clientSvc_
                    , (config?config->getConfigurationItem(
                        TLSClientInfoKey {locator.host(), port}
                    ):std::nullopt)
                    , logger
                    , noVerify
                );
                if (!client->initializationFailure()) {
                    {
                        std::lock_guard<std::mutex> _(clientSetMutex_);
                        clientSet_.insert(client);
                    }
                    client->run();
                }
            }
        }
        void addJsonRESTClientViaCurlpp(ConnectionLocator const &locator, std::string &&urlQueryPart, std::string &&request, std::function<
            void(unsigned, std::string &&, std::unordered_map<std::string,std::string> &&)
        > const &clientCallback
        , std::string const &contentType
        , std::optional<std::string> const &method
        , TLSClientConfigurationComponent const *config
        , basic::LoggingComponentBase *logger) {
            try {
                std::lock_guard<std::mutex> _(curlppEasyMutex_);
                curlppEasy_.reset();

                std::ostringstream urlSS;
                urlSS << (config?"https://":"http://");
                urlSS << locator.host();
                if (locator.port() != 0) {
                    urlSS << ':' << locator.port();
                }
                if (!boost::starts_with(locator.identifier(), "/")) {
                    urlSS << '/';
                }
                urlSS << locator.identifier();
                if (urlQueryPart != "") {
                    urlSS << '?' << urlQueryPart;
                }
                //std::cerr << urlSS.str() << "\n";
                curlppEasy_.setOpt(new curlpp::options::Url(urlSS.str()));
                std::string methodStr = "";
                if (method) {
                    methodStr = *method;
                } else {
                    if (urlQueryPart.length() > 0 && request.length() == 0) {
                        methodStr = "GET";
                    } else {
                        methodStr = "POST";
                    }
                }
                if (methodStr != "GET" && methodStr != "POST") {
                    curlppEasy_.setOpt(new curlpp::options::CustomRequest(methodStr));
                }
                std::list<std::string> header;
                header.push_back("Content-Type: "+contentType);
                locator.for_all_properties([&header](std::string const &key, std::string const &value) {
                    if (boost::starts_with(key, "header/")) {
                        header.push_back(key.substr(std::string_view("header/").length())+": "+value);
                    } else if (boost::starts_with(key, "http_header/")) {
                        header.push_back(key.substr(std::string_view("http_header/").length())+": "+value);
                    }
                });

                auto tokenStr = locator.query("auth_token", "");
                if (tokenStr != "") {
                    header.push_back("Authorization: Bearer "+tokenStr);
                } else if (locator.userName() != "") {
                    std::string authStringOrig = locator.userName()+":"+locator.password();
                    std::string authString;
                    authString.resize(boost::beast::detail::base64::encoded_size(authStringOrig.length()));
                    authString.resize(boost::beast::detail::base64::encode(authString.data(), reinterpret_cast<uint8_t const *>(authStringOrig.data()), authStringOrig.length()));

                    header.push_back("Authorization: Basic "+authString);
                }
                curlppEasy_.setOpt(new curlpp::options::HttpHeader(header));

                if (methodStr == "POST") {
                    curlppEasy_.setOpt(new curlpp::options::Post(1));
                    curlppEasy_.setOpt(new curlpp::options::PostFields(request));
                    curlppEasy_.setOpt(new curlpp::options::PostFieldSize(request.length()));
                }

                std::unordered_map<std::string, std::string> retHeaders;
                curlppEasy_.setOpt(new curlpp::options::HeaderFunction(
                    [&retHeaders] (char* buffer, size_t size, size_t items) -> size_t {
                        std::string s(buffer, size * items); 
                        std::vector<std::string> parts;
                        boost::split(parts, s, boost::is_any_of(":"));
                        if (parts.size() >= 2) {
                            retHeaders[boost::trim_copy(parts[0])] = boost::trim_copy(parts[1]);
                        }
                        return size * items;
                    }
                ));

                std::ostringstream response;
                curlppEasy_.setOpt(new curlpp::options::WriteStream(&response));

                curlppEasy_.setOpt(new curlpp::options::Encoding(""));

                curlppEasy_.perform();

                //std::cerr << response.str() << '\n';
                clientCallback(
                    curlpp::infos::ResponseCode::get(curlppEasy_)
                    , response.str()
                    , std::move(retHeaders)
                );
            } catch (curlpp::LogicError const &e) {
                logger->logThroughLoggingComponentBase(infra::LogLevel::Error, e.what());
                clientCallback(500, e.what(), {});
            } catch (curlpp::RuntimeError const &e) {
                logger->logThroughLoggingComponentBase(infra::LogLevel::Error, e.what());
                clientCallback(500, e.what(), {});
            }
        }
        void removeJsonRESTClient(std::shared_ptr<OneClient> const &p) {
            std::lock_guard<std::mutex> _(clientSetMutex_);
            clientSet_.erase(p);
        }
        void removeKeepAliveJsonRESTClient(std::shared_ptr<OneKeepAliveClient> const &p) {
            std::lock_guard<std::mutex> _(keepAliveClientMapMutex_);
            keepAliveClientMap_.erase(p->locator());
        }
        void registerHandler(ConnectionLocator const &locator, HandlerFunc const &handler, TLSServerConfigurationComponent const *tlsConfig, basic::LoggingComponentBase *logger) {
            int port = locator.port();
            if (port == 0) {
                if (tlsConfig) {
                    port = 443;
                } else {
                    port = 80;
                }
            }
            if (locator.userName() != "") {
                if (locator.password() == "") {
                    addBasicAuthentication(port, locator.userName(), std::nullopt);
                } else {
                    addBasicAuthentication(port, locator.userName(), locator.password());
                }
            }
            std::lock_guard<std::mutex> _(handlerMapMutex_);
            bool newPort = false;
            auto iter = handlerMap_.find(port);
            if (iter == handlerMap_.end()) {
                iter = handlerMap_.insert({port, {}}).first;
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
        void addTokenAuthentication(int port, std::string const &login, std::string const &password) {
            std::lock_guard<std::mutex> _(allPasswordsMutex_);
            auto iter = tokenPasswords_.find(port);
            if (iter == tokenPasswords_.end()) {
                iter = tokenPasswords_.insert({port, TokenPasswordInfo{}}).first;
            }
            std::string hashed;
            hashed.resize(crypto_pwhash_STRBYTES);
            if (crypto_pwhash_str(
                hashed.data(), password.data(), password.length()
                , crypto_pwhash_OPSLIMIT_SENSITIVE, crypto_pwhash_MEMLIMIT_SENSITIVE
            ) != 0) {
                throw JsonRESTComponentException("Error hashing password for login '"+login+"' on port "+std::to_string(port));
            }
            iter->second.saltedPasswords[login] = hashed;
        }
        void addTokenAuthentication_salted(int port, std::string const &login, std::string const &saltedPassword) {
            std::lock_guard<std::mutex> _(allPasswordsMutex_);
            auto iter = tokenPasswords_.find(port);
            if (iter == tokenPasswords_.end()) {
                iter = tokenPasswords_.insert({port, TokenPasswordInfo{}}).first;
            }
            iter->second.saltedPasswords[login] = saltedPassword;
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
        static std::string base64URLEnc(std::string const &input) {
            std::string ret;
            ret.resize(boost::beast::detail::base64::encoded_size(input.length()));
            ret.resize(boost::beast::detail::base64::encode(ret.data(), reinterpret_cast<uint8_t const *>(input.data()), input.length()));
            if (ret.length() > 0) {
                boost::replace_all(ret, "+", "-");
                boost::replace_all(ret, "/", "_");
                int s = (int) ret.length()-1;
                for (int ii=0; ii<4 && s>=0; ++ii,--s) {
                    if (ret[s] != '=') {
                        break;
                    }
                }
                ++s;
                if (s != (int) ret.length()) {
                    ret = ret.substr(0, s);
                }
            }
            return ret;
        }
        static std::string base64URLDec(std::string const &input) {
            std::string inputCopy = input;
            if (inputCopy.length() > 0) {
                boost::replace_all(inputCopy, "-", "+");
                boost::replace_all(inputCopy, "_", "/");
                if (inputCopy.length()%4 != 0) {
                    inputCopy = inputCopy+std::string(4-inputCopy.length()%4, '=');
                }
            }
            std::string ret;
            ret.resize(boost::beast::detail::base64::decoded_size(inputCopy.length()));
            auto decodeRes = boost::beast::detail::base64::decode(
                ret.data(), inputCopy.data(), inputCopy.length()
            );
            ret.resize(decodeRes.first);
            return ret;
        }
        std::optional<std::string> createAuthToken_internal(
            int port
            , std::string const &authReqBody
        ) {
            try {
                nlohmann::json parsedBody = nlohmann::json::parse(authReqBody);
                if (parsedBody.is_null()) {
                    return std::nullopt;
                }

                std::string login;
                std::string password;
                if (parsedBody["request"].is_null()) {
                    if (!parsedBody["username"].is_string() || !parsedBody["password"].is_string()) {
                        return std::nullopt;
                    }
                    parsedBody["username"].get_to(login);
                    parsedBody["password"].get_to(password);
                } else {
                    auto const &part = parsedBody["request"];
                    if (!part["username"].is_string() || !part["password"].is_string()) {
                        return std::nullopt;
                    }
                    part["username"].get_to(login);
                    part["password"].get_to(password);
                }

                uint8_t secret[64];
                std::string saltedPassword;

                {
                    std::lock_guard<std::mutex> _(allPasswordsMutex_);
                    auto iter = tokenPasswords_.find(port);
                    if (iter == tokenPasswords_.end()) {
                        return std::nullopt;
                    }
                    auto innerIter = iter->second.saltedPasswords.find(login);
                    if (innerIter == iter->second.saltedPasswords.end()) {
                        return std::nullopt;
                    }
                    memcpy(secret, iter->second.secret, 64);
                    saltedPassword = innerIter->second;
                }

                if (crypto_pwhash_str_verify(
                    saltedPassword.c_str(), password.c_str(), password.length()
                ) != 0) {
                    return std::nullopt;
                }
                nlohmann::json respHead, respPayload;
                respHead["alg"] = "BLAKE2b";
                respHead["typ"] = "JWT";
                respPayload["login"] = login;
                respPayload["expiration"] = infra::withtime_utils::sinceEpoch<std::chrono::seconds>(std::chrono::system_clock::now())+3600;

                auto respHeadStr = base64URLEnc(respHead.dump());
                auto respPayloadStr = base64URLEnc(respPayload.dump());

                auto respFrontPart = respHeadStr+"."+respPayloadStr;

                uint8_t hash[32];
                if (crypto_generichash(
                    hash
                    , 32
                    , reinterpret_cast<const unsigned char *>(respFrontPart.data())
                    , respFrontPart.length()
                    , secret
                    , 64
                ) != 0) {
                    return std::nullopt;
                }
                char hashStr[65];
                for (int ii=0; ii<32; ++ii) {
                    sprintf(&hashStr[ii*2], "%02X", hash[ii]);
                }
                return respFrontPart+"."+hashStr;
            } catch (...) {
                return std::nullopt;
            }
        }
        void createAuthToken(
            int port
            , std::string const &authReqBody
            , unsigned version
            , bool keepAlive
            , std::function<void(
                boost::beast::http::response<boost::beast::http::string_body> *
                , std::string const &
            )> const &writer
        ) {
            auto token = createAuthToken_internal(port, authReqBody);
            if (!token) {
                auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::bad_request, version};
                res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                res->keep_alive(keepAlive);
                writer(res, "");
                return;
            } else {
                auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::ok, version};
                res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                res->set(boost::beast::http::field::content_type, "application/jwt");
                res->keep_alive(keepAlive);
                writer(res, *token);
            }
        }
        void scheduleCreateAuthToken(
            int port
            , std::string const &authReqBody
            , unsigned version
            , bool keepAlive
            , std::function<void(
                boost::beast::http::response<boost::beast::http::string_body> *
                , std::string const &
            )> const &writer
        ) {
            OneTokenThreadData *p = nullptr;
            {
                std::lock_guard<std::mutex> _(tokenThreadMutex_);
                auto iter = tokenThreads_.find(port);
                if (iter == tokenThreads_.end()) {
                    iter = tokenThreads_.insert({port, std::make_unique<OneTokenThreadData>(this, port)}).first;
                }
                p = iter->second.get();
            }
            p->addRequest(OneTokenReq {
                authReqBody 
                , version 
                , keepAlive
                , writer
            });
        }
        std::optional<std::string> checkTokenAuthentication(int port, std::string const &authToken) {
            try {
                uint8_t secret[64];
                {
                    std::lock_guard<std::mutex> _(allPasswordsMutex_);
                    
                    auto iter = tokenPasswords_.find(port);
                    if (iter == tokenPasswords_.end()) {
                        return std::nullopt;
                    }
                    memcpy(secret, iter->second.secret, 64);
                }
                
                std::vector<std::string> parts;
                boost::split(parts, authToken, boost::is_any_of("."));
                if (parts.size() != 3) {
                    return std::nullopt;
                }
                auto toHash = parts[0]+"."+parts[1];
                uint8_t hash[32];

                if (crypto_generichash(
                    hash
                    , 32
                    , reinterpret_cast<const unsigned char *>(toHash.data())
                    , toHash.length()
                    , secret
                    , 64
                ) != 0) {
                    return std::nullopt;
                }
                char hashStr[65];
                for (int ii=0; ii<32; ++ii) {
                    sprintf(&hashStr[ii*2], "%02X", hash[ii]);
                }
                //When comparing, do not exit early
                int diffs = 0;
                char *parts2Ptr = parts[2].data();
                std::size_t parts2Length = parts[2].length();
                for (std::size_t ii=0; ii<64; ++ii) {
                    if (parts2Length <= ii) {
                        diffs += 1;
                    } else {
                        diffs += ((parts2Ptr[ii]==hashStr[ii])?0:1);
                    }
                }
                if (diffs != 0) {
                    return std::nullopt;
                }

                auto payload = base64URLDec(parts[1]);
                nlohmann::json parsedPayload = nlohmann::json::parse(payload);
                if (parsedPayload.is_null()) {
                    return std::nullopt;
                }
                if (!parsedPayload["login"].is_string() || !parsedPayload["expiration"].is_number()) {
                    return std::nullopt;
                }
                int64_t expiration;
                parsedPayload["expiration"].get_to(expiration);
                if (expiration < infra::withtime_utils::sinceEpoch<std::chrono::seconds>(std::chrono::system_clock::now())) {
                    return std::nullopt;
                }
                std::string login;
                parsedPayload["login"].get_to(login);
                return login;
            } catch (...) {
                return std::nullopt;
            }
        }
        bool requiresTokenAuthentication(int port) {
            std::lock_guard<std::mutex> _(allPasswordsMutex_);
            return (tokenPasswords_.find(port) != tokenPasswords_.end());
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
    void JsonRESTComponent::addJsonRESTClient(ConnectionLocator const &locator, std::string &&urlQueryPart, std::string &&request, std::function<
        void(unsigned, std::string &&, std::unordered_map<std::string,std::string> &&)
    > const &clientCallback, std::string const &contentType, std::optional<std::string> const &method) {
        impl_->addJsonRESTClient(locator, std::move(urlQueryPart), std::move(request), clientCallback, contentType, method, dynamic_cast<TLSClientConfigurationComponent const *>(this), dynamic_cast<basic::LoggingComponentBase *>(this));
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
    void JsonRESTComponent::addTokenAuthentication(int port, std::string const &login, std::string const &password) {
        impl_->addTokenAuthentication(port, login, password);
    }
    void JsonRESTComponent::addTokenAuthentication_salted(int port, std::string const &login, std::string const &saltedPassword) {
        impl_->addTokenAuthentication_salted(port, login, saltedPassword);
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
    const std::string JsonRESTComponent::TOKEN_AUTHENTICATION_REQUEST = "/__API_AUTHENTICATION";

} } } } }
