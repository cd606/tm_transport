#include <tm_kit/transport/json_rest/JsonRESTComponent.hpp>
#include <tm_kit/transport/TLSConfigurationComponent.hpp>

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

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace json_rest {

    class JsonRESTComponentImpl {
    private:
        using HandlerFunc = std::function<
            void(std::string const &, std::function<void(std::string const &)> const &)
        >;
        std::unordered_map<int, std::unordered_map<std::string, HandlerFunc>> handlerMap_;
        std::mutex handlerMapMutex_;

        class Acceptor : public std::enable_shared_from_this<Acceptor> {
        private:
            JsonRESTComponentImpl *parent_;
            boost::asio::io_context svc_;
            std::optional<boost::asio::ssl::context> sslCtx_;
            int port_;
            std::unordered_map<std::string, HandlerFunc> const *handlers_;
            std::thread th_;
            std::atomic<bool> running_;

            boost::asio::ip::tcp::acceptor acceptor_;

            class OneHandler : public std::enable_shared_from_this<OneHandler> {
            private:
                std::unordered_map<std::string, HandlerFunc> const *handlers_;
                std::variant<
                    std::monostate
                    , boost::beast::tcp_stream
                    , boost::beast::ssl_stream<boost::beast::tcp_stream>
                > stream_;
                boost::beast::flat_buffer buffer_;
                boost::beast::http::request<boost::beast::http::string_body> req_;
            public:
                OneHandler(
                    boost::asio::ip::tcp::socket &&socket
                    , std::unordered_map<std::string, HandlerFunc> const *handlers
                    , std::optional<boost::asio::ssl::context> &sslCtx
                )
                    : handlers_(handlers)
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
                        boost::asio::dispatch(
                            std::get<2>(stream_).get_executor()
                            , boost::beast::bind_front_handler(
                                &OneHandler::doRead
                                , shared_from_this()
                            )
                        );
                    }
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
                void onRead(boost::beast::error_code ec, std::size_t bytes_transferred) {
                    if (ec) {
                        doClose(ec);
                        return;
                    }
                    auto path = req_.target();
                    std::string pathStr {path.data(), path.size()};
                    auto iter = handlers_->find(pathStr);
                    if (iter == handlers_->end()) {
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
                        auto handler = iter->second;
                        auto *res = new boost::beast::http::response<boost::beast::http::string_body> {boost::beast::http::status::ok, req_.version()};
                        res->set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                        res->set(boost::beast::http::field::content_type, "application/json");
                        res->keep_alive(req_.keep_alive());
                        handler(
                            req_.body()
                            , [res,x=shared_from_this()](std::string const &resp) {
                                x->writeResp(res, resp);
                            }
                        );
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
                , std::unordered_map<std::string, HandlerFunc> const *handlers
                , std::optional<TLSServerInfo> const &sslInfo
            )
                : parent_(parent)
                , svc_()
                , sslCtx_(
                    sslInfo
                    ? std::optional<boost::asio::ssl::context>(boost::asio::ssl::context {boost::asio::ssl::context::tlsv12})
                    : std::nullopt
                )
                , port_(port)
                , handlers_(handlers)
                , th_()
                , running_(true)
                , acceptor_(svc_)
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
                svc_.stop();
                th_.join();
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
                    std::make_shared<OneHandler>(std::move(socket), handlers_, sslCtx_)->run();
                    acceptor_.async_accept(
                        boost::asio::make_strand(svc_)
                        , boost::beast::bind_front_handler(
                            &Acceptor::onAccept
                            , shared_from_this()
                        )
                    );
                } else {
                    parent_->removeAcceptor(port_);
                }
            }
        };

        std::unordered_map<int, std::shared_ptr<Acceptor>> acceptorMap_;
        std::mutex acceptorMapMutex_;
    public:
        JsonRESTComponentImpl() : handlerMap_(), handlerMapMutex_(), acceptorMap_(), acceptorMapMutex_() {
        }
        ~JsonRESTComponentImpl() {
        }
        void registerHandler(ConnectionLocator const &locator, HandlerFunc const &handler) {
            std::lock_guard<std::mutex> _(handlerMapMutex_);
            auto iter = handlerMap_.find(locator.port());
            if (iter == handlerMap_.end()) {
                iter = handlerMap_.insert({locator.port(), {}}).first;
            }
            std::string path = locator.identifier();
            if (!boost::starts_with(path, "/")) {
                path = std::string("/")+path;
            }
            auto innerIter = iter->second.find(path);
            if (innerIter == iter->second.end()) {
                iter->second.insert({path, handler});
            }
        }
        void finalizeEnvironment(TLSServerConfigurationComponent const *tlsConfig) {
            std::lock_guard<std::mutex> _(handlerMapMutex_);
            for (auto const &item : handlerMap_) {
                auto sslInfo = (tlsConfig?(tlsConfig->getConfigurationItem(
                    TLSServerInfoKey {item.first}
                )):std::nullopt);
                std::lock_guard<std::mutex> _(acceptorMapMutex_);
                auto iter = acceptorMap_.insert({item.first, std::make_shared<Acceptor>(
                    this
                    , item.first 
                    , &(item.second) 
                    , sslInfo
                )}).first;
                iter->second->run();
            }
        }
        void removeAcceptor(int port) {
            std::lock_guard<std::mutex> _(acceptorMapMutex_);
            acceptorMap_.erase(port);
        }
    };

    JsonRESTComponent::JsonRESTComponent() : impl_(std::make_unique<JsonRESTComponentImpl>()) {}
    JsonRESTComponent::JsonRESTComponent(JsonRESTComponent &&) = default;
    JsonRESTComponent &JsonRESTComponent::operator=(JsonRESTComponent &&) = default;
    JsonRESTComponent::~JsonRESTComponent() = default;
    void JsonRESTComponent::registerHandler(ConnectionLocator const &locator, std::function<
        void(std::string const &, std::function<void(std::string const &)> const &)
    > const &handler) {
        impl_->registerHandler(locator, handler);
    }
    void JsonRESTComponent::finalizeEnvironment() {
        impl_->finalizeEnvironment(dynamic_cast<TLSServerConfigurationComponent const *>(this));
    }

} } } } }