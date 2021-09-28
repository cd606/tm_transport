#include <tm_kit/transport/websocket/WebSocketComponent.hpp>
#include <tm_kit/transport/TLSConfigurationComponent.hpp>

#include <tm_kit/basic/ByteData.hpp>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/socket_base.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/config.hpp>
#include <boost/algorithm/string.hpp>

#include <unordered_map>
#include <thread>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sstream>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace web_socket {

    class WebSocketComponentImpl {
    private:
        class OnePublisher : public std::enable_shared_from_this<OnePublisher> {
        private:
            class OneClientHandler : public std::enable_shared_from_this<OneClientHandler> {
            private:
                OnePublisher *parent_;
                std::variant<
                    std::monostate
                    , boost::beast::websocket::stream<boost::beast::tcp_stream>
                    , boost::beast::websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>>
                > stream_;
                boost::beast::flat_buffer buffer_;
                std::atomic<bool> good_;
            public:
                OneClientHandler(
                    OnePublisher *parent
                    , boost::asio::ip::tcp::socket &&socket
                    , std::optional<boost::asio::ssl::context> &sslCtx
                ) 
                    : parent_(parent)
                    , stream_()
                    , buffer_()
                    , good_(false)
                {
                    if (sslCtx) {
                        stream_.emplace<2>(std::move(socket), *sslCtx);
                    } else {
                        stream_.emplace<1>(std::move(socket));
                    }
                }
                ~OneClientHandler() {
                    good_ = false;
                }

                void run() {
                    if (stream_.index() == 1) {
                        boost::asio::dispatch(
                            std::get<1>(stream_).get_executor()
                            , boost::beast::bind_front_handler(
                                &OneClientHandler::onRun
                                , shared_from_this()
                            )
                        );
                    } else {
                        boost::asio::dispatch(
                            std::get<2>(stream_).get_executor()
                            , boost::beast::bind_front_handler(
                                &OneClientHandler::onRun
                                , shared_from_this()
                            )
                        );
                    }
                }

                void onRun() {
                    if (stream_.index() == 1) {
                        std::get<1>(stream_).set_option(
                            boost::beast::websocket::stream_base::timeout::suggested(
                                boost::beast::role_type::server
                            )
                        );
                        std::get<1>(stream_).set_option(
                            boost::beast::websocket::stream_base::decorator(
                                [](boost::beast::websocket::response_type &res) {
                                    res.set(
                                        boost::beast::http::field::server
                                        , BOOST_BEAST_VERSION_STRING
                                    );
                                }
                            )
                        );
                        std::get<1>(stream_).async_accept(
                            boost::beast::bind_front_handler(
                                &OneClientHandler::onWsHandshake 
                                , shared_from_this()
                            )
                        );
                    } else {
                        boost::beast::get_lowest_layer(std::get<2>(stream_)).expires_after(std::chrono::seconds(30));
                        std::get<2>(stream_).next_layer().async_handshake(
                            boost::asio::ssl::stream_base::server 
                            , boost::beast::bind_front_handler(
                                &OneClientHandler::onSslHandshake
                                , shared_from_this()
                            )
                        );
                    }
                }

                void onSslHandshake(boost::system::error_code ec) {
                    if (ec) {
                        parent_->removeClientHandler(shared_from_this());
                    } else {
                        boost::beast::get_lowest_layer(std::get<2>(stream_)).expires_never();
                        std::get<2>(stream_).set_option(
                            boost::beast::websocket::stream_base::timeout::suggested(
                                boost::beast::role_type::server
                            )
                        );
                        std::get<2>(stream_).set_option(
                            boost::beast::websocket::stream_base::decorator(
                                [](boost::beast::websocket::response_type &res) {
                                    res.set(
                                        boost::beast::http::field::server
                                        , BOOST_BEAST_VERSION_STRING
                                    );
                                }
                            )
                        );
                        std::get<2>(stream_).async_accept(
                            boost::beast::bind_front_handler(
                                &OneClientHandler::onWsHandshake 
                                , shared_from_this()
                            )
                        );
                    }
                }
                void onWsHandshake(boost::system::error_code ec) {
                    if (ec) {
                        parent_->removeClientHandler(shared_from_this());
                    } else {
                        good_ = true;
                        if (stream_.index() == 1) {
                            std::get<1>(stream_).async_read(
                                buffer_
                                , boost::beast::bind_front_handler(
                                    &OneClientHandler::onRead
                                    , shared_from_this()
                                )
                            );
                        } else {
                            std::get<2>(stream_).async_read(
                                buffer_
                                , boost::beast::bind_front_handler(
                                    &OneClientHandler::onRead
                                    , shared_from_this()
                                )
                            );
                        }
                    }
                }
                void onRead(boost::beast::error_code ec, std::size_t bytes_transferred) {
                    if (ec) {
                        good_ = false;
                        parent_->removeClientHandler(shared_from_this());
                    } else {
                        if (stream_.index() == 1) {
                            std::get<1>(stream_).async_read(
                                buffer_
                                , boost::beast::bind_front_handler(
                                    &OneClientHandler::onRead
                                    , shared_from_this()
                                )
                            );
                        } else {
                            std::get<2>(stream_).async_read(
                                buffer_
                                , boost::beast::bind_front_handler(
                                    &OneClientHandler::onRead
                                    , shared_from_this()
                                )
                            );
                        }
                    }
                }
                void doPublish(std::string_view const &data) {
                    if (!good_) {
                        return;
                    }
                    if (stream_.index() == 1) {
                        std::get<1>(stream_).async_write(
                            boost::asio::buffer(reinterpret_cast<const char *>(data.data()), data.size())
                            , boost::beast::bind_front_handler(
                                &OneClientHandler::onWrite
                                , shared_from_this()
                            )
                        );
                    } else {
                        std::get<2>(stream_).async_write(
                            boost::asio::buffer(reinterpret_cast<const char *>(data.data()), data.size())
                            , boost::beast::bind_front_handler(
                                &OneClientHandler::onWrite
                                , shared_from_this()
                            )
                        );
                    }
                }
                void onWrite(boost::beast::error_code ec, std::size_t bytes_transferred) {
                    if (ec) {
                        good_ = false;
                        parent_->removeClientHandler(shared_from_this());
                    }
                }
            };

            WebSocketComponentImpl *parent_;
            int port_;
            bool ignoreTopic_;
            boost::asio::io_context svc_;
            std::optional<boost::asio::ssl::context> sslCtx_;
            std::thread th_;
            std::atomic<bool> running_;
            boost::asio::ip::tcp::acceptor acceptor_;

            std::unordered_set<std::shared_ptr<OneClientHandler>> clientHandlers_;
            std::mutex clientHandlersMutex_;
        
            void doPublish(std::string_view const &data) {
                std::lock_guard<std::mutex> _(clientHandlersMutex_);
                for (auto const &h : clientHandlers_) {
                    h->doPublish(data);
                }
            }
        public:
            OnePublisher(WebSocketComponentImpl *parent, int port, bool ignoreTopic, std::optional<TLSServerInfo> const &sslInfo) 
                : parent_(parent), port_(port), ignoreTopic_(ignoreTopic)
                , svc_()
                , sslCtx_(
                    sslInfo
                    ? std::optional<boost::asio::ssl::context>(boost::asio::ssl::context {boost::asio::ssl::context::tlsv12})
                    : std::nullopt
                )
                , th_(), running_(false)
                , acceptor_(svc_)
                , clientHandlers_(), clientHandlersMutex_()
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
                    throw WebSocketComponentException("Cannot open acceptor on port "+std::to_string(port_));
                }
                acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
                if (ec) {
                    throw WebSocketComponentException("Cannot set reuse_addr on port "+std::to_string(port_));
                }
                acceptor_.bind(ep, ec);
                if (ec) {
                    throw WebSocketComponentException("Cannot bind to port "+std::to_string(port_));
                }
                acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
                if (ec) {
                    throw WebSocketComponentException("Cannot listen on port "+std::to_string(port_));
                }
            }
            ~OnePublisher() {
                if (running_) {
                    running_ = false;
                    svc_.stop();
                    th_.join();
                }
            }
            void publish(basic::ByteDataWithTopic &&data) {
                if (!running_) {
                    return;
                }
                if (ignoreTopic_) {
                    doPublish(data.content);
                } else {
                    auto s = basic::bytedata_utils::RunCBORSerializer<basic::ByteDataWithTopic>::apply(data);
                    doPublish(s);
                }
            }
            void run() {
                running_ = true;
                th_ = std::thread([this]() {                   
                    boost::asio::io_context::work work(svc_);
                    svc_.run();
                });
                th_.detach();

                acceptor_.async_accept(boost::asio::make_strand(svc_)
                    , boost::beast::bind_front_handler(
                        &OnePublisher::onAccept
                        , shared_from_this()
                    )
                );
            }
            void onAccept(boost::beast::error_code ec, boost::asio::ip::tcp::socket socket) {
                if (!ec) {
                    auto h = std::make_shared<OneClientHandler>(this, std::move(socket), sslCtx_);
                    {
                        std::lock_guard<std::mutex> _(clientHandlersMutex_);
                        clientHandlers_.insert(h);
                    }
                    h->run();
                    acceptor_.async_accept(boost::asio::make_strand(svc_)
                        , boost::beast::bind_front_handler(
                            &OnePublisher::onAccept
                            , shared_from_this()
                        )
                    );
                } else {
                    parent_->removePublisher(port_);
                }
            }
            void removeClientHandler(std::shared_ptr<OneClientHandler> const &toBeRemoved) {
                std::lock_guard<std::mutex> _(clientHandlersMutex_);
                clientHandlers_.erase(toBeRemoved);
            }
        };
        std::unordered_map<int, std::shared_ptr<OnePublisher>> publisherMap_;
        mutable std::mutex publisherMapMutex_;
        std::atomic<bool> started_;
    public:
        WebSocketComponentImpl() 
            : publisherMap_(), publisherMapMutex_(), started_(false)
        {

        }
        ~WebSocketComponentImpl() {
        }
        std::function<void(basic::ByteDataWithTopic &&)> getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook, TLSServerConfigurationComponent *config) {
            std::lock_guard<std::mutex> _(publisherMapMutex_);
            auto iter = publisherMap_.find(locator.port());
            if (iter == publisherMap_.end()) {
                iter = publisherMap_.insert({
                    locator.port()
                    , std::make_shared<OnePublisher>(
                        this
                        , locator.port()
                        , (locator.query("ignoreTopic","false")=="true")
                        , (config?config->getConfigurationItem(TLSServerInfoKey {locator.port()}):std::nullopt)
                    )
                }).first;
                if (started_) {
                    iter->second->run();
                }
            }
            auto *p = iter->second.get();
            if (userToWireHook) {
                auto hook = userToWireHook->hook;
                return [p,hook](basic::ByteDataWithTopic &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    p->publish({std::move(data.topic), std::move(x.content)});
                };
            } else {
                return [p](basic::ByteDataWithTopic &&data) {
                    p->publish(std::move(data));
                };
            }
        }
        void finalizeEnvironment() {
            std::lock_guard<std::mutex> _(publisherMapMutex_);
            for (auto const &item : publisherMap_) {
                item.second->run();
            }
            started_ = true;
        }

        void removePublisher(int port) {
            std::lock_guard<std::mutex> _(publisherMapMutex_);
            publisherMap_.erase(port);
        }
    };

    WebSocketComponent::WebSocketComponent() : impl_(std::make_unique<WebSocketComponentImpl>()) {}
    WebSocketComponent::WebSocketComponent(WebSocketComponent &&) = default;
    WebSocketComponent &WebSocketComponent::operator=(WebSocketComponent &&) = default;
    WebSocketComponent::~WebSocketComponent() = default;
    std::function<void(basic::ByteDataWithTopic &&)> WebSocketComponent::websocket_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook, dynamic_cast<TLSServerConfigurationComponent *>(this));
    }
    void WebSocketComponent::finalizeEnvironment() {
        impl_->finalizeEnvironment();
    }

} } } } }