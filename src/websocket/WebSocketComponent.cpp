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
                boost::beast::http::request<boost::beast::http::string_body> initialReq_;
                std::string targetPath_;
                std::atomic<bool> good_;
                std::mutex writeMutex_;
            public:
                OneClientHandler(
                    OnePublisher *parent
                    , boost::asio::ip::tcp::socket &&socket
                    , std::optional<boost::asio::ssl::context> &sslCtx
                    , bool *needToRun
                ) 
                    : parent_(parent)
                    , stream_()
                    , buffer_()
                    , initialReq_()
                    , targetPath_()
                    , good_(false)
                    , writeMutex_()
                {
                    if (sslCtx) {
                        stream_.emplace<2>(std::move(socket), *sslCtx);
                        std::get<2>(stream_).text(false);
                        *needToRun = true;
                    } else {
                        boost::beast::http::read(socket, buffer_, initialReq_);
                        if (boost::beast::websocket::is_upgrade(initialReq_)) {
                            auto t = initialReq_.target();
                            targetPath_ = std::string {t.data(), t.length()};
                            stream_.emplace<1>(std::move(socket));
                            std::get<1>(stream_).text(false);
                            *needToRun = true;
                        } else {
                            *needToRun = false;
                        }
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
                    buffer_.clear();
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
                            initialReq_
                            , boost::beast::bind_front_handler(
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
                        boost::beast::http::read(std::get<2>(stream_).next_layer(), buffer_, initialReq_);
                        if (boost::beast::websocket::is_upgrade(initialReq_)) {
                            auto t = initialReq_.target();
                            targetPath_ = std::string {t.data(), t.length()};
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
                                initialReq_
                                , boost::beast::bind_front_handler(
                                    &OneClientHandler::onWsHandshake 
                                    , shared_from_this()
                                )
                            );
                        } else {
                            parent_->removeClientHandler(shared_from_this());
                        }
                    }
                }
                void onWsHandshake(boost::system::error_code ec) {
                    if (ec) {
                        parent_->removeClientHandler(shared_from_this());
                    } else {
                        good_ = true;
                        parent_->registerClientHandlerForPath(this);
                        buffer_.clear();
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
                        buffer_.clear(); 
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
                    try {
                        std::lock_guard<std::mutex> _(writeMutex_);
                        if (stream_.index() == 1) {
                            std::get<1>(stream_).write(
                                boost::asio::buffer(reinterpret_cast<const char *>(data.data()), data.size())
                            );
                        } else {
                            std::get<2>(stream_).write(
                                boost::asio::buffer(reinterpret_cast<const char *>(data.data()), data.size())
                            );
                        }
                    } catch (...) {
                        good_ = false;
                        parent_->removeClientHandler(shared_from_this());
                    }
                }
                void onWrite(boost::beast::error_code ec, std::size_t bytes_transferred) {
                    if (ec) {
                        good_ = false;
                        parent_->removeClientHandler(shared_from_this());
                    }
                }
                std::string const &targetPath() const {
                    return targetPath_;
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
            std::unordered_map<std::string, std::unordered_set<OneClientHandler *>> pathToClientMap_;
            std::mutex clientHandlersMutex_;
        
            void doPublish(std::unordered_set<OneClientHandler *> &handlers, std::string_view const &data) {
                for (auto *h : handlers) {
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
                , clientHandlers_(), pathToClientMap_(), clientHandlersMutex_()
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
            void publish(std::string const &path, basic::ByteDataWithTopic &&data) {
                if (!running_) {
                    return;
                }
                std::lock_guard<std::mutex> _(clientHandlersMutex_);
                auto iter = pathToClientMap_.find(path);
                if (iter == pathToClientMap_.end()) {
                    return;
                }
                if (iter->second.empty()) {
                    return;
                }
                if (ignoreTopic_) {
                    doPublish(iter->second, data.content);
                } else {
                    auto s = basic::bytedata_utils::RunCBORSerializer<basic::ByteDataWithTopic>::apply(data);
                    doPublish(iter->second, s);
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
                    bool needToRun;
                    auto h = std::make_shared<OneClientHandler>(this, std::move(socket), sslCtx_, &needToRun);
                    if (needToRun) {
                        {
                            std::lock_guard<std::mutex> _(clientHandlersMutex_);
                            clientHandlers_.insert(h);
                        }
                        h->run();
                    } else {
                        h.reset();
                    }
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
                auto *p = toBeRemoved.get();
                pathToClientMap_[p->targetPath()].erase(p);
                clientHandlers_.erase(toBeRemoved);
            }
            void registerClientHandlerForPath(OneClientHandler *p) {
                std::lock_guard<std::mutex> _(clientHandlersMutex_);
                pathToClientMap_[p->targetPath()].insert(p);
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };
        std::unordered_map<int, std::shared_ptr<OnePublisher>> publisherMap_;
        mutable std::mutex publisherMapMutex_;

        class OneRPCClient : public std::enable_shared_from_this<OneRPCClient> {
        private:
            WebSocketComponentImpl *parent_;
            ConnectionLocator locator_;
            std::string handshakeHost_;
            boost::asio::io_context svc_;
            std::optional<boost::asio::ssl::context> sslCtx_;
            struct OneClientInfo {
                std::function<void(bool, basic::ByteDataWithID &&)> callback_;
                std::optional<WireToUserHook> wireToUserHook_;
            };
            uint32_t clientCounter_;
            std::unordered_map<uint32_t, OneClientInfo> clients_;
            std::unordered_map<uint32_t, std::unordered_set<std::string>> clientToIDMap_;
            std::unordered_map<std::string, uint32_t> idToClientMap_;
            std::mutex clientsMutex_;
            std::thread th_;
            std::atomic<bool> running_, ready_;
            boost::asio::ip::tcp::resolver resolver_;
            std::variant<
                std::monostate
                , boost::beast::websocket::stream<boost::beast::tcp_stream>
                , boost::beast::websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>>
            > stream_;
            boost::beast::flat_buffer buffer_;
            bool initializationFailure_;
            std::mutex writeMutex_;
            std::list<std::tuple<uint32_t,basic::ByteDataWithID>> beforeReadyBuffer_;
        public:
            OneRPCClient(
                WebSocketComponentImpl *parent
                , ConnectionLocator const &locator
                , std::optional<TLSClientInfo> const &sslInfo
            )
                : parent_(parent), locator_(locator), svc_()
                , sslCtx_(
                    sslInfo
                    ? std::optional<boost::asio::ssl::context>(boost::asio::ssl::context {boost::asio::ssl::context::tlsv12_client})
                    : std::nullopt
                )
                , clientCounter_(0), clients_(), clientToIDMap_(), idToClientMap_(), clientsMutex_()
                , th_(), running_(false), ready_(false)
                , resolver_(boost::asio::make_strand(svc_))
                , stream_()
                , buffer_()
                , initializationFailure_(false)
                , writeMutex_()
                , beforeReadyBuffer_()
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
                        initializationFailure_ = true;
                        return;
                    }
                    stream_.emplace<2>(boost::asio::make_strand(svc_), *sslCtx_);
                    std::get<2>(stream_).binary(true);
                } else {
                    stream_.emplace<1>(boost::asio::make_strand(svc_));
                    std::get<1>(stream_).binary(true);
                }
            }
            ~OneRPCClient() {
                if (stream_.index() == 1) {
                    std::get<1>(stream_).close(boost::beast::websocket::close_code::normal);
                } else if (stream_.index() == 2) {
                    std::get<2>(stream_).close(boost::beast::websocket::close_code::normal);
                }
                if (running_) {
                    running_ = false;
                    svc_.stop();
                    th_.join();
                }
            }
            void run() {
                running_ = true;
                th_ = std::thread([this]() {                   
                    boost::asio::io_context::work work(svc_);
                    svc_.run();
                });
                th_.detach();
                resolver_.async_resolve(
                    locator_.host()
                    , std::to_string(locator_.port())
                    , boost::beast::bind_front_handler(
                        &OneRPCClient::onResolve
                        , shared_from_this()
                    )
                );
            }
            void onResolve(boost::beast::error_code ec, boost::asio::ip::tcp::resolver::results_type results) {
                if (ec) {
                    parent_->removeRPCClient(locator_);
                    return;
                }
                if (stream_.index() == 1) {
                    boost::beast::get_lowest_layer(std::get<1>(stream_)).expires_after(std::chrono::seconds(30));
                    boost::beast::get_lowest_layer(std::get<1>(stream_)).async_connect(
                        results,
                        boost::beast::bind_front_handler(
                            &OneRPCClient::onConnect,
                            shared_from_this()
                        )
                    );
                } else {
                    boost::beast::get_lowest_layer(std::get<2>(stream_)).expires_after(std::chrono::seconds(30));
                    boost::beast::get_lowest_layer(std::get<2>(stream_)).async_connect(
                        results,
                        boost::beast::bind_front_handler(
                            &OneRPCClient::onConnect,
                            shared_from_this()
                        )
                    );
                }
            }
            void onConnect(boost::beast::error_code ec, boost::asio::ip::tcp::resolver::results_type::endpoint_type ep) {
                if (ec) {
                    parent_->removeRPCClient(locator_);
                    return;
                }
                handshakeHost_ = locator_.host()+":"+std::to_string(ep.port());
                if (stream_.index() == 1) {
                    boost::beast::get_lowest_layer(std::get<1>(stream_)).expires_never();
                    std::get<1>(stream_).set_option(
                        boost::beast::websocket::stream_base::timeout::suggested(
                            boost::beast::role_type::client
                        )
                    );
                    std::get<1>(stream_).set_option(
                        boost::beast::websocket::stream_base::decorator(
                            [](boost::beast::websocket::request_type& req)
                            {
                                req.set(
                                    boost::beast::http::field::user_agent
                                    , std::string(BOOST_BEAST_VERSION_STRING)
                                );
                            }
                        )
                    );

                    std::string targetPath = locator_.identifier();
                    if (!boost::starts_with(targetPath, "/")) {
                        targetPath = "/"+targetPath;
                    }
                    std::get<1>(stream_).async_handshake(
                        handshakeHost_
                        , targetPath
                        , boost::beast::bind_front_handler(
                            &OneRPCClient::onWebSocketHandshake
                            ,shared_from_this()
                        )
                    );
                } else {
                    boost::beast::get_lowest_layer(std::get<2>(stream_)).expires_after(std::chrono::seconds(30));
                    if(!SSL_set_tlsext_host_name(
                        std::get<2>(stream_).next_layer().native_handle(),
                        handshakeHost_.c_str()
                    ))
                    {
                        parent_->removeRPCClient(locator_);
                        return;
                    }
                    std::get<2>(stream_).next_layer().async_handshake(
                        boost::asio::ssl::stream_base::client
                        , boost::beast::bind_front_handler(
                            &OneRPCClient::onSSLHandshake,
                            shared_from_this()
                        )
                    );
                }
            }
            void onSSLHandshake(boost::beast::error_code ec) {
                if (ec) {
                    parent_->removeRPCClient(locator_);
                    return;
                }
                boost::beast::get_lowest_layer(std::get<2>(stream_)).expires_never();
                std::get<2>(stream_).set_option(
                    boost::beast::websocket::stream_base::timeout::suggested(
                        boost::beast::role_type::client
                    )
                );
                std::get<2>(stream_).set_option(
                    boost::beast::websocket::stream_base::decorator(
                        [](boost::beast::websocket::request_type& req)
                        {
                            req.set(
                                boost::beast::http::field::user_agent
                                , std::string(BOOST_BEAST_VERSION_STRING)
                            );
                        }
                    )
                );

                std::string targetPath = locator_.identifier();
                if (!boost::starts_with(targetPath, "/")) {
                    targetPath = "/"+targetPath;
                }
                std::get<2>(stream_).async_handshake(
                    handshakeHost_
                    , targetPath
                    , boost::beast::bind_front_handler(
                        &OneRPCClient::onWebSocketHandshake
                        ,shared_from_this()
                    )
                );
            }
            void onWebSocketHandshake(boost::beast::error_code ec) {
                if (ec) {
                    parent_->removeRPCClient(locator_);
                    return;
                }
                if (stream_.index() == 1) {
                    std::get<1>(stream_).async_read(
                        buffer_ 
                        , boost::beast::bind_front_handler(
                            &OneRPCClient::onRead 
                            , shared_from_this()
                        )
                    );
                } else {
                    std::get<2>(stream_).async_read(
                        buffer_ 
                        , boost::beast::bind_front_handler(
                            &OneRPCClient::onRead 
                            , shared_from_this()
                        )
                    );
                }
                ready_ = true;
                std::list<std::tuple<uint32_t,basic::ByteDataWithID>> bufferCopy;
                {
                    std::lock_guard<std::mutex> _(writeMutex_);
                    bufferCopy.splice(bufferCopy.end(), beforeReadyBuffer_);
                }
                for (auto &x : bufferCopy) {
                    doSendRequest(std::get<0>(x), std::move(std::get<1>(x)));
                }
            }
            void onRead(boost::beast::error_code ec, std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);
                if (ec) {
                    ready_ = false;
                    parent_->removeRPCClient(locator_);
                    return;
                }
                auto input = boost::beast::buffers_to_string(buffer_.data());
                auto parseRes = basic::bytedata_utils::RunCBORDeserializer<std::tuple<bool,basic::ByteDataWithID>>::apply(std::string_view {input}, 0);
                if (parseRes) {
                    std::lock_guard<std::mutex> _(clientsMutex_);
                    std::string theID = std::get<1>(std::get<0>(*parseRes)).id;
                    auto iter = idToClientMap_.find(theID);
                    if (iter != idToClientMap_.end()) {
                        auto clientIter = clients_.find(iter->second);
                        if (clientIter != clients_.end()) {
                            if (clientIter->second.wireToUserHook_) {
                                auto d = (clientIter->second.wireToUserHook_->hook)(basic::ByteDataView {std::string_view(std::get<1>(std::get<0>(*parseRes)).content)});
                                if (d) {
                                    clientIter->second.callback_(std::get<0>(std::get<0>(*parseRes)), {std::move(std::get<1>(std::get<0>(*parseRes)).id), std::move(d->content)});
                                }
                            } else {
                                clientIter->second.callback_(std::get<0>(std::get<0>(*parseRes)), std::move(std::get<1>(std::get<0>(*parseRes))));
                            }
                        }
                        if (std::get<0>(std::get<0>(*parseRes))) {
                            clientToIDMap_[iter->second].erase(theID);
                            idToClientMap_.erase(iter);
                        }
                    }
                }
                
                buffer_.clear();
                if (stream_.index() == 1) {
                    std::get<1>(stream_).async_read(
                        buffer_ 
                        , boost::beast::bind_front_handler(
                            &OneRPCClient::onRead 
                            , shared_from_this()
                        )
                    );
                } else {
                    std::get<2>(stream_).async_read(
                        buffer_ 
                        , boost::beast::bind_front_handler(
                            &OneRPCClient::onRead 
                            , shared_from_this()
                        )
                    );
                }
            }
            bool initializationFailure() const {
                return initializationFailure_;
            }
            uint32_t addClient(std::function<void(bool, basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook) {
                std::lock_guard<std::mutex> _(clientsMutex_);
                clients_[++clientCounter_] = {callback, wireToUserHook};
                return clientCounter_;
            }
            std::size_t removeClient(uint32_t clientNumber) {
                std::lock_guard<std::mutex> _(clientsMutex_);
                auto iter = clientToIDMap_.find(clientNumber);
                if (iter != clientToIDMap_.end()) {
                    for (auto const &id : iter->second) {
                        idToClientMap_.erase(id);
                    }
                    clientToIDMap_.erase(iter);
                }
                clients_.erase(clientNumber);
                return clients_.size();
            }
            void sendRequest(uint32_t clientNumber, basic::ByteDataWithID &&data) {
                if (!running_) {
                    return;
                }
                if (!ready_) {
                    std::lock_guard<std::mutex> _(writeMutex_);
                    beforeReadyBuffer_.push_back({clientNumber, std::move(data)});
                    return;
                }
                doSendRequest(clientNumber, std::move(data));
            }
            void doSendRequest(uint32_t clientNumber, basic::ByteDataWithID &&data) {
                if (!ready_) {
                    return;
                }
                {
                    std::lock_guard<std::mutex> _(clientsMutex_);
                    clientToIDMap_[clientNumber].insert(data.id);
                    idToClientMap_[data.id] = clientNumber;
                }
                auto encodedData = basic::bytedata_utils::RunSerializer<basic::CBOR<basic::ByteDataWithID>>::apply({std::move(data)});
                try {
                    std::lock_guard<std::mutex> _(writeMutex_);
                    if (stream_.index() == 1) {
                        std::get<1>(stream_).write(
                            boost::asio::buffer(
                                reinterpret_cast<const char *>(encodedData.data()), encodedData.length()
                            )
                        );
                    } else {
                        std::get<2>(stream_).write(
                            boost::asio::buffer(
                                reinterpret_cast<const char *>(encodedData.data()), encodedData.length()
                            )
                        );
                    }
                } catch (...) {
                    ready_ = false;
                    parent_->removeRPCClient(locator_);
                }
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };
        std::unordered_map<ConnectionLocator, std::shared_ptr<OneRPCClient>> rpcClientMap_;
        mutable std::mutex rpcClientMapMutex_;

        class OneRPCServer : public std::enable_shared_from_this<OneRPCServer> {
        private:
            class OneClientHandler : public std::enable_shared_from_this<OneClientHandler> {
            private:
                OneRPCServer *parent_;
                std::variant<
                    std::monostate
                    , boost::beast::websocket::stream<boost::beast::tcp_stream>
                    , boost::beast::websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>>
                > stream_;
                boost::beast::flat_buffer buffer_;
                boost::beast::http::request<boost::beast::http::string_body> initialReq_;
                std::string targetPath_;
                std::atomic<bool> good_;
                std::mutex writeMutex_;
            public:
                OneClientHandler(
                    OneRPCServer *parent
                    , boost::asio::ip::tcp::socket &&socket
                    , std::optional<boost::asio::ssl::context> &sslCtx
                    , bool *needToRun
                ) 
                    : parent_(parent)
                    , stream_()
                    , buffer_()
                    , initialReq_()
                    , targetPath_()
                    , good_(false)
                    , writeMutex_()
                {
                    if (sslCtx) {
                        stream_.emplace<2>(std::move(socket), *sslCtx);
                        std::get<2>(stream_).text(false);
                        *needToRun = true;
                    } else {
                        try {
                            boost::beast::http::read(socket, buffer_, initialReq_);
                            if (boost::beast::websocket::is_upgrade(initialReq_)) {
                                auto t = initialReq_.target();
                                targetPath_ = std::string {t.data(), t.length()};
                                stream_.emplace<1>(std::move(socket));
                                std::get<1>(stream_).text(false);
                                *needToRun = true;
                            } else {
                                *needToRun = false;
                            }
                        } catch (...) {
                            *needToRun = false;
                        }
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
                            initialReq_
                            , boost::beast::bind_front_handler(
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
                        boost::beast::http::read(std::get<2>(stream_).next_layer(), buffer_, initialReq_);
                        if (boost::beast::websocket::is_upgrade(initialReq_)) {
                            auto t = initialReq_.target();
                            targetPath_ = std::string {t.data(), t.length()};
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
                                initialReq_
                                , boost::beast::bind_front_handler(
                                    &OneClientHandler::onWsHandshake 
                                    , shared_from_this()
                                )
                            );
                        } else {
                            parent_->removeClientHandler(shared_from_this());
                        }
                    }
                }
                void onWsHandshake(boost::system::error_code ec) {
                    if (ec) {
                        parent_->removeClientHandler(shared_from_this());
                    } else {
                        good_ = true;
                        buffer_.clear();
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
                        auto input = boost::beast::buffers_to_string(buffer_.data());
                        auto parseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithID>::apply(std::string_view {input}, 0);
                        if (parseRes && std::get<1>(*parseRes) == input.length()) {
                            parent_->callServer(this, std::move(std::get<0>(*parseRes)));
                        }         
                        buffer_.clear();         
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
                void sendReply(bool isFinal, basic::ByteDataWithID &&data) {
                    if (!good_) {
                        return;
                    } 
                    try {
                        std::lock_guard<std::mutex> _(writeMutex_);
                        auto encodedData = basic::bytedata_utils::RunSerializer<basic::CBOR<std::tuple<bool,basic::ByteDataWithID>>>::apply({{isFinal, std::move(data)}});
                        if (stream_.index() == 1) {
                            std::get<1>(stream_).write(
                                boost::asio::buffer(reinterpret_cast<const char *>(encodedData.data()), encodedData.size())
                            );
                        } else {
                            std::get<2>(stream_).write(
                                boost::asio::buffer(reinterpret_cast<const char *>(encodedData.data()), encodedData.size())
                            );
                        }
                    } catch (...) {
                        good_ = false;
                        parent_->removeClientHandler(shared_from_this());
                    }
                }
                void onWrite(boost::beast::error_code ec, std::size_t bytes_transferred) {
                    if (ec) {
                        good_ = false;
                        parent_->removeClientHandler(shared_from_this());
                    }
                }
                std::string const &targetPath() const {
                    return targetPath_;
                }
            };

            WebSocketComponentImpl *parent_;
            int port_;
            struct OneServerInfo {
                std::function<void(basic::ByteDataWithID &&)> server_;
                std::optional<WireToUserHook> wireToUserHook_;
            };
            std::unordered_map<std::string, OneServerInfo> servers_;
            std::mutex serversMutex_;
            boost::asio::io_context svc_;
            std::optional<boost::asio::ssl::context> sslCtx_;
            std::thread th_;
            std::atomic<bool> running_;
            boost::asio::ip::tcp::acceptor acceptor_;

            std::unordered_set<std::shared_ptr<OneClientHandler>> clientHandlers_;
            std::unordered_map<std::string, std::unordered_map<std::string, OneClientHandler *>> clientHandlersByID_;
            std::unordered_map<OneClientHandler *, std::unordered_set<std::string>> idsByClientHandler_;
            std::mutex clientHandlersMutex_;
        
        public:
            OneRPCServer(
                WebSocketComponentImpl *parent
                , int port
                , std::optional<TLSServerInfo> const &sslInfo
            ) 
                : parent_(parent), port_(port), servers_(), serversMutex_()
                , svc_()
                , sslCtx_(
                    sslInfo
                    ? std::optional<boost::asio::ssl::context>(boost::asio::ssl::context {boost::asio::ssl::context::tlsv12})
                    : std::nullopt
                )
                , th_(), running_(false)
                , acceptor_(svc_)
                , clientHandlers_(), clientHandlersByID_(), idsByClientHandler_(), clientHandlersMutex_()
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
            ~OneRPCServer() {
                if (running_) {
                    running_ = false;
                    svc_.stop();
                    th_.join();
                }
            }
            void addServer(
                std::string const &targetPath
                , std::function<void(basic::ByteDataWithID &&)> server
                , std::optional<WireToUserHook> const &wireToUserHook
            ) {
                std::lock_guard<std::mutex> _(serversMutex_);
                auto iter = servers_.find(targetPath);
                if (iter != servers_.end()) {
                    throw WebSocketComponentException("Can't add multiple servers to path '"+targetPath+"' on port "+std::to_string(port_));
                }
                servers_.insert({targetPath, OneServerInfo {server, wireToUserHook}});
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
                        &OneRPCServer::onAccept
                        , shared_from_this()
                    )
                );
            }
            void onAccept(boost::beast::error_code ec, boost::asio::ip::tcp::socket socket) {
                if (!ec) {
                    bool needToRun;
                    auto h = std::make_shared<OneClientHandler>(this, std::move(socket), sslCtx_, &needToRun);
                    if (needToRun) {
                        {
                            std::lock_guard<std::mutex> _(clientHandlersMutex_);
                            clientHandlers_.insert(h);
                        }
                        h->run();
                    }
                    acceptor_.async_accept(boost::asio::make_strand(svc_)
                        , boost::beast::bind_front_handler(
                            &OneRPCServer::onAccept
                            , shared_from_this()
                        )
                    );
                } else {
                    parent_->removeRPCServer(port_);
                }
            }
            void removeClientHandler(std::shared_ptr<OneClientHandler> const &toBeRemoved) {
                std::lock_guard<std::mutex> _(clientHandlersMutex_);
                auto iter = idsByClientHandler_.find(toBeRemoved.get());
                auto iter1 = clientHandlersByID_.find(toBeRemoved->targetPath());
                if (iter != idsByClientHandler_.end()) {
                    for (auto const &id : iter->second) {
                        if (iter1 != clientHandlersByID_.end()) {
                            iter1->second.erase(id);
                        }
                    }
                    iter->second.clear();
                    idsByClientHandler_.erase(iter);
                }
                clientHandlers_.erase(toBeRemoved);
            }
            void sendReply(std::string const &targetPath, bool isFinal, basic::ByteDataWithID &&reply) {
                if (!running_) {
                    return;
                }
                std::lock_guard<std::mutex> _(clientHandlersMutex_);
                auto iter = clientHandlersByID_.find(targetPath);
                if (iter == clientHandlersByID_.end()) {
                    return;
                }
                auto iter1 = iter->second.find(reply.id);
                if (iter1 == iter->second.end()) {
                    return;
                }
                auto *p = iter1->second;
                if (isFinal) {
                    auto iter2 = idsByClientHandler_.find(p);
                    if (iter2 != idsByClientHandler_.end()) {
                        iter2->second.erase(reply.id);
                    }
                    iter->second.erase(iter1);
                }
                p->sendReply(isFinal, std::move(reply));
            }
            void callServer(OneClientHandler *p, basic::ByteDataWithID &&data) {
                {
                    std::lock_guard<std::mutex> _(clientHandlersMutex_);
                    clientHandlersByID_[p->targetPath()][data.id] = p;
                    idsByClientHandler_[p].insert(data.id);
                }
                {
                    std::lock_guard<std::mutex> _(serversMutex_);
                    auto iter = servers_.find(p->targetPath());
                    if (iter == servers_.end()) {
                        return;
                    }
                    if (iter->second.wireToUserHook_) {
                        auto d = (iter->second.wireToUserHook_->hook)(basic::ByteDataView {std::string_view(data.content)});
                        if (d) {
                            iter->second.server_({std::move(data.id), std::move(d->content)});
                        }
                    } else {
                        iter->second.server_(std::move(data));
                    }
                }
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
        };
        std::unordered_map<int, std::shared_ptr<OneRPCServer>> rpcServerMap_;
        std::mutex rpcServerMapMutex_;
        
        std::atomic<bool> started_;       
    public:
        WebSocketComponentImpl() 
            : publisherMap_(), publisherMapMutex_(), rpcClientMap_(), rpcClientMapMutex_(), rpcServerMap_(), rpcServerMapMutex_(), started_(false)
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
            std::string targetPath = locator.identifier();
            if (!boost::starts_with(targetPath, "/")) {
                targetPath = "/"+targetPath;
            }
            auto *p = iter->second.get();
            if (userToWireHook) {
                auto hook = userToWireHook->hook;
                return [targetPath,p,hook](basic::ByteDataWithTopic &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    p->publish(targetPath, {std::move(data.topic), std::move(x.content)});
                };
            } else {
                return [targetPath,p](basic::ByteDataWithTopic &&data) {
                    p->publish(targetPath,std::move(data));
                };
            }
        }
        std::function<void(basic::ByteDataWithID &&)> websocket_setRPCClient(ConnectionLocator const &locator,
                        std::function<void(bool, basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair,
                        uint32_t *clientNumberOutput,
                        TLSClientConfigurationComponent *config) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            OneRPCClient *conn = nullptr;
            {
                std::lock_guard<std::mutex> _(rpcClientMapMutex_);
                auto iter = rpcClientMap_.find(locator);
                if (iter == rpcClientMap_.end()) {
                    iter = rpcClientMap_.insert(
                        {
                            locator
                            , std::make_shared<OneRPCClient>(
                                this
                                , locator
                                , (config?config->getConfigurationItem(TLSClientInfoKey {locator.host(), locator.port()}):std::nullopt)
                            )
                        }
                    ).first;
                    if (iter->second->initializationFailure()) {
                        rpcClientMap_.erase(iter);
                    } else {
                        iter->second->run();
                    }
                }
                if (iter != rpcClientMap_.end()) {
                    conn = iter->second.get();
                }
            }
            if (!conn) {
                if (clientNumberOutput) {
                    *clientNumberOutput = 0;
                }
                return {};
            }
            auto clientNum = conn->addClient(client, wireToUserHook);
            if (clientNumberOutput) {
                *clientNumberOutput = clientNum;
            }
            if (hookPair && hookPair->userToWire) {
                auto hook = hookPair->userToWire->hook;
                return [conn,hook,clientNum](basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->sendRequest(clientNum, {data.id, std::move(x.content)});
                };
            } else {
                return [conn,clientNum](basic::ByteDataWithID &&data) {
                    conn->sendRequest(clientNum, std::move(data));
                };
            }
        }
        void websocket_removeRPCClient(ConnectionLocator const &locator, uint32_t clientNumber) {
            std::lock_guard<std::mutex> _(rpcClientMapMutex_);
            auto iter = rpcClientMap_.find(locator);
            if (iter != rpcClientMap_.end()) {
                if (iter->second->removeClient(clientNumber) == 0) {
                    rpcClientMap_.erase(iter);
                }
            }
        }
        std::function<void(bool, basic::ByteDataWithID &&)> websocket_setRPCServer(
            ConnectionLocator const &locator,
            std::function<void(basic::ByteDataWithID &&)> server,
            std::optional<ByteDataHookPair> hookPair,
            TLSServerConfigurationComponent *config)
        {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            std::string targetPath = locator.identifier();
            if (!boost::starts_with(targetPath, "/")) {
                targetPath = "/"+targetPath;
            }
            auto iter = rpcServerMap_.find(locator.port());
            if (iter == rpcServerMap_.end()) {
                iter = rpcServerMap_.insert({
                    locator.port()
                    , std::make_shared<OneRPCServer>(
                        this
                        , locator.port()
                        , (config?config->getConfigurationItem(TLSServerInfoKey {locator.port()}):std::nullopt)
                    )
                }).first;
                if (started_) {
                    iter->second->run();
                }
            }
            iter->second->addServer(targetPath, server, wireToUserHook);
            auto *p = iter->second.get();
            if (hookPair && hookPair->userToWire) {
                auto hook = hookPair->userToWire->hook;
                return [targetPath,p,hook](bool isFinal, basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    p->sendReply(targetPath, isFinal, {data.id, std::move(x.content)});
                };
            } else {
                return [targetPath,p](bool isFinal, basic::ByteDataWithID &&data) {
                    p->sendReply(targetPath, isFinal, std::move(data));
                };
            }
        }
        void finalizeEnvironment() {
            {
                std::lock_guard<std::mutex> _(publisherMapMutex_);
                for (auto const &item : publisherMap_) {
                    item.second->run();
                }
            }
            {
                std::lock_guard<std::mutex> _(rpcServerMapMutex_);
                for (auto const &item : rpcServerMap_) {
                    item.second->run();
                }
            }
            started_ = true;
        }

        void removePublisher(int port) {
            std::lock_guard<std::mutex> _(publisherMapMutex_);
            publisherMap_.erase(port);
        }
        //signature forces copy
        void removeRPCClient(ConnectionLocator locator) {
            std::lock_guard<std::mutex> _(rpcClientMapMutex_);
            rpcClientMap_.erase(locator);
        }
        void removeRPCServer(int port) {
            std::lock_guard<std::mutex> _(rpcServerMapMutex_);
            rpcServerMap_.erase(port);
        }
        std::unordered_map<ConnectionLocator, std::thread::native_handle_type> threadHandles() {
            std::unordered_map<ConnectionLocator, std::thread::native_handle_type> retVal;
            {
                std::lock_guard<std::mutex> _(publisherMapMutex_);
                for (auto &item : publisherMap_) {
                    ConnectionLocator l {"", item.first, "", ""};
                    retVal[l] = item.second->getThreadHandle();
                }
            }
            {
                std::lock_guard<std::mutex> _(rpcClientMapMutex_);
                for (auto &item : rpcClientMap_) {
                    retVal[item.first] = item.second->getThreadHandle();
                }
            }
            {
                std::lock_guard<std::mutex> _(rpcServerMapMutex_);
                for (auto &item : rpcServerMap_) {
                    ConnectionLocator l {"", item.first, "", ""};
                    retVal[l] = item.second->getThreadHandle();
                }
            }
            return retVal;
        }
    };

    WebSocketComponent::WebSocketComponent() : impl_(std::make_unique<WebSocketComponentImpl>()) {}
    WebSocketComponent::WebSocketComponent(WebSocketComponent &&) = default;
    WebSocketComponent &WebSocketComponent::operator=(WebSocketComponent &&) = default;
    WebSocketComponent::~WebSocketComponent() = default;
    std::function<void(basic::ByteDataWithTopic &&)> WebSocketComponent::websocket_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook) {
        return impl_->getPublisher(locator, userToWireHook, dynamic_cast<TLSServerConfigurationComponent *>(this));
    }
    std::function<void(basic::ByteDataWithID &&)> WebSocketComponent::websocket_setRPCClient(ConnectionLocator const &locator,
                    std::function<void(bool, basic::ByteDataWithID &&)> client,
                    std::optional<ByteDataHookPair> hookPair,
                    uint32_t *clientNumberOutput) {
        return impl_->websocket_setRPCClient(locator, client, hookPair, clientNumberOutput, dynamic_cast<TLSClientConfigurationComponent *>(this));
    }
    void WebSocketComponent::websocket_removeRPCClient(ConnectionLocator const &locator, uint32_t clientNumber) {
        impl_->websocket_removeRPCClient(locator, clientNumber);
    }
    std::function<void(bool, basic::ByteDataWithID &&)> WebSocketComponent::websocket_setRPCServer(
        ConnectionLocator const &locator,
        std::function<void(basic::ByteDataWithID &&)> server,
        std::optional<ByteDataHookPair> hookPair
    ) {
        return impl_->websocket_setRPCServer(locator, server, hookPair, dynamic_cast<TLSServerConfigurationComponent *>(this));    
    }
    void WebSocketComponent::finalizeEnvironment() {
        impl_->finalizeEnvironment();
    }
    std::unordered_map<ConnectionLocator, std::thread::native_handle_type> WebSocketComponent::websocket_threadHandles() {
        return impl_->threadHandles();
    }

} } } } }