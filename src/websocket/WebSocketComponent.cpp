#include <tm_kit/transport/websocket/WebSocketComponent.hpp>
#include <tm_kit/transport/TLSConfigurationComponent.hpp>

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/LoggingComponentBase.hpp>

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

#include "../BoostCertifyAdaptor.hpp"

#include <openssl/ssl.h>

#include <unordered_map>
#include <thread>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sstream>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace web_socket {

    class WebSocketComponentImpl {
    private:
        class OneSubscriber : public std::enable_shared_from_this<OneSubscriber> {
        private:
            WebSocketComponentImpl *parent_;
            ConnectionLocator locator_;
            std::optional<basic::ByteData> initialData_;
            std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> protocolReactor_;
            std::function<void()> protocolRestartReactor_;
            bool ignoreTopic_;
            std::string handshakeHost_;
            boost::asio::io_context svc_;
            std::optional<TLSClientInfo> sslInfo_;
            std::optional<boost::asio::ssl::context> sslCtx_;
            struct ClientCB {
                uint32_t id;
                std::function<void(basic::ByteDataWithTopic &&)> cb;
                std::optional<WireToUserHook> hook;
            };
            std::vector<ClientCB> noFilterClients_;
            std::vector<std::tuple<std::string, ClientCB>> stringMatchClients_;
            std::vector<std::tuple<std::regex, ClientCB>> regexMatchClients_;
            std::mutex clientsMutex_;
            std::thread th_;
            std::atomic<bool> running_;
            boost::asio::ip::tcp::resolver resolver_;
            std::variant<
                std::monostate
                , boost::beast::websocket::stream<boost::beast::tcp_stream>
                , boost::beast::websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>>
            > stream_;
            boost::beast::flat_buffer buffer_;
            bool initializationFailure_;
            basic::LoggingComponentBase *loggingBase_;
            bool noVerify_;
            int logPerMessageCount_;
            uint64_t messageCount_;
        public:
            OneSubscriber(
                WebSocketComponentImpl *parent
                , ConnectionLocator const &locator
                , std::optional<basic::ByteData> &&initialData
                , std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> const &protocolReactor
                , std::function<void()> const &protocolRestartReactor
                , std::optional<TLSClientInfo> const &sslInfo
                , basic::LoggingComponentBase *loggingBase
                , bool noVerify
            )
                : parent_(parent), locator_(locator)
                , initialData_(std::move(initialData))
                , protocolReactor_(protocolReactor)
                , protocolRestartReactor_(protocolRestartReactor)
                , ignoreTopic_(locator.query("ignoreTopic","false")=="true")
                , svc_()
                , sslInfo_(sslInfo)
                , sslCtx_(
                    sslInfo
                    ? std::optional<boost::asio::ssl::context>(boost::asio::ssl::context {boost::asio::ssl::context::tlsv12_client})
                    : std::nullopt
                )
                , noFilterClients_(), stringMatchClients_(), regexMatchClients_(), clientsMutex_()
                , th_(), running_(false)
                , resolver_(boost::asio::make_strand(svc_))
                , stream_()
                , buffer_()
                , initializationFailure_(false)
                , loggingBase_(loggingBase)
                , noVerify_(noVerify)
                , logPerMessageCount_(std::stoi(locator.query("logPerMessageCount", "-1")))
                , messageCount_(0)
            {
                bool binary = (locator.query("binary", "true") == "true");
                if (loggingBase_) {
                    loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::(constructor)] connection binary is ")+(binary?"true":"false"));
                    if (logPerMessageCount_ > 0) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::(constructor)] logging message count for every ")+std::to_string(logPerMessageCount_)+" messages");
                    } else {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::(constructor)] not logging message count"));
                    }
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
                            initializationFailure_ = true;
                            return;
                        }
                    } else {
                        boost_certify_adaptor::initializeSslCtx(*sslCtx_, noVerify_);
                    }
                    
                    stream_.emplace<2>(boost::asio::make_strand(svc_), *sslCtx_);
                    std::get<2>(stream_).binary(binary);

                    if (loggingBase_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::(constructor)] wss (TLS) stream initialized"));
                    }
                } else {
                    stream_.emplace<1>(boost::asio::make_strand(svc_));
                    std::get<1>(stream_).binary(binary);
                    if (loggingBase_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::(constructor)] ws stream initialized"));
                    }
                }
            }
            ~OneSubscriber() {
                try {
                    if (stream_.index() == 1) {
                        std::get<1>(stream_).close(boost::beast::websocket::close_code::normal);
                    } else if (stream_.index() == 2) {
                        std::get<2>(stream_).close(boost::beast::websocket::close_code::normal);
                    }
                } catch (...) {}
                if (running_) {
                    running_ = false;
                    try {
                        svc_.stop();
                        th_.join();
                    } catch (...) {}
                }
            }
            std::shared_ptr<OneSubscriber> moveToNewOne() {
                if (loggingBase_) {
                    loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::moveToNewOne] websocket subscriber moving to new one"));
                }
                auto p = std::make_shared<OneSubscriber>(
                    parent_
                    , locator_
                    , std::move(initialData_)
                    , protocolReactor_
                    , protocolRestartReactor_
                    , sslInfo_
                    , loggingBase_
                    , noVerify_
                );
                {
                    std::lock_guard<std::mutex> _(clientsMutex_);
                    p->noFilterClients_ = std::move(noFilterClients_);
                    p->stringMatchClients_ = std::move(stringMatchClients_);
                    p->regexMatchClients_ = std::move(regexMatchClients_);
                }
                if (!p->initializationFailure()) {
                    if (loggingBase_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::moveToNewOne] starting the new one"));
                    }
                    p->run();
                } else {
                    if (loggingBase_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("[WebSocketComponentImpl::OneSubscriber::moveToNewOne] the new one has initialization failure"));
                    }
                }
                return p;
            }
            void run() {
                running_ = true;
                th_ = std::thread([this]() {                   
                    boost::asio::io_context::work work(svc_);
                    svc_.run();
                });
                th_.detach();
                if (stream_.index() == 2) {
                    if (!boost_certify_adaptor::setHostName(std::get<2>(stream_).next_layer(), locator_.host())) {
                        parent_->removeRPCClient(locator_);
                        return;
                    }
                    if(!SSL_set_tlsext_host_name(std::get<2>(stream_).next_layer().native_handle(), locator_.host().data())) {
                        parent_->removeSubscriber(locator_);
                        return;
                    }
                }
                if (loggingBase_) {
                    loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::run] websocket subscriber starting to resolve"));
                }
                resolver_.async_resolve(
                    locator_.host()
                    , std::to_string(locator_.port())
                    , boost::beast::bind_front_handler(
                        &OneSubscriber::onResolve
                        , shared_from_this()
                    )
                );
            }
            void onResolve(boost::beast::error_code ec, boost::asio::ip::tcp::resolver::results_type results) {
                if (ec) {
                    if (loggingBase_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("[WebSocketComponentImpl::OneSubscriber::onResolve] websocket subscriber resolving failure ")+ec.message());
                    }
                    parent_->removeSubscriber(locator_);
                    return;
                }
                if (loggingBase_) {
                    loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::onResolve] websocket subscriber resolution succeeded."));
                }
                if (protocolRestartReactor_) {
                    protocolRestartReactor_();
                    if (loggingBase_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::onResolve] Protocol restart reactor has run."));
                    }
                }
                if (stream_.index() == 1) {
                    boost::beast::get_lowest_layer(std::get<1>(stream_)).expires_after(std::chrono::seconds(30));
                    boost::beast::get_lowest_layer(std::get<1>(stream_)).async_connect(
                        results,
                        boost::beast::bind_front_handler(
                            &OneSubscriber::onConnect,
                            shared_from_this()
                        )
                    );
                } else {
                    boost::beast::get_lowest_layer(std::get<2>(stream_)).expires_after(std::chrono::seconds(30));
                    boost::beast::get_lowest_layer(std::get<2>(stream_)).async_connect(
                        results,
                        boost::beast::bind_front_handler(
                            &OneSubscriber::onConnect,
                            shared_from_this()
                        )
                    );
                }
            }
            void onConnect(boost::beast::error_code ec, boost::asio::ip::tcp::resolver::results_type::endpoint_type ep) {
                if (ec) {
                    if (loggingBase_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("[WebSocketComponentImpl::OneSubscriber::onConnect] websocket subscriber connection failure ")+ec.message());
                    }
                    parent_->removeSubscriber(locator_);
                    return;
                }
                if (loggingBase_) {
                    loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::onConnect] websocket subscriber connection succeeded."));
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
                            &OneSubscriber::onWebSocketHandshake
                            ,shared_from_this()
                        )
                    );
                } else {
                    boost::beast::get_lowest_layer(std::get<2>(stream_)).expires_after(std::chrono::seconds(30));
                    std::get<2>(stream_).next_layer().async_handshake(
                        boost::asio::ssl::stream_base::client
                        , boost::beast::bind_front_handler(
                            &OneSubscriber::onSSLHandshake,
                            shared_from_this()
                        )
                    );
                }
            }
            void onSSLHandshake(boost::beast::error_code ec) {
                if (ec) {
                    if (loggingBase_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("[WebSocketComponentImpl::OneSubscriber::onSSLHandshake] websocket subscriber SSL handshake failure ")+ec.message());
                    }
                    parent_->removeSubscriber(locator_);
                    return;
                }
                if (loggingBase_) {
                    loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::onSSLHandshake] websocket subscriber SSL handshake succeeded."));
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
                        &OneSubscriber::onWebSocketHandshake
                        ,shared_from_this()
                    )
                );
            }
            void onWebSocketHandshake(boost::beast::error_code ec) {
                if (ec) {
                    if (loggingBase_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("[WebSocketComponentImpl::OneSubscriber::onWebSocketHandshake] websocket subscriber ws handshake failure ")+ec.message());
                    }
                    parent_->removeSubscriber(locator_);
                    return;
                }

                if (loggingBase_) {
                    loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::onWebSocketHandshake] websocket subscriber ws handshake succeeded."));
                    if (initialData_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::onWebSocketHandshake] will send initial data of ")+std::to_string(initialData_->content.size()));
                    } else {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::onWebSocketHandshake] no initial data to send."));
                    }
                }
                
                if (stream_.index() == 1) {
                    if (initialData_) {
                        try {
                            std::get<1>(stream_).write(
                                boost::asio::buffer(reinterpret_cast<const char *>(initialData_->content.data()), initialData_->content.size())
                            );
                            if (loggingBase_) {
                                loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::onWebSocketHandshake] initial data written."));
                            }
                        } catch (...) {
                            if (loggingBase_) {
                                loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("[WebSocketComponentImpl::OneSubscriber::onWebSocketHandshake] write initial data failure."));
                            }
                            parent_->removeSubscriber(locator_);
                            return;
                        }
                    }
                    std::get<1>(stream_).async_read(
                        buffer_ 
                        , boost::beast::bind_front_handler(
                            &OneSubscriber::onRead 
                            , shared_from_this()
                        )
                    );
                } else {
                    if (initialData_) {
                        try {
                            std::get<2>(stream_).write(
                                boost::asio::buffer(reinterpret_cast<const char *>(initialData_->content.data()), initialData_->content.size())
                            );
                            if (loggingBase_) {
                                loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::onWebSocketHandshake] initial data written."));
                            }
                        } catch (...) {
                            if (loggingBase_) {
                                loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("[WebSocketComponentImpl::OneSubscriber::onWebSocketHandshake] write initial data failure."));
                            }
                            parent_->removeSubscriber(locator_);
                            return;
                        }
                    }
                    std::get<2>(stream_).async_read(
                        buffer_ 
                        , boost::beast::bind_front_handler(
                            &OneSubscriber::onRead 
                            , shared_from_this()
                        )
                    );
                }
            }
            inline void callClient(ClientCB const &c, basic::ByteDataWithTopic &&d) {
                if (c.hook) {
                    auto b = (c.hook->hook)(basic::ByteDataView {std::string_view(d.content)});
                    if (b) {
                        c.cb({std::move(d.topic), std::move(b->content)});
                    }
                } else {
                    c.cb(std::move(d));
                }
            }
            void onRead(boost::beast::error_code ec, std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);
                if (ec) {
                    if (loggingBase_) {
                        loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("[WebSocketComponentImpl::OneSubscriber::onRead] websocket subscriber read failure ")+ec.message());
                    }
                    parent_->removeSubscriber(locator_);
                    return;
                }

                if (logPerMessageCount_ > 0 && loggingBase_) {
                    ++messageCount_;
                    if ((messageCount_ % ((uint64_t) logPerMessageCount_)) == 0) {
                        if (loggingBase_) {
                            loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Info, std::string("[WebSocketComponentImpl::OneSubscriber::onRead] received ")+std::to_string(messageCount_)+ " messages so far.");
                        }
                    }
                }

                auto input = boost::beast::buffers_to_string(buffer_.data());
                std::optional<basic::ByteData> reactorRes = std::nullopt;
                if (protocolReactor_) {
                    reactorRes = protocolReactor_(basic::ByteDataView {std::string_view {input}});
                }
                
                if (ignoreTopic_) {
                    //std::cerr << "Will publish\n";
                    for (auto const &f : noFilterClients_) {
                        callClient(f, basic::ByteDataWithTopic {"", std::move(input)});
                    }
                } else {
                    basic::ByteDataWithTopic data;
                    auto parseRes = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithTopic>::applyInPlace(data, std::string_view {input}, 0);
                    if (parseRes && *parseRes == input.length()) {
                        std::lock_guard<std::mutex> _(clientsMutex_);
                        
                        for (auto const &f : noFilterClients_) {
                            callClient(f, std::move(data));
                        }
                        for (auto const &f : stringMatchClients_) {
                            if (data.topic == std::get<0>(f)) {
                                callClient(std::get<1>(f), std::move(data));
                            }
                        }
                        for (auto const &f : regexMatchClients_) {
                            if (std::regex_match(data.topic, std::get<0>(f))) {
                                callClient(std::get<1>(f), std::move(data));
                            }
                        }
                    }
                }

                buffer_.clear();
                if (stream_.index() == 1) {
                    if (reactorRes) {
                        try {
                            std::get<1>(stream_).write(
                                boost::asio::buffer(reinterpret_cast<const char *>(reactorRes->content.data()), reactorRes->content.size())
                            );
                        } catch (...) {
                            if (loggingBase_) {
                                loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("[WebSocketComponentImpl::OneSubscriber::onRead] write reactor response failure."));
                            }
                            parent_->removeSubscriber(locator_);
                            return;
                        }
                    }
                    std::get<1>(stream_).async_read(
                        buffer_ 
                        , boost::beast::bind_front_handler(
                            &OneSubscriber::onRead 
                            , shared_from_this()
                        )
                    );
                } else {
                    if (reactorRes) {
                        try {
                            std::get<2>(stream_).write(
                                boost::asio::buffer(reinterpret_cast<const char *>(reactorRes->content.data()), reactorRes->content.size())
                            );
                        } catch (...) {
                            if (loggingBase_) {
                                loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("[WebSocketComponentImpl::OneSubscriber::onRead] write reactor response failure."));
                            }
                            parent_->removeSubscriber(locator_);
                            return;
                        }
                    }
                    std::get<2>(stream_).async_read(
                        buffer_ 
                        , boost::beast::bind_front_handler(
                            &OneSubscriber::onRead 
                            , shared_from_this()
                        )
                    );
                }
            }
            bool initializationFailure() const {
                return initializationFailure_;
            }
            std::thread::native_handle_type getThreadHandle() {
                return th_.native_handle();
            }
            void addClient(
                uint32_t clientID
                , std::variant<WebSocketComponent::NoTopicSelection, std::string, std::regex> const &topic 
                , std::function<void(basic::ByteDataWithTopic &&)> client
                , std::optional<WireToUserHook> wireToUserHook
            ) {
                std::lock_guard<std::mutex> _(clientsMutex_);
                if (ignoreTopic_) {
                    noFilterClients_.push_back({clientID, client, wireToUserHook});
                } else {
                    std::visit([this,clientID,client,wireToUserHook](auto const &t) {
                        using T = std::decay_t<decltype(t)>;
                        if constexpr (std::is_same_v<T,WebSocketComponent::NoTopicSelection>) {
                            noFilterClients_.push_back({clientID, client, wireToUserHook});
                        } else if constexpr (std::is_same_v<T,std::string>) {
                            stringMatchClients_.push_back({t, {clientID, client, wireToUserHook}});
                        } else if constexpr (std::is_same_v<T,std::regex>) {
                            regexMatchClients_.push_back({t, {clientID, client, wireToUserHook}});
                        }
                    }, topic);
                }
            }
            void removeClient(uint32_t clientID) {
                std::lock_guard<std::mutex> _(clientsMutex_);
                noFilterClients_.erase(
                    std::remove_if(
                        noFilterClients_.begin()
                        , noFilterClients_.end()
                        , [clientID](auto const &x) {
                            return x.id == clientID;
                        })
                    , noFilterClients_.end()
                );
                stringMatchClients_.erase(
                    std::remove_if(
                        stringMatchClients_.begin()
                        , stringMatchClients_.end()
                        , [clientID](auto const &x) {
                            return std::get<1>(x).id == clientID;
                        })
                    , stringMatchClients_.end()
                );
                regexMatchClients_.erase(
                    std::remove_if(
                        regexMatchClients_.begin()
                        , regexMatchClients_.end()
                        , [clientID](auto const &x) {
                            return std::get<1>(x).id == clientID;
                        })
                    , regexMatchClients_.end()
                );
            }
        };
        std::unordered_map<ConnectionLocator, std::shared_ptr<OneSubscriber>> subscriberMap_;
        std::unordered_map<uint32_t, ConnectionLocator> subscriberClientIDToSubscriberLocatorMap_;
        std::atomic<uint32_t> subscriberClientIDCounter_;
        mutable std::mutex subscriberMapMutex_;

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
                std::function<std::optional<basic::ByteData>(basic::ByteDataView const &, std::atomic<bool> &)> protocolReactor_;
                basic::LoggingComponentBase *loggingBase_;
                std::atomic<bool> writeAuthorized_;
            public:
                OneClientHandler(
                    OnePublisher *parent
                    , boost::asio::ip::tcp::socket &&socket
                    , std::optional<boost::asio::ssl::context> &sslCtx
                    , bool *needToRun
                    , std::function<std::optional<basic::ByteData>(basic::ByteDataView const &, std::atomic<bool> &)> const &protocolReactor
                    , basic::LoggingComponentBase *loggingBase
                ) 
                    : parent_(parent)
                    , stream_()
                    , buffer_()
                    , initialReq_()
                    , targetPath_()
                    , good_(false)
                    , writeMutex_()
                    , protocolReactor_(protocolReactor)
                    , loggingBase_(loggingBase)
                    , writeAuthorized_(!protocolReactor)
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
                void onRead(boost::beast::error_code ec, std::size_t) {
                    if (ec) {
                        good_ = false;
                        parent_->removeClientHandler(shared_from_this());
                    } else {
                        if (protocolReactor_) {
                            auto input = boost::beast::buffers_to_string(buffer_.data());
                            std::optional<basic::ByteData> reactorRes = std::nullopt;
                            try {
                                reactorRes = protocolReactor_(basic::ByteDataView {std::string_view {input}}, writeAuthorized_);
                                if (reactorRes) {
                                    try {
                                        std::lock_guard<std::mutex> _(writeMutex_);
                                        if (stream_.index() == 1) {
                                            std::get<1>(stream_).write(
                                                boost::asio::buffer(reinterpret_cast<const char *>(reactorRes->content.data()), reactorRes->content.size())
                                            );
                                        } else {
                                            std::get<2>(stream_).write(
                                                boost::asio::buffer(reinterpret_cast<const char *>(reactorRes->content.data()), reactorRes->content.size())
                                            );
                                        }
                                    } catch (...) {
                                        if (loggingBase_) {
                                            loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, "Web socket publisher protocol responding resulted in unknown error");
                                        }
                                        good_ = false;
                                        parent_->removeClientHandler(shared_from_this());
                                        return;
                                    }
                                }
                            } catch (WebSocketComponentException const &ex) {
                                if (loggingBase_) {
                                    loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Warning, std::string("Web socket publisher protocol error '")+ex.what()+"'");
                                }
                                good_ = false;
                                parent_->removeClientHandler(shared_from_this());
                                return;
                            } catch (std::exception const &ex) {
                                if (loggingBase_) {
                                    loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, std::string("Web socket publisher protocol non-websocket-component error '")+ex.what()+"'");
                                }
                                good_ = false;
                                parent_->removeClientHandler(shared_from_this());
                                return;
                            } catch (...) {
                                if (loggingBase_) {
                                    loggingBase_->logThroughLoggingComponentBase(infra::LogLevel::Error, "Web socket publisher protocol unknown error");
                                }
                                good_ = false;
                                parent_->removeClientHandler(shared_from_this());
                                return;
                            }
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
                void doPublish(std::string_view const &data) {
                    if (!good_) {
                        return;
                    }
                    if (!writeAuthorized_) {
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
                void onWrite(boost::beast::error_code ec, std::size_t) {
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

            std::function<std::function<std::optional<basic::ByteData>(basic::ByteDataView const &, std::atomic<bool> &)>()> protocolReactorFactory_;
            basic::LoggingComponentBase *loggingBase_;
        
            void doPublish(std::unordered_set<OneClientHandler *> &handlers, std::string_view const &data) {
                for (auto *h : handlers) {
                    h->doPublish(data);
                }
            }
        public:
            OnePublisher(WebSocketComponentImpl *parent, int port, bool ignoreTopic, std::optional<TLSServerInfo> const &sslInfo, std::function<std::function<std::optional<basic::ByteData>(basic::ByteDataView const &, std::atomic<bool> &)>()> const &protocolReactorFactory, basic::LoggingComponentBase *loggingBase) 
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
                , protocolReactorFactory_(protocolReactorFactory), loggingBase_(loggingBase)
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
                    std::shared_ptr<OneClientHandler> h;
                    if (protocolReactorFactory_) {
                        h = std::make_shared<OneClientHandler>(this, std::move(socket), sslCtx_, &needToRun, protocolReactorFactory_(), loggingBase_);
                    } else {
                        h = std::make_shared<OneClientHandler>(this, std::move(socket), sslCtx_, &needToRun, std::function<std::optional<basic::ByteData>(basic::ByteDataView const &, std::atomic<bool> &)> {}, loggingBase_);
                    }
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
            bool noVerify_;
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
                , bool noVerify
            )
                : parent_(parent), locator_(locator), svc_()
                , sslCtx_(
                    sslInfo
                    ? std::optional<boost::asio::ssl::context>(boost::asio::ssl::context {boost::asio::ssl::context::tlsv12_client})
                    : std::nullopt
                )
                , noVerify_(noVerify)
                , clientCounter_(0), clients_(), clientToIDMap_(), idToClientMap_(), clientsMutex_()
                , th_(), running_(false), ready_(false)
                , resolver_(boost::asio::make_strand(svc_))
                , stream_()
                , buffer_()
                , initializationFailure_(false)
                , writeMutex_()
                , beforeReadyBuffer_()
            {
                bool binary = (locator.query("binary", "true") == "true");
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
                            initializationFailure_ = true;
                            return;
                        }
                    } else {
                        boost_certify_adaptor::initializeSslCtx(*sslCtx_, noVerify_);
                    }
                    
                    stream_.emplace<2>(boost::asio::make_strand(svc_), *sslCtx_);
                    std::get<2>(stream_).binary(binary);
                } else {
                    stream_.emplace<1>(boost::asio::make_strand(svc_));
                    std::get<1>(stream_).binary(binary);
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
                if (stream_.index() == 2) {
                    if (!boost_certify_adaptor::setHostName(std::get<2>(stream_).next_layer(), locator_.host())) {
                        parent_->removeRPCClient(locator_);
                        return;
                    }
                    if(!SSL_set_tlsext_host_name(std::get<2>(stream_).next_layer().native_handle(), locator_.host().data())) {
                        parent_->removeRPCClient(locator_);
                        return;
                    }
                }
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
                void onRead(boost::beast::error_code ec, std::size_t) {
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
                void onWrite(boost::beast::error_code ec, std::size_t) {
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
            : subscriberMap_(), subscriberClientIDToSubscriberLocatorMap_(), subscriberClientIDCounter_(0), subscriberMapMutex_(), publisherMap_(), publisherMapMutex_(), rpcClientMap_(), rpcClientMapMutex_(), rpcServerMap_(), rpcServerMapMutex_(), started_(false)
        {
        }
        ~WebSocketComponentImpl() {
        }
        uint32_t websocket_addSubscriptionClient(
            ConnectionLocator const &locator,
            std::variant<WebSocketComponent::NoTopicSelection, std::string, std::regex> const &topic,
            std::function<void(basic::ByteDataWithTopic &&)> client,
            std::optional<WireToUserHook> wireToUserHook,
            std::optional<basic::ByteData> &&initialData,
            std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> const &protocolReactor,
            std::function<void()> const &protocolRestartReactor,
            TLSClientConfigurationComponent *config,
            basic::LoggingComponentBase *loggingBase) 
        {
            int port = locator.port();
            if (port == 0) {
                if (config) {
                    port = 443;
                } else {
                    port = 80;
                }
            }
            auto actualLocator = locator.modifyPort(port);
            bool noVerify = (locator.query("no_verify", "false") == "true");
            uint32_t retVal = 0;
            {
                std::lock_guard<std::mutex> _(subscriberMapMutex_);
                retVal = ++subscriberClientIDCounter_;
                auto iter = subscriberMap_.find(locator);
                if (iter == subscriberMap_.end()) {
                    iter = subscriberMap_.insert(
                        {
                            actualLocator
                            , std::make_shared<OneSubscriber>(
                                this
                                , actualLocator
                                , std::move(initialData)
                                , protocolReactor
                                , protocolRestartReactor
                                , (config?config->getConfigurationItem(TLSClientInfoKey {actualLocator.host(), actualLocator.port()}):std::nullopt)
                                , loggingBase
                                , noVerify
                            )
                        }
                    ).first;
                    if (iter->second->initializationFailure()) {
                        subscriberMap_.erase(iter);
                        iter = subscriberMap_.end();
                    } else {
                        iter->second->run();
                    }
                }
                if (iter != subscriberMap_.end()) {
                    subscriberClientIDToSubscriberLocatorMap_[retVal] = actualLocator;
                    iter->second->addClient(retVal, topic, client, wireToUserHook);
                }
            }
            return retVal;
        }
        void websocket_removeSubscriptionClient(uint32_t id) {
            std::lock_guard<std::mutex> _(subscriberMapMutex_);
            auto iter = subscriberClientIDToSubscriberLocatorMap_.find(id);
            if (iter != subscriberClientIDToSubscriberLocatorMap_.end()) {
                auto iter1 = subscriberMap_.find(iter->second);
                if (iter1 != subscriberMap_.end()) {
                    iter1->second->removeClient(id);
                }
                subscriberClientIDToSubscriberLocatorMap_.erase(iter);
            }
        }
        std::function<void(basic::ByteDataWithTopic &&)> getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook, TLSServerConfigurationComponent *config, std::function<std::function<std::optional<basic::ByteData>(basic::ByteDataView const &, std::atomic<bool> &)>()> const &protocolReactorFactory, basic::LoggingComponentBase *loggingBase) {
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
                        , protocolReactorFactory
                        , loggingBase
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
            bool noVerify = (locator.query("no_verify", "false") == "true");
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
                                , noVerify
                            )
                        }
                    ).first;
                    if (iter->second->initializationFailure()) {
                        rpcClientMap_.erase(iter);
                        iter = rpcClientMap_.end();
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

        //signature forces copy
        void removeSubscriber(ConnectionLocator locator) {
            std::lock_guard<std::mutex> _(subscriberMapMutex_);
            auto iter = subscriberMap_.find(locator);
            if (iter != subscriberMap_.end()) {
                iter->second = iter->second->moveToNewOne();
            }
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
                std::lock_guard<std::mutex> _(subscriberMapMutex_);
                for (auto &item : subscriberMap_) {
                    retVal[item.first] = item.second->getThreadHandle();
                }
            }
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
    uint32_t WebSocketComponent::websocket_addSubscriptionClient(ConnectionLocator const &locator,
        std::variant<WebSocketComponent::NoTopicSelection, std::string, std::regex> const &topic,
        std::function<void(basic::ByteDataWithTopic &&)> client,
        std::optional<WireToUserHook> wireToUserHook,
        std::optional<basic::ByteData> &&initialData,
        std::function<std::optional<basic::ByteData>(basic::ByteDataView const &)> const &protocolReactor,
        std::function<void()> const &protocolRestartReactor) {
        return impl_->websocket_addSubscriptionClient(
            locator 
            , topic 
            , client 
            , wireToUserHook 
            , std::move(initialData)
            , protocolReactor
            , protocolRestartReactor
            , dynamic_cast<TLSClientConfigurationComponent *>(this)
            , dynamic_cast<basic::LoggingComponentBase *>(this)
        );
    }
    void WebSocketComponent::websocket_removeSubscriptionClient(uint32_t id) {
        impl_->websocket_removeSubscriptionClient(id);
    }
    std::function<void(basic::ByteDataWithTopic &&)> WebSocketComponent::websocket_getPublisher(ConnectionLocator const &locator, std::optional<UserToWireHook> userToWireHook, std::function<std::function<std::optional<basic::ByteData>(basic::ByteDataView const &, std::atomic<bool> &)>()> const &protocolReactorFactory) {
        return impl_->getPublisher(locator, userToWireHook, dynamic_cast<TLSServerConfigurationComponent *>(this), protocolReactorFactory, dynamic_cast<basic::LoggingComponentBase *>(this));
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