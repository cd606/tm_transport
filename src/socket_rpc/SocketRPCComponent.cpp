#include <thread>
#include <mutex>
#include <cstring>
#include <unordered_map>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/endian/conversion.hpp>

#include <tm_kit/transport/socket_rpc/SocketRPCComponent.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace socket_rpc {

    class SocketRPCComponentImpl {
    private:
        class OneSocketRPCClient {
        private:
            boost::asio::io_service *service_;
            ConnectionLocator locator_;
            std::function<void(bool, basic::ByteDataWithID &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            boost::asio::ip::tcp::endpoint remotePoint_;
            boost::asio::ip::tcp::socket sock_;
            std::array<char, 16*1024*1024> buffer_;
            bool good_;
            uint32_t reconnectTimeoutMs_;
            std::mutex mutex_;

            void handleReconnectTimeout(boost::system::error_code const &ec) {
                if (!ec) {
                    if (sock_.is_open()) {
                        sock_.async_connect(
                            remotePoint_
                            , boost::bind(
                                &OneSocketRPCClient::handleConnect
                                , this
                                , boost::asio::placeholders::error
                            )
                        );
                    }
                }
            }
            void handleConnect(boost::system::error_code const &ec) {
                if (ec) {
                    good_ = false;
                    if (reconnectTimeoutMs_ <= 2000) {
                        reconnectTimeoutMs_ *= 2;
                    }
                    if (sock_.is_open()) {
                        boost::asio::deadline_timer timer(*service_, boost::posix_time::milliseconds(reconnectTimeoutMs_));
                        timer.async_wait(
                            boost::bind(
                                &OneSocketRPCClient::handleReconnectTimeout
                                , this
                                , boost::asio::placeholders::error
                            )
                        );
                    }
                } else {
                    good_ = true;
                    reconnectTimeoutMs_ = 50;
                    boost::asio::async_read(
                        sock_ 
                        , boost::asio::buffer(buffer_.data(), sizeof(uint32_t))
                        , boost::bind(
                            &OneSocketRPCClient::handleDataLengthRead 
                            , this
                            , boost::asio::placeholders::error
                            , boost::asio::placeholders::bytes_transferred
                        )
                    );
                }
            }
            void handleDataLengthRead(boost::system::error_code const &ec, size_t bytes_transferred) {
                if (!ec && bytes_transferred == sizeof(uint32_t)) {
                    uint32_t len = boost::endian::little_to_native<uint32_t>(*reinterpret_cast<uint32_t const *>(buffer_.data()));
                    boost::asio::async_read(
                        sock_
                        , boost::asio::buffer(buffer_.data(), len)
                        , boost::bind(
                            &OneSocketRPCClient::handleDataRead
                            , this
                            , boost::asio::placeholders::error
                            , boost::asio::placeholders::bytes_transferred
                        )
                    );
                } else {
                    good_ = false;
                    if (reconnectTimeoutMs_ <= 2000) {
                        reconnectTimeoutMs_ *= 2;
                    }
                    if (sock_.is_open()) {
                        boost::asio::deadline_timer timer(*service_, boost::posix_time::milliseconds(reconnectTimeoutMs_));
                        timer.async_wait(
                            boost::bind(
                                &OneSocketRPCClient::handleReconnectTimeout
                                , this
                                , boost::asio::placeholders::error
                            )
                        );
                    }
                }
            }
            void handleDataRead(boost::system::error_code const &ec, size_t bytes_transferred) {
                if (!ec) {
                    std::tuple<bool, basic::ByteDataWithID> decodedDataWithID;
                    auto res = basic::bytedata_utils::RunCBORDeserializer<std::tuple<bool, basic::ByteDataWithID>>::applyInPlace(
                        decodedDataWithID 
                        , std::string_view {buffer_.data(), bytes_transferred}
                        , 0
                    );
                    if (res && *res == bytes_transferred) {
                        if (wireToUserHook_) {
                            auto d = (wireToUserHook_->hook)(basic::ByteDataView {std::string_view(std::get<1>(decodedDataWithID).content)});
                            if (d) {
                                callback_(std::get<0>(decodedDataWithID), {std::move(std::get<1>(decodedDataWithID).id), std::move(d->content)});
                            }
                        } else {
                            callback_(std::get<0>(decodedDataWithID), std::move(std::get<1>(decodedDataWithID)));
                        }
                    }
                    boost::asio::async_read(
                        sock_ 
                        , boost::asio::buffer(buffer_.data(), sizeof(uint32_t))
                        , boost::bind(
                            &OneSocketRPCClient::handleDataLengthRead 
                            , this
                            , boost::asio::placeholders::error
                            , boost::asio::placeholders::bytes_transferred
                        )
                    );
                } else {
                    good_ = false;
                    if (reconnectTimeoutMs_ <= 2000) {
                        reconnectTimeoutMs_ *= 2;
                    }
                    if (sock_.is_open()) {
                        boost::asio::deadline_timer timer(*service_, boost::posix_time::milliseconds(reconnectTimeoutMs_));
                        timer.async_wait(
                            boost::bind(
                                &OneSocketRPCClient::handleReconnectTimeout
                                , this
                                , boost::asio::placeholders::error
                            )
                        );
                    }
                }
            }
        public:
            OneSocketRPCClient(boost::asio::io_service *service, ConnectionLocator const &locator, std::function<void(bool, basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook)
                : service_(service)
                , locator_(locator.host(), locator.port())
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , sock_(*service)
                , buffer_()
                , good_(false)
                , reconnectTimeoutMs_(50)
                , mutex_()
            {
                boost::asio::ip::tcp::resolver resolver(*service);
                boost::asio::ip::tcp::resolver::query query(locator_.host(), std::to_string(locator_.port()));
                remotePoint_ = resolver.resolve(query)->endpoint();
                sock_.open(remotePoint_.protocol());
                sock_.set_option(boost::asio::ip::tcp::socket::receive_buffer_size(16*1024*1024));
                sock_.async_connect(
                    remotePoint_
                    , boost::bind(
                        &OneSocketRPCClient::handleConnect
                        , this
                        , boost::asio::placeholders::error
                    )
                );
            }
            ~OneSocketRPCClient() {
                sock_.close();
            }
            ConnectionLocator const &connectionLocator() const {
                return locator_;
            }
            void sendRequest(basic::ByteDataWithID &&data) {
                if (good_) {
                    auto encodedData = basic::bytedata_utils::RunSerializer<basic::CBOR<basic::ByteDataWithID>>::apply({std::move(data)});
                    uint32_t len = boost::endian::native_to_little<uint32_t>((uint32_t) encodedData.length());
                    {
                        std::lock_guard<std::mutex> _(mutex_);
                        boost::asio::write(
                            sock_
                            , boost::asio::buffer(reinterpret_cast<const char *>(&len), sizeof(uint32_t))
                        );
                        boost::asio::write(
                            sock_
                            , boost::asio::buffer(encodedData, encodedData.length())
                        );
                    }
                }
            }
        };

        class OneSocketRPCServer {
        private:
            friend class OneConnection;
            class OneConnection {
            private:
                friend class OneSocketRPCServer;
                OneSocketRPCServer *parent_;
                boost::asio::ip::tcp::socket sock_;
                std::array<char, 16*1024*1024> buffer_;
                std::atomic<bool> stopped_;
                void handleAccept(boost::system::error_code const &ec) {
                    if (ec) {
                        stopped_ = true;
                        delete this;
                    } else {
                        new OneConnection(parent_);
                        boost::asio::async_read(
                            sock_
                            , boost::asio::buffer(buffer_, sizeof(uint32_t))
                            , boost::bind(
                                &OneConnection::handleDataLength
                                , this
                                , boost::asio::placeholders::error
                                , boost::asio::placeholders::bytes_transferred
                            )
                        );
                    }
                }
                void handleDataLength(boost::system::error_code const &ec, size_t bytes_transferred) {
                    if (!ec && bytes_transferred == sizeof(uint32_t)) {
                        uint32_t len = boost::endian::little_to_native<uint32_t>(*reinterpret_cast<uint32_t const *>(buffer_.data()));
                        boost::asio::async_read(
                            sock_ 
                            , boost::asio::buffer(buffer_, len)
                            , boost::bind(
                                &OneConnection::handleData
                                , this
                                , boost::asio::placeholders::error
                                , boost::asio::placeholders::bytes_transferred
                            )
                        );
                    } else {
                        stopped_ = true;
                        delete this;
                    }
                }
                void handleData(boost::system::error_code const &ec, size_t bytes_transferred) {
                    if (!ec) {
                        basic::ByteDataWithID data;
                        auto res = basic::bytedata_utils::RunCBORDeserializer<basic::ByteDataWithID>::applyInPlace(
                            data, std::string_view(buffer_.data(), bytes_transferred), 0
                        );
                        if (res && *res == bytes_transferred) {
                            parent_->registerConnection(data.id, this);
                            if (parent_->wireToUserHook_) {
                                auto d = (parent_->wireToUserHook_->hook)(basic::ByteDataView {std::string_view(data.content)});
                                if (d) {
                                    parent_->callback_(basic::ByteDataWithID {std::move(data.id), std::move(d->content)});
                                }
                            } else {
                                parent_->callback_(std::move(data));
                            }
                        }
                        boost::asio::async_read(
                            sock_ 
                            , boost::asio::buffer(buffer_, sizeof(uint32_t))
                            , boost::bind(
                                &OneConnection::handleDataLength
                                , this
                                , boost::asio::placeholders::error
                                , boost::asio::placeholders::bytes_transferred
                            )
                        );
                    } else {
                        stopped_ = true;
                        parent_->removeConnection(this);
                        delete this;
                    }
                }
                void sendReply(bool isFinal, basic::ByteDataWithID &&data) {
                    if (stopped_) {
                        return;
                    }
                    auto encodedData = basic::bytedata_utils::RunSerializer<basic::CBOR<std::tuple<bool,basic::ByteDataWithID>>>::apply({{isFinal, std::move(data)}});
                    uint32_t len = boost::endian::native_to_little<uint32_t>((uint32_t) encodedData.length());
                    boost::asio::write(
                        sock_
                        , boost::asio::buffer(reinterpret_cast<char const *>(&len), sizeof(uint32_t))
                    );
                    boost::asio::write(
                        sock_ 
                        , boost::asio::buffer(encodedData.data(), encodedData.length())
                    );
                }
            public:
                OneConnection(OneSocketRPCServer *parent) 
                    : parent_(parent)
                    , sock_(*(parent_->service_)) 
                    , buffer_()
                    , stopped_(false)
                {
                    parent_->acceptor_.async_accept(
                        sock_
                        , boost::bind(
                            &OneConnection::handleAccept
                            , this
                            , boost::asio::placeholders::error
                        )
                    );
                }
                ~OneConnection() {
                    stopped_ = true;
                    parent_->removeConnection(this);
                    sock_.close();
                }
            };
            
            boost::asio::io_service *service_;
            ConnectionLocator locator_;
            std::function<void(basic::ByteDataWithID &&)> callback_;
            std::optional<WireToUserHook> wireToUserHook_;
            std::unordered_map<std::string, OneConnection *> replySocketMap_;
            std::unordered_map<OneConnection *, std::unordered_set<std::string>> reverseReplySocketMap_;
            boost::asio::ip::tcp::acceptor acceptor_;
            std::mutex mutex_;

            void registerConnection(std::string const &id, OneConnection *conn) {
                std::lock_guard<std::mutex> _(mutex_);
                replySocketMap_[id] = conn;
                reverseReplySocketMap_[conn].insert(id);
            }
            void removeConnection(OneConnection *conn) {
                std::lock_guard<std::mutex> _(mutex_);
                auto iter = reverseReplySocketMap_.find(conn);
                if (iter != reverseReplySocketMap_.end()) {
                    for (auto const &s : iter->second) {
                        replySocketMap_.erase(s);
                    }
                    reverseReplySocketMap_.erase(iter);
                }
            }
        public:
            OneSocketRPCServer(boost::asio::io_service *service, ConnectionLocator const &locator, std::function<void(basic::ByteDataWithID &&)> callback, std::optional<WireToUserHook> wireToUserHook)
                : service_(service)
                , locator_("", locator.port())
                , callback_(callback)
                , wireToUserHook_(wireToUserHook)
                , replySocketMap_()
                , reverseReplySocketMap_()
                , acceptor_(*service)
                , mutex_()
            {
                boost::asio::ip::tcp::resolver resolver(*service_);
                boost::asio::ip::tcp::resolver::query query("0.0.0.0", std::to_string(locator.port()));
                auto ep = resolver.resolve(query)->endpoint();

                acceptor_.open(ep.protocol());
                acceptor_.set_option(boost::asio::ip::udp::socket::send_buffer_size(16*1024*1024));
                acceptor_.bind(ep);

                acceptor_.listen(boost::asio::socket_base::max_listen_connections);

                new OneConnection(this);
            }
            ~OneSocketRPCServer() {
                acceptor_.close();
            }
            ConnectionLocator const &connectionLocator() const {
                return locator_;
            }
            void sendReply(bool isFinal, basic::ByteDataWithID &&data) {
                std::lock_guard<std::mutex> _(mutex_);
                auto iter = replySocketMap_.find(data.id);
                if (iter != replySocketMap_.end()) {
                    iter->second->sendReply(isFinal, std::move(data));
                    if (isFinal) {
                        auto iter1 = reverseReplySocketMap_.find(iter->second);
                        if (iter1 != reverseReplySocketMap_.end()) {
                            iter1->second.erase(iter->first);
                        }
                        replySocketMap_.erase(iter);
                    }
                }
            }
        };

        std::unordered_map<ConnectionLocator, std::unique_ptr<OneSocketRPCClient>> rpcClientMap_;
        std::unordered_map<int, std::unique_ptr<OneSocketRPCServer>> rpcServerMap_;
        boost::asio::io_service service_;

        std::thread th_;
        std::mutex mutex_;

        OneSocketRPCClient *createRpcClient(ConnectionLocator const &l, std::function<void(bool, basic::ByteDataWithID &&)> client, std::optional<WireToUserHook> wireToUserHook) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcClientMap_.find(l);
            if (iter != rpcClientMap_.end()) {
                throw SocketRPCComponentException("Cannot create duplicate RPC client connection for "+l.toSerializationFormat());
            }
            iter = rpcClientMap_.insert(
                {l, std::make_unique<OneSocketRPCClient>(&service_, l, client, wireToUserHook)}
            ).first;
            return iter->second.get();
        }
        OneSocketRPCServer *createRpcServer(ConnectionLocator const &l, std::function<void(basic::ByteDataWithID &&)> handler, std::optional<WireToUserHook> wireToUserHook) {
            std::lock_guard<std::mutex> _(mutex_);
            auto iter = rpcServerMap_.find(l.port());
            if (iter != rpcServerMap_.end()) {
                throw SocketRPCComponentException("Cannot create duplicate RPC server connection for "+l.toSerializationFormat());
            }
            iter = rpcServerMap_.insert(
                {l.port(), std::make_unique<OneSocketRPCServer>(&service_, l, handler, wireToUserHook)}
            ).first;
            return iter->second.get();
        }
    public:
        SocketRPCComponentImpl() 
            : rpcClientMap_(), rpcServerMap_(), service_(), th_(), mutex_()
        { 
            th_ = std::thread([this] {
                boost::asio::io_service::work work(service_);
                service_.run();
            });
        }
        ~SocketRPCComponentImpl() {
            std::lock_guard<std::mutex> _(mutex_);
            rpcClientMap_.clear();
            rpcServerMap_.clear();
            try {
                service_.stop();
                th_.join();
            } catch (...) {}
        }
        std::function<void(basic::ByteDataWithID &&)> setRPCClient(ConnectionLocator const &locator,
            std::function<void(bool, basic::ByteDataWithID &&)> client,
            std::optional<ByteDataHookPair> hookPair) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            ConnectionLocator simplifiedLocator {locator.host(), locator.port()};
            auto *conn = createRpcClient(simplifiedLocator, client, wireToUserHook);
            if (hookPair && hookPair->userToWire) {
                auto hook = hookPair->userToWire->hook;
                return [conn,hook](basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->sendRequest({data.id, std::move(x.content)});
                };
            } else {
                return [conn](basic::ByteDataWithID &&data) {
                    conn->sendRequest(std::move(data));
                };
            }
        }
        void removeRPCClient(ConnectionLocator const &locator) {
            std::lock_guard<std::mutex> _(mutex_);
            rpcClientMap_.erase(ConnectionLocator {locator.host(), locator.port()});
        }
        std::function<void(bool, basic::ByteDataWithID &&)> setRPCServer(ConnectionLocator const &locator,
            std::function<void(basic::ByteDataWithID &&)> server,
            std::optional<ByteDataHookPair> hookPair) {
            std::optional<WireToUserHook> wireToUserHook;
            if (hookPair) {
                wireToUserHook = hookPair->wireToUser;
            } else {
                wireToUserHook = std::nullopt;
            }
            auto *conn = createRpcServer(locator, server, wireToUserHook);
            if (hookPair && hookPair->userToWire) {
                auto hook = hookPair->userToWire->hook;
                return [conn,hook](bool isFinal, basic::ByteDataWithID &&data) {
                    auto x = hook(basic::ByteData {std::move(data.content)});
                    conn->sendReply(isFinal, {data.id, std::move(x.content)});
                };
            } else {
                return [conn](bool isFinal, basic::ByteDataWithID &&data) {
                    conn->sendReply(isFinal, std::move(data));
                };
            }
        }
        std::thread::native_handle_type threadHandle() {
            return th_.native_handle();
        }
    };

    SocketRPCComponent::SocketRPCComponent() : impl_(std::make_unique<SocketRPCComponentImpl>()) {}
    SocketRPCComponent::~SocketRPCComponent() {}
    SocketRPCComponent::SocketRPCComponent(SocketRPCComponent &&) = default;
    SocketRPCComponent &SocketRPCComponent::operator=(SocketRPCComponent &&) = default;
    std::function<void(basic::ByteDataWithID &&)> SocketRPCComponent::socket_rpc_setRPCClient(ConnectionLocator const &locator,
                        std::function<void(bool, basic::ByteDataWithID &&)> client,
                        std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCClient(locator, client, hookPair);
    }
    void SocketRPCComponent::socket_rpc_removeRPCClient(ConnectionLocator const &locator) {
        impl_->removeRPCClient(locator);
    }
    std::function<void(bool, basic::ByteDataWithID &&)> SocketRPCComponent::socket_rpc_setRPCServer(ConnectionLocator const &locator,
                    std::function<void(basic::ByteDataWithID &&)> server,
                    std::optional<ByteDataHookPair> hookPair) {
        return impl_->setRPCServer(locator, server, hookPair);
    }
    std::thread::native_handle_type SocketRPCComponent::socket_rpc_threadHandle() {
        return impl_->threadHandle();
    }

} } } } }