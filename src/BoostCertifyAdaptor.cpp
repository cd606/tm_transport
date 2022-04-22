#include <boost/certify/https_verification.hpp>
#include "BoostCertifyAdaptor.hpp"

namespace dev { namespace cd606 {namespace tm {namespace transport {
    namespace boost_certify_adaptor {
        void initializeSslCtx(boost::asio::ssl::context &ctx) {
            ctx.set_verify_mode(
                boost::asio::ssl::context::verify_peer |
                boost::asio::ssl::context::verify_fail_if_no_peer_cert
            );
            ctx.set_default_verify_paths();
            boost::certify::enable_native_https_server_verification(ctx);
            SSL_CTX_set_options(ctx.native_handle(), SSL_OP_LEGACY_SERVER_CONNECT);
        }
        bool setHostName(boost::beast::ssl_stream<boost::beast::tcp_stream> &stream, std::string const &hostName) {
            boost::system::error_code ec;
            boost::certify::detail::set_server_hostname(stream.native_handle(), hostName, ec);
            return (!ec);
        }
    }
}}}}