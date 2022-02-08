#ifndef TM_KIT_TRANSPORT_SRC_BOOST_CERTIFY_ADAPTOR_HPP_
#define TM_KIT_TRANSPORT_SRC_BOOST_CERTIFY_ADAPTOR_HPP_

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include <string>

namespace dev { namespace cd606 {namespace tm {namespace transport {
    namespace boost_certify_adaptor {
        extern void initializeSslCtx(boost::asio::ssl::context &ctx);
        extern bool setHostName(boost::beast::ssl_stream<boost::beast::tcp_stream> &stream, std::string const &hostName);
    }
}}}}

#endif