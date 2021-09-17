#include <tm_kit/transport/grpc_interop/GrpcConnectionLocatorUtils.hpp>
#include <tm_kit/transport/grpc_interop/GrpcInteropComponentException.hpp>

#include <boost/algorithm/string.hpp>
#include <vector>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {

    namespace connection_locator_utils {
        GrpcServiceInfo parseServiceInfo(ConnectionLocator const &l) {
            std::string s = l.identifier();
            std::vector<std::string> parts;
            boost::split(parts, s, boost::is_any_of("/"));
            int count = parts.size();
            int ii = 0;
            while (ii < count && boost::trim_copy(parts[ii]) == "") {
                ++ii;
            }
            if (count-ii < 3) {
                throw GrpcInteropComponentException("Wrong connection locator identity: must be namespace/service/method");
            }
            return GrpcServiceInfo {
                boost::trim_copy(parts[ii])
                , boost::trim_copy(parts[ii+1])
                , boost::trim_copy(parts[ii+2])
                , (l.query("single_rpc", "false") == "true")
            };
        }

        GrpcSSLInfo parseSSLInfo(ConnectionLocator const &l) {
            GrpcSSLInfo sslInfo = std::monostate {};
            if (l.query("ssl", "false") == "true") {
                if (
                    l.query("ca_cert", "") != ""
                    && l.query("client_cert", "") != ""
                    && l.query("client_key", "") != ""
                ) {
                    sslInfo = GrpcSSLClientInfo {
                        l.query("ca_cert", "")
                        , l.query("client_cert", "")
                        , l.query("client_key", "")
                    };
                } else if (
                    l.query("server_cert", "") != ""
                    && l.query("server_key", "") != ""
                ) {
                    sslInfo = GrpcSSLServerInfo {
                        l.query("server_cert", "")
                        , l.query("server_key", "")
                    };
                }
            }
            return sslInfo;
        }
    }

} } } } }
