#include <grpcpp/grpcpp.h>
#include <iostream>
#include <fstream>

int main(int argc, char **argv) {
    std::ofstream ofs(argv[1]);
    ofs << "#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_VERSION_INFO_HPP_\n";
    ofs << "#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_VERSION_INFO_HPP_\n";
    ofs << "\n#include <string_view>\n";
    ofs << "namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {\n";
    ofs << "\tconstexpr std::string_view GRPC_VERSION_STRING = \"" << grpc::Version() << "\";\n";
    ofs << "} } } } }\n";
    ofs << "#endif\n";
    ofs.close();
    return 0;
}