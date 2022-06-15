#include <grpc/grpc.h>
#include <boost/algorithm/string.hpp>
#include <vector>
#include <iostream>
#include <fstream>

int main(int argc, char **argv) {
    std::ofstream ofs(argv[1]);
    ofs << "#ifndef TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_VERSION_INFO_HPP_\n";
    ofs << "#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_VERSION_INFO_HPP_\n";
    ofs << "\n#include <string_view>\n";
    ofs << "namespace dev { namespace cd606 { namespace tm { namespace transport { namespace grpc_interop {\n";
    std::string version = grpc_version_string();
    ofs << "\tconstexpr std::string_view GRPC_VERSION_STRING = \"" << version << "\";\n";
    std::vector<std::string> versionParts;
    boost::split(versionParts, version, boost::is_any_of("."));
    ofs << "\t#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_VERSION_INFO_MAJOR_VERSION " << versionParts[0] << "\n";
    if (versionParts.size() > 1) {
        ofs << "\t#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_VERSION_INFO_MINOR_VERSION " << versionParts[1] << "\n";
    }
    if (versionParts.size() > 2) {
        ofs << "\t#define TM_KIT_TRANSPORT_GRPC_INTEROP_GRPC_VERSION_INFO_UPDATE_NUMBER " << versionParts[2] << "\n";
    }
    ofs << "} } } } }\n";
    ofs << "#endif\n";
    ofs.close();
    return 0;
}