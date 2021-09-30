#ifdef _MSC_VER
#include <windows.h>
#include <processthreadsapi.h>
#include <winsock.h>
#else
#include <sys/types.h>
#include <unistd.h>
#endif

#include <string>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace hostname_util {
    std::string hostname() {
        char buf[1024];
        if (gethostname(buf, 1024) == 0) {
            return buf;
        } else {
            return "";
        }
    }
} } } } }