#include <tm_kit/transport/HeartbeatComponent.hpp>

#ifdef _MSC_VER
#include <processthreadsapi.h>
#include <winsock.h>
#else
#include <sys/types.h>
#include <unistd.h>
#endif

namespace dev { namespace cd606 { namespace tm { namespace transport {
    std::string HeartbeatComponent::getHost() {
        char buf[1024];
        if (gethostname(buf, 1024) == 0) {
            return buf;
        } else {
            return "";
        }
    }
    int64_t HeartbeatComponent::getPid() {
        #ifdef _MSC_VER
            return (int64_t) GetCurrentProcessId();
        #else
            return (int64_t) getpid();
        #endif
    }

} } } }