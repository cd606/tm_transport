#ifndef TM_KIT_TRANSPORT_EXIT_DATA_SOURCE_HPP_
#define TM_KIT_TRANSPORT_EXIT_DATA_SOURCE_HPP_

#include <tm_kit/infra/RealTimeApp.hpp>
#include <tm_kit/basic/VoidStruct.hpp>

#include <boost/asio/signal_set.hpp>
#if BOOST_VERSION >= 108700
#include <boost/asio/io_context.hpp>
#else
#include <boost/asio/io_service.hpp>
#endif

#include <thread>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    class ExitDataSourceCreator {
    public:
        template <class R>
#if BOOST_VERSION >= 108700
        static auto addExitDataSource(R &r, std::string const &sourceName, boost::asio::io_context *svcPtr = nullptr)
#else
        static auto addExitDataSource(R &r, std::string const &sourceName, boost::asio::io_service *svcPtr = nullptr)
#endif
        -> std::tuple<
            typename R::template Source<basic::VoidStruct>
            , std::function<void()>
        >
        {
            using M = typename R::AppType;
            auto importerPair = M::template constTriggerImporter<basic::VoidStruct>();
            r.registerImporter(sourceName, std::get<0>(importerPair));
            auto triggerF = std::get<1>(importerPair);

            std::shared_ptr<boost::asio::signal_set> signals;
            if (svcPtr) {
                signals = std::make_shared<boost::asio::signal_set>(*svcPtr, SIGINT, SIGTERM);
            } else {
#if BOOST_VERSION >= 108700
                auto svc = std::make_shared<boost::asio::io_context>();
#else
                auto svc = std::make_shared<boost::asio::io_service>();
#endif
                r.preservePointer(svc);

                std::thread th([svc]() {
#if BOOST_VERSION >= 108700
                    auto work_guard = boost::asio::make_work_guard(*svc);
#else
                    boost::asio::io_service::work w(*svc);
#endif
                    svc->run();
                });
                th.detach();
                signals = std::make_shared<boost::asio::signal_set>(*svc, SIGINT, SIGTERM);
            }
            r.preservePointer(signals);
            signals->async_wait([triggerF](const boost::system::error_code &error_code, int signal_number) {
                triggerF();
            });
            return {r.importItem(std::get<0>(importerPair)), triggerF};
        }
    };
    
} } } }

#endif