#ifndef TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_PUBLISHER_MANAGING_UTILS_HPP_
#define TM_KIT_TRANSPORT_MULTI_TRANSPORT_BROADCAST_PUBLISHER_MANAGING_UTILS_HPP_

#include <tm_kit/transport/MultiTransportBroadcastListener.hpp>
#include <tm_kit/transport/multicast/MulticastImporterExporter.hpp>
#include <tm_kit/transport/rabbitmq/RabbitMQImporterExporter.hpp>
#include <tm_kit/transport/redis/RedisImporterExporter.hpp>
#include <tm_kit/transport/zeromq/ZeroMQImporterExporter.hpp>
#include <tm_kit/transport/nng/NNGImporterExporter.hpp>

#include <tm_kit/basic/CommonFlowUtils.hpp>
#include <tm_kit/basic/AppRunnerUtils.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport {

    template <class R>
    class MultiTransportBroadcastPublisherManagingUtils {
    public:
        using M = typename R::AppType;
        using Env = typename R::EnvironmentType;

        template <class OutputType>
        static auto oneBroadcastPublisher(
            R &r
            , std::string const &name
            , std::string const &channelSpec
            , std::optional<UserToWireHook> hook = std::nullopt
            , bool threaded = false
        ) -> typename R::template Sink<basic::TypedDataWithTopic<OutputType>>
        {
            if constexpr (!basic::bytedata_utils::DirectlySerializableChecker<OutputType>::IsDirectlySerializable()) {
                auto s = oneBroadcastPublisher<basic::CBOR<OutputType>>(
                    r, name, channelSpec, hook, false
                );
                auto transform = M::template liftPure<basic::TypedDataWithTopic<OutputType>>(
                    [](basic::TypedDataWithTopic<OutputType> &&x) -> basic::TypedDataWithTopic<basic::CBOR<OutputType>> {
                        return {
                            std::move(x.topic)
                            , {std::move(x.content)}
                        };
                    }
                    , typename infra::LiftParameters<typename M::TimePoint>().SuggestThreaded(threaded)
                );
                r.registerAction(name+":transform", transform);
                r.connect(r.actionAsSource(transform), s);
                return r.actionAsSink(transform);
            } else {
                if (threaded) {
                    auto wrapper = M::template kleisli<basic::TypedDataWithTopic<OutputType>>(
                        basic::CommonFlowUtilComponents<M>::template idFunc<basic::TypedDataWithTopic<OutputType>>()
                        , typename infra::LiftParameters<typename M::TimePoint>().SuggestThreaded(true)
                    );
                    auto s = oneBroadcastPublisher<OutputType>(r, name, channelSpec, hook, false);
                    r.registerAction(name+":threadWrapper", wrapper);
                    r.connect(r.actionAsSource(wrapper), s);
                    return r.actionAsSink(wrapper);
                }
                auto parsed = parseMultiTransportBroadcastChannel(channelSpec);
                if (!parsed) {
                    throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneBroadcastPublisher] Unknown channel spec '"+channelSpec+"'");
                }
                switch (std::get<0>(*parsed)) {
                case MultiTransportBroadcastListenerConnectionType::Multicast:
                    if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                        auto pub = multicast::MulticastImporterExporter<Env>::template createTypedExporter<OutputType>(
                            std::get<1>(*parsed), hook, name
                        );
                        r.registerExporter(name, pub);
                        return r.exporterAsSink(pub);
                    } else {
                        throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneBroadcastPublisher] Trying to create multicast publisher with channel spec '"+channelSpec+"', but multicast is unsupported in the environment");
                    }
                    break;
                case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                    if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                        auto pub = rabbitmq::RabbitMQImporterExporter<Env>::template createTypedExporter<OutputType>(
                            std::get<1>(*parsed), hook, name
                        );
                        r.registerExporter(name, pub);
                        return r.exporterAsSink(pub);
                    } else {
                        throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneBroadcastPublisher] Trying to create rabbitmq publisher with channel spec '"+channelSpec+"', but rabbitmq is unsupported in the environment");
                    }
                    break;
                case MultiTransportBroadcastListenerConnectionType::Redis:
                    if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                        auto pub = redis::RedisImporterExporter<Env>::template createTypedExporter<OutputType>(
                            std::get<1>(*parsed), hook, name
                        );
                        r.registerExporter(name, pub);
                        return r.exporterAsSink(pub);
                    } else {
                        throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneBroadcastPublisher] Trying to create redis publisher with channel spec '"+channelSpec+"', but redis is unsupported in the environment");
                    }
                    break;
                case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                    if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                        auto pub = zeromq::ZeroMQImporterExporter<Env>::template createTypedExporter<OutputType>(
                            std::get<1>(*parsed), hook, name
                        );
                        r.registerExporter(name, pub);
                        return r.exporterAsSink(pub);
                    } else {
                        throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneBroadcastPublisher] Trying to create zeromq publisher with channel spec '"+channelSpec+"', but zeromq is unsupported in the environment");
                    }
                    break;
                case MultiTransportBroadcastListenerConnectionType::NNG:
                    if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                        auto pub = nng::NNGImporterExporter<Env>::template createTypedExporter<OutputType>(
                            std::get<1>(*parsed), hook, name
                        );
                        r.registerExporter(name, pub);
                        return r.exporterAsSink(pub);
                    } else {
                        throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneBroadcastPublisher] Trying to create nng publisher with channel spec '"+channelSpec+"', but nng is unsupported in the environment");
                    }
                    break;
                default:
                    throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneBroadcastPublisher] Trying to create unknown-protocol publisher with channel spec '"+channelSpec+"'");
                    break;
                }
            }
        }
        
        static auto oneByteDataBroadcastPublisher(
            R &r
            , std::string const &name
            , std::string const &channelSpec
            , std::optional<UserToWireHook> hook = std::nullopt
            , bool threaded = false
        ) -> typename R::template Sink<basic::ByteDataWithTopic>
        {
            if (threaded) {
                auto wrapper = M::template kleisli<basic::ByteDataWithTopic>(
                    basic::CommonFlowUtilComponents<M>::template idFunc<basic::ByteDataWithTopic>()
                    , typename infra::LiftParameters<typename M::TimePoint>().SuggestThreaded(true)
                );
                auto s = oneByteDataBroadcastPublisher(r, name, channelSpec, hook, false);
                r.registerAction(name+":threadWrapper", wrapper);
                r.connect(r.actionAsSource(wrapper), s);
                return r.actionAsSink(wrapper);
            }
            auto parsed = parseMultiTransportBroadcastChannel(channelSpec);
            if (!parsed) {
                throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneByteDataBroadcastPublisher] Unknown channel spec '"+channelSpec+"'");
            }
            switch (std::get<0>(*parsed)) {
            case MultiTransportBroadcastListenerConnectionType::Multicast:
                if constexpr (std::is_convertible_v<Env *, multicast::MulticastComponent *>) {
                    auto pub = multicast::MulticastImporterExporter<Env>::createExporter(
                        std::get<1>(*parsed), hook, name
                    );
                    r.registerExporter(name, pub);
                    return r.exporterAsSink(pub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneByteDataBroadcastPublisher] Trying to create multicast publisher with channel spec '"+channelSpec+"', but multicast is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::RabbitMQ:
                if constexpr (std::is_convertible_v<Env *, rabbitmq::RabbitMQComponent *>) {
                    auto pub = rabbitmq::RabbitMQImporterExporter<Env>::createExporter(
                        std::get<1>(*parsed), hook, name
                    );
                    r.registerExporter(name, pub);
                    return r.exporterAsSink(pub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneByteDataBroadcastPublisher] Trying to create rabbitmq publisher with channel spec '"+channelSpec+"', but rabbitmq is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::Redis:
                if constexpr (std::is_convertible_v<Env *, redis::RedisComponent *>) {
                    auto pub = redis::RedisImporterExporter<Env>::createExporter(
                        std::get<1>(*parsed), hook, name
                    );
                    r.registerExporter(name, pub);
                    return r.exporterAsSink(pub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneByteDataBroadcastPublisher] Trying to create redis publisher with channel spec '"+channelSpec+"', but redis is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::ZeroMQ:
                if constexpr (std::is_convertible_v<Env *, zeromq::ZeroMQComponent *>) {
                    auto pub = zeromq::ZeroMQImporterExporter<Env>::createExporter(
                        std::get<1>(*parsed), hook, name
                    );
                    r.registerExporter(name, pub);
                    return r.exporterAsSink(pub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneByteDataBroadcastPublisher] Trying to create zeromq publisher with channel spec '"+channelSpec+"', but zeromq is unsupported in the environment");
                }
                break;
            case MultiTransportBroadcastListenerConnectionType::NNG:
                if constexpr (std::is_convertible_v<Env *, nng::NNGComponent *>) {
                    auto pub = nng::NNGImporterExporter<Env>::createExporter(
                        std::get<1>(*parsed), hook, name
                    );
                    r.registerExporter(name, pub);
                    return r.exporterAsSink(pub);
                } else {
                    throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneByteDataBroadcastPublisher] Trying to create nng publisher with channel spec '"+channelSpec+"', but nng is unsupported in the environment");
                }
                break;
            default:
                throw std::runtime_error("[MultiTransportBroadcastPublisherManagingUtils::oneByteDataBroadcastPublisher] Trying to create unknown-protocol publisher with channel spec '"+channelSpec+"'");
                break;
            }
        }
    };

} } } }

#endif