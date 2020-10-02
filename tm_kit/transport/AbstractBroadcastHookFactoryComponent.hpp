#ifndef TM_KIT_TRANSPORT_ABSTRACT_BROADCAST_HOOK_FACTORY_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_ABSTRACT_BROADCAST_HOOK_FACTORY_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>
#include <type_traits>

namespace dev { namespace cd606 { namespace tm { namespace transport {
 
    template <class DataT>
    class AbstractOutgoingBroadcastHookFactoryComponent {
    public:
        virtual ~AbstractOutgoingBroadcastHookFactoryComponent() {}
        virtual UserToWireHook defaultHook() = 0;
    };
    template <class DataT>
    class AbstractIncomingBroadcastHookFactoryComponent {
    public:
        virtual ~AbstractIncomingBroadcastHookFactoryComponent() {}
        virtual WireToUserHook defaultHook() = 0;
    };

    template <class DataT>
    class EmptyOutgoingBroadcastHookFactoryComponent final : public AbstractOutgoingBroadcastHookFactoryComponent<DataT> {
    public:
        virtual ~EmptyOutgoingBroadcastHookFactoryComponent() {}
        virtual UserToWireHook defaultHook() override final {
            return UserToWireHook {
                [](basic::ByteData &&d) -> basic::ByteData {
                    return std::move(d);
                }
            };
        }
    };
    template <class DataT>
    class EmptyIncomingBroadcastHookFactoryComponent final : public AbstractIncomingBroadcastHookFactoryComponent<DataT> {
    public:
        virtual ~EmptyIncomingBroadcastHookFactoryComponent() {}
        virtual WireToUserHook defaultHook() override final {
            return WireToUserHook {
                [](basic::ByteData &&d) -> std::optional<basic::ByteData> {
                    return { std::move(d) };
                }
            };
        }
    };

    template <class Env>
    class DefaultBroadcastHookFactory {
    public:
        template <class DataT>
        static std::optional<UserToWireHook> outgoingHook(Env *env) {
            if constexpr (std::is_convertible_v<Env *, AbstractOutgoingBroadcastHookFactoryComponent<DataT> *>) {
                return static_cast<AbstractOutgoingBroadcastHookFactoryComponent<DataT> *>(env)->defaultHook();
            } else {
                return std::nullopt;
            }
        }
        template <class DataT>
        static std::optional<WireToUserHook> incomingHook(Env *env) {
            if constexpr (std::is_convertible_v<Env *, AbstractIncomingBroadcastHookFactoryComponent<DataT> *>) {
                return static_cast<AbstractIncomingBroadcastHookFactoryComponent<DataT> *>(env)->defaultHook();
            } else {
                return std::nullopt;
            }
        }
    };

} } } }

#endif