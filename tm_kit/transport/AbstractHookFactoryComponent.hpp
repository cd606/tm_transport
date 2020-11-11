#ifndef TM_KIT_TRANSPORT_ABSTRACT_HOOK_FACTORY_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_ABSTRACT_HOOK_FACTORY_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/transport/ByteDataHook.hpp>
#include <type_traits>

namespace dev { namespace cd606 { namespace tm { namespace transport {
 
    template <class DataT>
    class AbstractOutgoingHookFactoryComponent {
    public:
        virtual ~AbstractOutgoingHookFactoryComponent() {}
        virtual UserToWireHook defaultHook() = 0;
    };
    template <class DataT>
    class AbstractIncomingHookFactoryComponent {
    public:
        virtual ~AbstractIncomingHookFactoryComponent() {}
        virtual WireToUserHook defaultHook() = 0;
    };

    template <class DataT>
    class TrivialOutgoingHookFactoryComponent : public AbstractOutgoingHookFactoryComponent<DataT> {
    public:
        virtual ~TrivialOutgoingHookFactoryComponent() {}
        virtual UserToWireHook defaultHook() override final {
            return {
                [](basic::ByteData &&d) -> basic::ByteData {
                    return std::move(d);
                }
            };
        }
    };
    template <class DataT>
    class TrivialIncomingHookFactoryComponent : public AbstractIncomingHookFactoryComponent<DataT> {
    public:
        virtual ~TrivialIncomingHookFactoryComponent() {}
        virtual WireToUserHook defaultHook() override final {
            return {
                [](basic::ByteDataView const &d) -> std::optional<basic::ByteData> {
                    return basic::ByteData {
                        std::string {d.content}
                    };
                }
            };
        }
    };

    template <class Env>
    class DefaultHookFactory {
    public:
        template <class DataT>
        static constexpr bool HasOutgoingHookFactory() {
            return std::is_convertible_v<Env *, AbstractOutgoingHookFactoryComponent<DataT> *>;
        }
        template <class DataT>
        static constexpr bool HasIncomingHookFactory() {
            return std::is_convertible_v<Env *, AbstractIncomingHookFactoryComponent<DataT> *>;
        }
        template <class DataT>
        static std::optional<UserToWireHook> outgoingHook(Env *env) {
            if constexpr (std::is_convertible_v<Env *, AbstractOutgoingHookFactoryComponent<DataT> *>) {
                return static_cast<AbstractOutgoingHookFactoryComponent<DataT> *>(env)->defaultHook();
            } else {
                return std::nullopt;
            }
        }
        template <class DataT>
        static std::optional<WireToUserHook> incomingHook(Env *env) {
            if constexpr (std::is_convertible_v<Env *, AbstractIncomingHookFactoryComponent<DataT> *>) {
                return static_cast<AbstractIncomingHookFactoryComponent<DataT> *>(env)->defaultHook();
            } else {
                return std::nullopt;
            }
        }

        template <class DataT>
        static std::optional<ByteDataHookPair> supplyFacilityHookPair_SingleType(Env *env, std::optional<ByteDataHookPair> hooks) {
            std::optional<UserToWireHook> userToWire = std::nullopt;
            if (hooks && hooks->userToWire) {
                userToWire = hooks->userToWire;
            } else {
                userToWire = DefaultHookFactory<Env>::template outgoingHook<DataT>(env);
            }
            std::optional<WireToUserHook> wireToUser = std::nullopt;
            if (hooks && hooks->wireToUser) {
                wireToUser = hooks->wireToUser;
            } else {
                wireToUser = DefaultHookFactory<Env>::template incomingHook<DataT>(env);
            }
            if (userToWire || wireToUser) {
                return ByteDataHookPair {userToWire, wireToUser};
            } else {
                return std::nullopt;
            }
        }

        template <class A, class B>
        static std::optional<ByteDataHookPair> supplyFacilityHookPair_ServerSide(Env *env, std::optional<ByteDataHookPair> hooks) {
            std::optional<UserToWireHook> userToWire = std::nullopt;
            if (hooks && hooks->userToWire) {
                userToWire = hooks->userToWire;
            } else {
                userToWire = DefaultHookFactory<Env>::template outgoingHook<B>(env);
            }
            std::optional<WireToUserHook> wireToUser = std::nullopt;
            if (hooks && hooks->wireToUser) {
                wireToUser = hooks->wireToUser;
            } else {
                wireToUser = DefaultHookFactory<Env>::template incomingHook<A>(env);
            }
            if (userToWire || wireToUser) {
                return ByteDataHookPair {userToWire, wireToUser};
            } else {
                return std::nullopt;
            }
        }
        template <class A, class B>
        static std::optional<ByteDataHookPair> supplyFacilityHookPair_ClientSide(Env *env, std::optional<ByteDataHookPair> hooks) {
            std::optional<UserToWireHook> userToWire = std::nullopt;
            if (hooks && hooks->userToWire) {
                userToWire = hooks->userToWire;
            } else {
                userToWire = DefaultHookFactory<Env>::template outgoingHook<A>(env);
            }
            std::optional<WireToUserHook> wireToUser = std::nullopt;
            if (hooks && hooks->wireToUser) {
                wireToUser = hooks->wireToUser;
            } else {
                wireToUser = DefaultHookFactory<Env>::template incomingHook<B>(env);
            }
            if (userToWire || wireToUser) {
                return ByteDataHookPair {userToWire, wireToUser};
            } else {
                return std::nullopt;
            }
        }
        template <class A>
        static std::optional<ByteDataHookPair> supplyFacilityHookPair_ClientSideOutgoingOnly(Env *env, std::optional<ByteDataHookPair> hooks) {
            std::optional<UserToWireHook> userToWire = std::nullopt;
            if (hooks && hooks->userToWire) {
                userToWire = hooks->userToWire;
            } else {
                userToWire = DefaultHookFactory<Env>::template outgoingHook<A>(env);
            }
            std::optional<WireToUserHook> wireToUser = std::nullopt;
            if (hooks) {
                wireToUser = hooks->wireToUser;
            }
            if (userToWire || wireToUser) {
                return ByteDataHookPair {userToWire, wireToUser};
            } else {
                return std::nullopt;
            }
        }
    };

} } } }

#endif