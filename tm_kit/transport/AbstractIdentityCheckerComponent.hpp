#ifndef TM_KIT_TRANSPORT_ABSTRACT_IDENTITY_CHECKER_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_ABSTRACT_IDENTITY_CHECKER_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>
#include <tm_kit/basic/WrapFacilitioidConnectorForSerialization.hpp>
#include <type_traits>

namespace dev { namespace cd606 { namespace tm { namespace transport {
    //The two "base" classes are only used for template purposes,
    //therefore they are not RTTI classes
    
    template <class Request>
    class ClientSideAbstractIdentityAttacherComponentBase {};

    template <class Identity, class Request>
    class ClientSideAbstractIdentityAttacherComponent 
        : public virtual ClientSideAbstractIdentityAttacherComponentBase<Request> {
    public:
        virtual basic::ByteData attach_identity(basic::ByteData &&d) = 0;
        virtual std::optional<basic::ByteData> process_incoming_data(basic::ByteData &&d) = 0;
        virtual ~ClientSideAbstractIdentityAttacherComponent() {}
    };

    template <class Request>
    class ServerSideAbstractIdentityCheckerComponentBase {};

    template <class Identity, class Request>
    class ServerSideAbstractIdentityCheckerComponent 
        : public virtual ServerSideAbstractIdentityCheckerComponentBase<Request> {
    public:
        virtual std::optional<std::tuple<Identity, basic::ByteData>> check_identity(basic::ByteData &&d) = 0;
        virtual basic::ByteData process_outgoing_data(Identity const &identity, basic::ByteData &&d) = 0;
        virtual ~ServerSideAbstractIdentityCheckerComponent() {}
    };

    template <class Env, class Request, bool Check=std::is_convertible_v<Env *, ClientSideAbstractIdentityAttacherComponentBase<Request> *>, bool Check2=std::is_convertible_v<Env *, ClientSideAbstractIdentityAttacherComponentBase<typename basic::WrapFacilitioidConnectorForSerializationHelpers::UnwrappedType<Request>> *>, bool Check3=std::is_convertible_v<Env *, ClientSideAbstractIdentityAttacherComponentBase<std::any> *>>
    class DetermineClientSideIdentityForRequest {};
    template <class Env, class Request, bool Check2, bool Check3>
    class DetermineClientSideIdentityForRequest<Env, Request, true, Check2, Check3> {
    private:
        class InnerC {
        public:
            template <class Identity> static constexpr Identity *f(
                ClientSideAbstractIdentityAttacherComponent<Identity, Request> *
            ) {
                return (Identity *) nullptr;
            }
        };
    public:
        using IdentityType =
            std::remove_pointer_t<decltype(InnerC::f((Env *) nullptr))>;
        using FullRequestType =
            std::tuple<IdentityType, Request>;
        static constexpr bool HasIdentity = true;
        using ComponentType = ClientSideAbstractIdentityAttacherComponent<IdentityType, Request>;
    };
    template <class Env, class Request, bool Check3>
    class DetermineClientSideIdentityForRequest<Env, Request, false, true, Check3> {
    private:
        class InnerC {
        public:
            template <class Identity> static constexpr Identity *f(
                ClientSideAbstractIdentityAttacherComponent<Identity, typename basic::WrapFacilitioidConnectorForSerializationHelpers::UnwrappedType<Request>> *
            ) {
                return (Identity *) nullptr;
            }
        };
    public:
        using IdentityType =
            std::remove_pointer_t<decltype(InnerC::f((Env *) nullptr))>;
        using FullRequestType =
            std::tuple<IdentityType, Request>;
        static constexpr bool HasIdentity = true;
        using ComponentType = ClientSideAbstractIdentityAttacherComponent<IdentityType, typename basic::WrapFacilitioidConnectorForSerializationHelpers::UnwrappedType<Request>>;
    };
    template <class Env, class Request>
    class DetermineClientSideIdentityForRequest<Env, Request, false, false, true> {
    private:
        class InnerC {
        public:
            template <class Identity> static constexpr Identity *f(
                ClientSideAbstractIdentityAttacherComponent<Identity, std::any> *
            ) {
                return (Identity *) nullptr;
            }
        };
    public:
        using IdentityType =
            std::remove_pointer_t<decltype(InnerC::f((Env *) nullptr))>;
        using FullRequestType =
            std::tuple<IdentityType, Request>;
        static constexpr bool HasIdentity = true;
        using ComponentType = ClientSideAbstractIdentityAttacherComponent<IdentityType, std::any>;
    };
    template <class Env, class Request>
    class DetermineClientSideIdentityForRequest<Env, Request, false, false, false> {
    public:
        using IdentityType = void;
        using FullRequestType = Request;
        static constexpr bool HasIdentity = false;
        using ComponentType = void;
    };
    template <class Env, class Request, bool Check=std::is_convertible_v<Env *, ServerSideAbstractIdentityCheckerComponentBase<Request> *>, bool Check2=std::is_convertible_v<Env *, ServerSideAbstractIdentityCheckerComponentBase<typename basic::WrapFacilitioidConnectorForSerializationHelpers::UnwrappedType<Request>> *>>
    class DetermineServerSideIdentityForRequest {};
    template <class Env, class Request, bool Check2>
    class DetermineServerSideIdentityForRequest<Env, Request, true, Check2> {
    private:
        class InnerC {
        public:
            template <class Identity> static constexpr Identity *f(
                ServerSideAbstractIdentityCheckerComponent<Identity, Request> *
            ) {
                return (Identity *) nullptr;
            }
        };
    public:
        using IdentityType =
            std::remove_pointer_t<decltype(InnerC::f((Env *) nullptr))>;
        using FullRequestType =
            std::tuple<IdentityType, Request>;
        static constexpr bool HasIdentity = true;
        using ComponentType = ServerSideAbstractIdentityCheckerComponent<IdentityType, Request>;
    };
    template <class Env, class Request>
    class DetermineServerSideIdentityForRequest<Env, Request, false, true> {
    private:
        class InnerC {
        public:
            template <class Identity> static constexpr Identity *f(
                ServerSideAbstractIdentityCheckerComponent<Identity, typename basic::WrapFacilitioidConnectorForSerializationHelpers::UnwrappedType<Request>> *
            ) {
                return (Identity *) nullptr;
            }
        };
    public:
        using IdentityType =
            std::remove_pointer_t<decltype(InnerC::f((Env *) nullptr))>;
        using FullRequestType =
            std::tuple<IdentityType, Request>;
        static constexpr bool HasIdentity = true;
        using ComponentType = ServerSideAbstractIdentityCheckerComponent<IdentityType, typename basic::WrapFacilitioidConnectorForSerializationHelpers::UnwrappedType<Request>>;
    };
    template <class Env, class Request>
    class DetermineServerSideIdentityForRequest<Env, Request, false, false> {
    public:
        using IdentityType = void;
        using FullRequestType = Request;
        static constexpr bool HasIdentity = false;
        using ComponentType = void;
    };

} } } }

#endif