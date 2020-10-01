#ifndef TM_KIT_TRANSPORT_ABSTRACT_IDENTITY_CHECKER_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_ABSTRACT_IDENTITY_CHECKER_COMPONENT_HPP_

#include <tm_kit/basic/ByteData.hpp>
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
        virtual ~ClientSideAbstractIdentityAttacherComponent() {}
    };

    template <class Request>
    class ServerSideAbstractIdentityCheckerComponentBase {};

    template <class Identity, class Request>
    class ServerSideAbstractIdentityCheckerComponent 
        : public virtual ServerSideAbstractIdentityCheckerComponentBase<Request> {
    public:
        virtual std::optional<std::tuple<Identity, basic::ByteData>> check_identity(basic::ByteData &&d) = 0;
        virtual ~ServerSideAbstractIdentityCheckerComponent() {}
    };

    template <class Env, class Request, bool Check=std::is_convertible_v<Env *, ClientSideAbstractIdentityAttacherComponentBase<Request> *>>
    class DetermineClientSideIdentityForRequest {};
    template <class Env, class Request>
    class DetermineClientSideIdentityForRequest<Env, Request, true> {
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
    };
    template <class Env, class Request>
    class DetermineClientSideIdentityForRequest<Env, Request, false> {
    public:
        using IdentityType = void;
        using FullRequestType = Request;
        static constexpr bool HasIdentity = false;
    };
    template <class Env, class Request, bool Check=std::is_convertible_v<Env *, ServerSideAbstractIdentityCheckerComponentBase<Request> *>>
    class DetermineServerSideIdentityForRequest {};
    template <class Env, class Request>
    class DetermineServerSideIdentityForRequest<Env, Request, true> {
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
    };
    template <class Env, class Request>
    class DetermineServerSideIdentityForRequest<Env, Request, false> {
    public:
        using IdentityType = void;
        using FullRequestType = Request;
        static constexpr bool HasIdentity = false;
    };

} } } }

#endif