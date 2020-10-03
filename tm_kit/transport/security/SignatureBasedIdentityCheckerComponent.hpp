#ifndef TM_KIT_TRANSPORT_SECURITY_SIGNATURE_BASED_IDENTITY_CHECKER_COMPONENT_HPP_
#define TM_KIT_TRANSPORT_SECURITY_SIGNATURE_BASED_IDENTITY_CHECKER_COMPONENT_HPP_

#include <tm_kit/transport/AbstractIdentityCheckerComponent.hpp>
#include <tm_kit/transport/security/SignatureHelper.hpp>

namespace dev { namespace cd606 { namespace tm {namespace transport {namespace security {
    template <class Req>
    class ClientSideSignatureBasedIdentityAttacherComponent 
        : public dev::cd606::tm::transport::ClientSideAbstractIdentityAttacherComponent<std::string, Req>
    {
    private:
        SignatureHelper::Signer signer_;
    public:
        ClientSideSignatureBasedIdentityAttacherComponent() : signer_() {}
        ClientSideSignatureBasedIdentityAttacherComponent(SignatureHelper::PrivateKey const &privateKey) : signer_(privateKey) {}
        ClientSideSignatureBasedIdentityAttacherComponent(ClientSideSignatureBasedIdentityAttacherComponent const &) = delete;
        ClientSideSignatureBasedIdentityAttacherComponent &operator=(ClientSideSignatureBasedIdentityAttacherComponent const &) = delete;
        ClientSideSignatureBasedIdentityAttacherComponent(ClientSideSignatureBasedIdentityAttacherComponent &&) = default;
        ClientSideSignatureBasedIdentityAttacherComponent &operator=(ClientSideSignatureBasedIdentityAttacherComponent &&) = default;
        ~ClientSideSignatureBasedIdentityAttacherComponent() = default;
        virtual dev::cd606::tm::basic::ByteData attach_identity(dev::cd606::tm::basic::ByteData &&d) override final {
            return signer_.sign(std::move(d));
        }
        virtual std::optional<basic::ByteData> process_incoming_data(basic::ByteData &&d) override final {
            return {std::move(d)};
        }
    };

    template <class Req>
    class ServerSideSignatureBasedIdentityCheckerComponent 
        : public dev::cd606::tm::transport::ServerSideAbstractIdentityCheckerComponent<std::string, Req>
    {
    private:
        SignatureHelper::Verifier verifier_;
    public:
        ServerSideSignatureBasedIdentityCheckerComponent() : verifier_() {}
        ServerSideSignatureBasedIdentityCheckerComponent(ServerSideSignatureBasedIdentityCheckerComponent const &) = delete;
        ServerSideSignatureBasedIdentityCheckerComponent &operator=(ServerSideSignatureBasedIdentityCheckerComponent const &) = delete;
        ServerSideSignatureBasedIdentityCheckerComponent(ServerSideSignatureBasedIdentityCheckerComponent &&) = default;
        ServerSideSignatureBasedIdentityCheckerComponent &operator=(ServerSideSignatureBasedIdentityCheckerComponent &&) = default;
        ~ServerSideSignatureBasedIdentityCheckerComponent() = default;
        void add_identity_and_key(std::string const &name, SignatureHelper::PublicKey const &publicKey) {
            verifier_.addKey(name, publicKey);
        }
        virtual std::optional<std::tuple<std::string,dev::cd606::tm::basic::ByteData>> check_identity(dev::cd606::tm::basic::ByteData &&d) override final {
            return verifier_.verify(std::move(d));
        }
        virtual basic::ByteData process_outgoing_data(std::string const &identity, basic::ByteData &&d) override final {
            return std::move(d);
        }
    };
}}}}}

#endif