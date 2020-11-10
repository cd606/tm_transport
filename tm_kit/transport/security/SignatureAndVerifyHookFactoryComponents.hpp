#ifndef TM_KIT_TRANSPORT_SECURITY_SIGNATURE_AND_VERIFY_HOOK_FACTORY_COMPONENTS_HPP_
#define TM_KIT_TRANSPORT_SECURITY_SIGNATURE_AND_VERIFY_HOOK_FACTORY_COMPONENTS_HPP_

#include <tm_kit/transport/AbstractHookFactoryComponent.hpp>
#include <tm_kit/transport/security/SignatureHelper.hpp>
#include <unordered_map>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace security {

template <class T>
class SignatureHookFactoryComponent : public AbstractOutgoingHookFactoryComponent<T> {
private:
    SignatureHelper::PrivateKey signKey_;
public:
    SignatureHookFactoryComponent() : signKey_() {}
    SignatureHookFactoryComponent(SignatureHelper::PrivateKey const &signKey)
        : signKey_(signKey) {}
    virtual ~SignatureHookFactoryComponent() {}
    virtual UserToWireHook defaultHook() override final {
        auto signer = std::make_shared<SignatureHelper::Signer>(signKey_);
        return UserToWireHook { 
            [signer](basic::ByteData &&d) {
                return signer->sign(std::move(d));
            }
        };
    }
};

template <class T>
class VerifyHookFactoryComponent : public AbstractIncomingHookFactoryComponent<T> {
private:
    SignatureHelper::PublicKeyMap verifyKeys_;
public:
    VerifyHookFactoryComponent() : verifyKeys_() {}
    VerifyHookFactoryComponent(SignatureHelper::PublicKeyMap const &verifyKeys)
        : verifyKeys_(verifyKeys) {}
    virtual ~VerifyHookFactoryComponent() {}
    virtual WireToUserHook defaultHook() override final {
        auto verifier = std::make_shared<SignatureHelper::Verifier>();
        for (auto const &k : verifyKeys_) {
            verifier->addKey(k.first, k.second);
        }
        return WireToUserHook { 
            [verifier](basic::ByteDataView const &d) -> std::optional<basic::ByteData> {
                auto res = verifier->verify(d);
                if (res) {
                    return std::move(std::get<1>(*res));
                } else {
                    return std::nullopt;
                }
            } 
        };
    }
};

} } } } }

#endif