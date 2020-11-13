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
    virtual std::optional<UserToWireHook> defaultHook() override final {
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
    virtual std::optional<WireToUserHook> defaultHook() override final {
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

template <class T>
class SignatureWithNameHookFactoryComponent : public AbstractOutgoingHookFactoryComponent<T> {
private:
    std::string name_;
    SignatureHelper::PrivateKey signKey_;
public:
    SignatureWithNameHookFactoryComponent() : name_(), signKey_() {}
    SignatureWithNameHookFactoryComponent(std::string const &name, SignatureHelper::PrivateKey const &signKey)
        : name_(name), signKey_(signKey) {}
    virtual ~SignatureWithNameHookFactoryComponent() {}
    virtual std::optional<UserToWireHook> defaultHook() override final {
        auto signer = std::make_shared<SignatureHelper::Signer>(signKey_);
        return UserToWireHook { 
            [this,signer](basic::ByteData &&d) {
                return signer->signWithName(name_, std::move(d));
            }
        };
    }
};

template <class T>
class VerifyUsingNameTagHookFactoryComponent : public AbstractIncomingHookFactoryComponent<T> {
private:
    SignatureHelper::PublicKeyMap verifyKeys_;
public:
    VerifyUsingNameTagHookFactoryComponent() : verifyKeys_() {}
    VerifyUsingNameTagHookFactoryComponent(SignatureHelper::PublicKeyMap const &verifyKeys)
        : verifyKeys_(verifyKeys) {}
    virtual ~VerifyUsingNameTagHookFactoryComponent() {}
    virtual std::optional<WireToUserHook> defaultHook() override final {
        auto verifier = std::make_shared<SignatureHelper::Verifier>();
        for (auto const &k : verifyKeys_) {
            verifier->addKey(k.first, k.second);
        }
        return WireToUserHook { 
            [verifier](basic::ByteDataView const &d) -> std::optional<basic::ByteData> {
                return verifier->verifyDataTaggedWithName(d);
            } 
        };
    }
};

} } } } }

#endif