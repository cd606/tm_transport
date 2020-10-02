#include <mutex>
#include <cstring>
#include <unordered_map>

#include <tm_kit/transport/security/SignatureHelper.hpp>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace security {
    class SignerImpl {
    private:
        std::array<unsigned char, crypto_sign_SECRETKEYBYTES> privateKey_;
    public:
        SignerImpl(std::array<unsigned char, crypto_sign_SECRETKEYBYTES> const &privateKey) : 
            privateKey_(privateKey) {}
        ~SignerImpl() {}
        basic::ByteData sign(basic::ByteData &&data) {       
            std::array<unsigned char, crypto_sign_BYTES> signature;
            crypto_sign_detached(
                signature.data()
                , 0
                , reinterpret_cast<unsigned char const *>(data.content.c_str())
                , data.content.length()
                , privateKey_.data()
            );
            std::tuple<basic::ByteData, basic::ByteData const *> t {basic::ByteData {std::string(reinterpret_cast<char const *>(signature.data()), signature.size())}, &data};
            auto res = basic::bytedata_utils::RunCBORSerializerWithNameList<
                std::tuple<basic::ByteData, basic::ByteData const *>
                , 2
            >::apply(t, {"signature", "data"});
            return basic::ByteData {std::string(reinterpret_cast<char const *>(res.data()), res.size())};
        }
    };
    
    SignatureHelper::Signer::Signer() : impl_() {}
    SignatureHelper::Signer::Signer(SignatureHelper::PrivateKey const &privateKey)
        : impl_(std::make_unique<SignerImpl>(privateKey))
        {}
    SignatureHelper::Signer::~Signer() {}
    SignatureHelper::Signer::Signer(SignatureHelper::Signer &&) = default;
    SignatureHelper::Signer &SignatureHelper::Signer::operator=(Signer &&) = default;
    basic::ByteData SignatureHelper::Signer::sign(basic::ByteData &&data) {
        if (impl_) {
            return impl_->sign(std::move(data));
        } else {
            return std::move(data);
        }
    }

    class VerifierImpl {
    private:
        std::unordered_map<std::string, std::array<unsigned char, crypto_sign_PUBLICKEYBYTES>> publicKeys_;
        std::mutex mutex_;
    public:
        VerifierImpl() : publicKeys_(), mutex_() {}
        ~VerifierImpl() {}
        void addKey(std::string const &name, std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> const &publicKey) {
            std::lock_guard<std::mutex> _(mutex_);
            publicKeys_.insert({name, publicKey});
        }
        std::optional<std::tuple<std::string,basic::ByteData>> verify(basic::ByteData &&data) {       
            auto res = basic::bytedata_utils::RunCBORDeserializerWithNameList<
                std::tuple<basic::ByteData, basic::ByteData>
                , 2
            >::apply(std::string_view {data.content}, 0, {"signature", "data"});
            if (!res) {
                return std::nullopt;
            }
            if (std::get<1>(*res) != data.content.length()) {
                return std::nullopt;
            }
            auto const &dataWithSignature = std::get<0>(*res);
            auto const &signature = std::get<0>(dataWithSignature);
            auto const &signedData = std::get<1>(dataWithSignature);
            if (signature.content.length() != crypto_sign_BYTES) {
                return std::nullopt;
            }

            bool result = false;
            
            auto const *p = reinterpret_cast<const unsigned char *>(signedData.content.c_str());
            auto const *q = reinterpret_cast<const unsigned char *>(signature.content.c_str());
            std::size_t l = signedData.content.length();
            std::string name;
            {
                std::lock_guard<std::mutex> _(mutex_);
                for (auto const &item : publicKeys_) {
                    result = 
                        result ||
                        (
                            crypto_sign_verify_detached(q, p, l, item.second.data()) == 0
                        );
                    if (result) {
                        name = item.first;
                        break;
                    }
                }
            }
            if (result) {
                return std::tuple<std::string, basic::ByteData> {std::move(name), std::move(signedData)};
            } else {
                return std::nullopt;
            }
        }
    };

    SignatureHelper::Verifier::Verifier() : impl_(std::make_unique<VerifierImpl>()) {}
    SignatureHelper::Verifier::~Verifier() {}
    SignatureHelper::Verifier::Verifier(Verifier &&) = default;
    SignatureHelper::Verifier &SignatureHelper::Verifier::operator=(Verifier &&) = default;
    void SignatureHelper::Verifier::addKey(std::string const &name, SignatureHelper::PublicKey const &publicKey) {
        impl_->addKey(name,publicKey);
    }
    void SignatureHelper::Verifier::addKeys(SignatureHelper::PublicKeyMap const &keys) {
        for (auto const &k : keys) {
            impl_->addKey(k.first, k.second);
        }
    }
    std::optional<std::tuple<std::string,basic::ByteData>> SignatureHelper::Verifier::verify(basic::ByteData &&data) {
        return impl_->verify(std::move(data));
    }
} } } } }







