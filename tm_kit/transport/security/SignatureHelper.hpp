#ifndef TM_KIT_TRANSPORT_SECURITY_SIGNATURE_HELPER_HPP_
#define TM_KIT_TRANSPORT_SECURITY_SIGNATURE_HELPER_HPP_

#include <array>
#include <memory>
#include <cstddef>
#include <string>
#include <tuple>
#include <optional>
#include <tm_kit/basic/ByteData.hpp>

#include <sodium/crypto_sign.h>

namespace dev { namespace cd606 { namespace tm { namespace transport { namespace security {
    class SignerImpl;
    class VerifierImpl;

    class SignatureHelper {
    public:
        static constexpr std::size_t PublicKeyLength = crypto_sign_PUBLICKEYBYTES;
        static constexpr std::size_t PrivateKeyLength = crypto_sign_SECRETKEYBYTES;

        using PublicKey = std::array<unsigned char, PublicKeyLength>;
        using PrivateKey = std::array<unsigned char, PrivateKeyLength>;

        class Signer {
        private:
            std::unique_ptr<SignerImpl> impl_;
        public:
            Signer();
            Signer(std::string const &name, PrivateKey const &privateKey);
            ~Signer();
            Signer(Signer const &) = delete;
            Signer &operator=(Signer const &) = delete;
            Signer(Signer &&);
            Signer &operator=(Signer &&);
            basic::ByteData sign(basic::ByteData &&);
        };

        class Verifier {
        private:
            std::unique_ptr<VerifierImpl> impl_;
        public:
            Verifier();
            ~Verifier();
            Verifier(Verifier const &) = delete;
            Verifier &operator=(Verifier const &) = delete;
            Verifier(Verifier &&);
            Verifier &operator=(Verifier &&);
            void addKey(std::string const &name, PublicKey const &publicKey);
            std::optional<std::tuple<std::string,basic::ByteData>> verify(basic::ByteData &&);
        };
    };
} } } } } 

#endif