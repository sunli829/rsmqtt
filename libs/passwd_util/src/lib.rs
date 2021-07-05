#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use anyhow::Error;
use argon2::Argon2;
use password_hash::{Ident, PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use pbkdf2::Pbkdf2;
use rand_core::OsRng;
use scrypt::Scrypt;
use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum HashType {
    #[serde(rename = "argon2d")]
    Argon2d,

    #[serde(rename = "argon2i")]
    Argon2i,

    #[serde(rename = "argon2id")]
    Argon2id,

    #[serde(rename = "pbkdf2-sha256")]
    Pbkdf2Sha256,

    #[serde(rename = "pbkdf2-sha512")]
    Pbkdf2Sha512,

    #[serde(rename = "scrypt")]
    Scrypt,
}

impl FromStr for HashType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use HashType::*;

        let ty = match s {
            "argon2d" => Argon2d,
            "argon2i" => Argon2i,
            "argon2id" => Argon2id,
            "pbkdf2-sha256" => Pbkdf2Sha256,
            "pbkdf2-sha512" => Pbkdf2Sha512,
            "scrypt" => Scrypt,
            _ => anyhow::bail!("unknown hash type: {}", s),
        };
        Ok(ty)
    }
}

impl Display for HashType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use HashType::*;

        match self {
            Argon2d => write!(f, "argon2d"),
            Argon2i => write!(f, "argon2i"),
            Argon2id => write!(f, "argon2id"),
            Pbkdf2Sha256 => write!(f, "pbkdf2-sha256"),
            Pbkdf2Sha512 => write!(f, "pbkdf2-sha512"),
            Scrypt => write!(f, "scrypt"),
        }
    }
}

impl HashType {
    pub fn create_phc(&self, password: impl AsRef<[u8]>) -> String {
        let salt = SaltString::generate(&mut OsRng);

        match self {
            HashType::Argon2d => Argon2::default()
                .hash_password(
                    password.as_ref(),
                    Some(argon2::Algorithm::Argon2d.ident()),
                    argon2::Params::default(),
                    salt.as_salt(),
                )
                .unwrap()
                .to_string(),
            HashType::Argon2i => Argon2::default()
                .hash_password(
                    password.as_ref(),
                    Some(argon2::Algorithm::Argon2i.ident()),
                    argon2::Params::default(),
                    salt.as_salt(),
                )
                .unwrap()
                .to_string(),
            HashType::Argon2id => Argon2::default()
                .hash_password(
                    password.as_ref(),
                    Some(argon2::Algorithm::Argon2id.ident()),
                    argon2::Params::default(),
                    salt.as_salt(),
                )
                .unwrap()
                .to_string(),
            HashType::Pbkdf2Sha256 => Pbkdf2
                .hash_password(
                    password.as_ref(),
                    Some(pbkdf2::Algorithm::Pbkdf2Sha256.ident()),
                    pbkdf2::Params::default(),
                    salt.as_salt(),
                )
                .unwrap()
                .to_string(),
            HashType::Pbkdf2Sha512 => Pbkdf2
                .hash_password(
                    password.as_ref(),
                    Some(pbkdf2::Algorithm::Pbkdf2Sha512.ident()),
                    pbkdf2::Params::default(),
                    salt.as_salt(),
                )
                .unwrap()
                .to_string(),
            HashType::Scrypt => Scrypt
                .hash_password_simple(password.as_ref(), salt.as_ref())
                .unwrap()
                .to_string(),
        }
    }
}

pub fn verify_password(phc: impl AsRef<str>, password: impl AsRef<[u8]>) -> bool {
    let parsed_hash = match PasswordHash::new(phc.as_ref()) {
        Ok(parsed_hash) => parsed_hash,
        Err(_) => return false,
    };

    const PBKDF2_SHA256: Ident = Ident::new("pbkdf2-sha256");
    const PBKDF2_SHA512: Ident = Ident::new("pbkdf2-sha512");

    match parsed_hash.algorithm {
        argon2::ARGON2I_IDENT | argon2::ARGON2D_IDENT | argon2::ARGON2ID_IDENT => Argon2::default()
            .verify_password(password.as_ref(), &parsed_hash)
            .is_ok(),
        PBKDF2_SHA256 | PBKDF2_SHA512 => Pbkdf2
            .verify_password(password.as_ref(), &parsed_hash)
            .is_ok(),
        scrypt::ALG_ID => Scrypt
            .verify_password(password.as_ref(), &parsed_hash)
            .is_ok(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash() {
        let types = [
            HashType::Argon2d,
            HashType::Argon2i,
            HashType::Argon2id,
            HashType::Pbkdf2Sha256,
            HashType::Pbkdf2Sha512,
            HashType::Scrypt,
        ];

        for hash_type in types {
            let password = "123456";
            let phc = hash_type.create_phc(password);
            assert!(verify_password(&phc, password));
            assert!(!verify_password(&phc, "abcdef"));
        }
    }
}
