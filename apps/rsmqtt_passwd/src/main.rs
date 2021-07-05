#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

use passwd_util::HashType;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Options {
    /// hash type (argon2d, argon2i, argon2id, pbkdf2-sha256, pbkdf2-sha512, scrypt)
    hash: HashType,

    /// password
    password: String,
}

fn main() {
    let options: Options = Options::from_args();
    println!("{}", options.hash.create_phc(options.password));
}
