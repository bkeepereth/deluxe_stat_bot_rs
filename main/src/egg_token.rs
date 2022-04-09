mod egg_token;

use egg_mode::Token;
use egg_mode::KeyPair;

pub struct EggToken {
    token: egg_mode::Token,
}

impl EggToken {
    pub fn new(con_key: String, con_secret: String, access_key: String, access_secret: String) -> EggToken {
        let connect_token = egg_mode::KeyPair::new(con_key, con_secret);
        let access_token = egg_mode::KeyPair::new(access_key, access_secret);

        EggToken { token: egg_mode::Token::Access {consumer: connect_token, access: access_token, } }
    }
}
