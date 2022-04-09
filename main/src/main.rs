use lib::{erc721_mint_act, bear_mint_act, bee_mint_act, hive_mint_act};
use conf::{parse_args, get_config, init_logger};

use log::info;
use clap::ArgMatches;
use std::collections::BTreeMap;

fn usage() {
    println!("Usage: cargo run -- "); 
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logger();
    info!("main|starting");

    let cli_args: ArgMatches = parse_args();
    let config_name = cli_args.value_of("conf").expect("ERR: cli [configuration] is invalid"); 
    let config: BTreeMap<String, String> = get_config(config_name); 
    let cmd = String::from(cli_args.value_of("cmd").expect("ERR: cli [cmd] is invalid"));

    match cmd.as_str() {
        "erc721_mint_act" => { erc721_mint_act(&config, cli_args,).await?; },
        "migration" => { bear_mint_act(&config, cli_args,).await?; },
        "bee_mint_act" => { bee_mint_act(&config, cli_args,).await?; }, 
        "hive_mint_act" => { hive_mint_act(&config, cli_args,).await?; },
        _ => { 
            usage(); 
            std::process::exit(1);
        },
    }

    info!("main|completed");
    Ok(())        
}
