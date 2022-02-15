use chrono::{Utc, NaiveDateTime, DateTime, Duration};
use std::fs::File;
use std::io::Read;
use std::env;
use std::collections::{BTreeMap, HashMap};
use std::process;
use log::{info, LevelFilter};
use log4rs::{
    append::{console::{ConsoleAppender, Target},
             file::FileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};

use reqwest::{Client, Url};
use clap::{Arg, App, ArgMatches};
use serde::{Serialize, Deserialize};
use serde_json::Value;

use egg_mode::media::{upload_media, media_types};
use egg_mode::tweet::DraftTweet;

use polars::datatypes::DataType::*;
use polars::prelude::{NamedFrom, BooleanChunked, IntoSeries, ChunkedBuilder, ChunkFilter};
use polars::datatypes::{Int32Chunked, UInt32Chunked, UInt32Type, Utf8Type};
use polars::series::Series;
use polars::frame::DataFrame;
use polars::chunked_array::ChunkedArray;
use polars::chunked_array::builder::BooleanChunkedBuilder;

type ResponseMap = HashMap<String, Vec<serde_json::Value>>;

fn usage() {
    println!("Usage: cargo run -- -c [config] -a [cmd]"); 
}

fn init_logger() {
    let level = log::LevelFilter::Info;
    let file_path = "/tmp/bkeeper.log";

    let stderr = ConsoleAppender::builder().target(Target::Stderr).build();
    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{h({d(%m-%d-%Y %H:%M:%S)})}|{m}{n}")))
        .build(file_path)
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .appender(Appender::builder()
                  .filter(Box::new(ThresholdFilter::new(level)))
                  .build("stderr", Box::new(stderr)),)
        .build(Root::builder()
               .appender("logfile")
               .appender("stderr")
               .build(LevelFilter::Trace),
        )
        .unwrap();

    let _handle = log4rs::init_config(config);
}

// utility fn to parse custom cli args
fn parse_args() -> clap::ArgMatches {
    info!("parse_args|starting");

    let cli_args = App::new("boot")
        .args(&[
            Arg::new("conf")
                .long("config")
                .short('c')
                .takes_value(true)
                .required(true),
            Arg::new("cmd")
                .long("command")
                .short('a')
                .takes_value(true)
                .required(true),
            Arg::new("help")
                .long("help")
                .short('h'),])
        .get_matches();

    info!("parse_args|completed");
    cli_args
}

// utility fn to read and parse configuration.yaml 
fn get_config(config_name: &str) -> BTreeMap<String, String> {
    info!("get_config|starting");

    let mut yaml_config = File::open(config_name.to_string()).expect(&format!("ERR: {} cannot be opened", config_name)); 
    
    let mut file_data = String::new();
    yaml_config.read_to_string(&mut file_data).expect(&format!("ERR: yaml_config cannot be read"));
    
    let conf: BTreeMap<String, String> = serde_yaml::from_str(&file_data).expect(&format!("ERR: serde_yaml parse failed. conf creation aborted..."));

    info!("get_config|configuration: {}", config_name);
    info!("get_config|completed");
    conf
}

// leverage public coingecko api to retrieve + process historical market data
async fn cg_historical_days(token_id: &str, days: i32) -> Result<ResponseMap, Box<dyn std::error::Error>> {
    info!("cg_historical_days|starting");

    let url: String = format!("https://api.coingecko.com/api/v3/coins/{}/market_chart?vs_currency=usd&days={}&interval=daily", token_id, days);

    //info!("cg_historical_days|{}", &url);

    let response = reqwest::get(url)
        .await?
        .text()
        .await?;

    //info!("cg_historical_days|{:?}", response);
    let contents: ResponseMap = serde_json::from_str(&response)?;
    
    info!("cg_historical_days|completed");
    Ok(contents)
}

fn to_date(ts: &str) -> String {
    info!("to_date|starting");
    
    //println!("{}", ts);

    let ts_sec = ts.parse::<i64>().unwrap();
    //let ts_sec = (tmp / 1000); 
    
    let naive = NaiveDateTime::from_timestamp(ts_sec, 0); 
    let datetime = DateTime::<Utc>::from_utc(naive, Utc);
    let dt = datetime.format("%Y-%m-%d"); // %Y-%m-%d %H:%M:%S
    //info!("to_date|dt={}", dt.to_string());

    info!("to_date|completed");
    dt.to_string()
}

fn percent_diff(v1: f64, v2: f64) -> f64 {
    ((v1 - v2) / ((v1 + v2) /  2.0)) * 100.0
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logger();
    info!("main|starting");

    let cli_args: clap::ArgMatches = parse_args();
    let config_name = cli_args.value_of("conf").expect("ERR: cli [configuration] is invalid").to_string(); 
    let config: BTreeMap<String, String> = get_config(&config_name); 

    let con_key = String::from(config.get("con_key").unwrap());
    let con_secret = String::from(config.get("con_secret").unwrap());
    let access_key = String::from(config.get("acc_key").unwrap());
    let access_secret = String::from(config.get("acc_secret").unwrap());
    let connect_token = egg_mode::KeyPair::new(con_key, con_secret); 
    let access_token = egg_mode::KeyPair::new(access_key, access_secret);
    let token = egg_mode::Token::Access {consumer: connect_token, access: access_token,};

    let cmd = cli_args.value_of("cmd").expect("ERR: cli [cmd] is invalid").to_string();

    if cmd.eq_ignore_ascii_case("honeyd_stat") {
        info!("main|honeyd_stat starting");

        info!("main|honeyd_stat completed");
    } else if cmd.eq_ignore_ascii_case("honeyd_mint_act") {
        info!("main|honeyd_mint_act starting");

        info!("main|honeyd_mint_act completed");
    } else {
        println!("ELSE ... ");
    } 

    info!("main|completed");
    Ok(())        
}
