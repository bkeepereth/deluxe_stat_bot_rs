use std::fs::File;
use std::io::Read;
use std::collections::{BTreeMap};

use log::{info, LevelFilter};
use log4rs::{
    append::{console::{ConsoleAppender, Target},
             file::FileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};

use clap::{Arg, App};

pub fn init_logger() {
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

/// utility fn to parse custom cli args
pub fn parse_args() -> clap::ArgMatches {
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
                .short('i')
                .takes_value(true)
                .required(true),
            Arg::new("addr")
                .long("address")
                .short('a')
                .takes_value(true)
                .required(false),
            Arg::new("lookback")
                .long("lookback_days")
                .short('l')
                .takes_value(true)
                .required(false),
            Arg::new("post")
                .long("post")
                .short('p')
                .takes_value(true)
                .required(false),
            Arg::new("help")
                .long("help")
                .short('h'),])
        .get_matches();

    info!("parse_args|completed");
    cli_args
}

/// Utility fn to read and parse configuration.yaml
pub fn get_config(config_name: &str) -> BTreeMap<String, String> {
    info!("get_config|starting");

    let mut yaml_config = File::open(String::from(config_name)).expect(&format!("ERR: {} cannot be opened", config_name));

    let mut file_data = String::new();
    yaml_config.read_to_string(&mut file_data).expect(&format!("ERR: yaml_config cannot be read"));

    let conf: BTreeMap<String, String> = serde_yaml::from_str(&file_data).expect(&format!("ERR: serde_yaml parse failed. conf creation aborted..."));

    info!("get_config|configuration: {}", config_name);
    info!("get_config|completed");
    conf
}
