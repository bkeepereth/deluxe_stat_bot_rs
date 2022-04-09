use std::collections::{BTreeMap, HashMap};
use std::thread::sleep;
use std::str::FromStr;
use std::io::{stdout, Write};
use std::time::Duration;
use log::info;

use polars::datatypes::DataType::*;
use polars::prelude::{NamedFrom, BooleanChunked, IntoSeries, ChunkApply};
use polars::datatypes::TimeUnit::Milliseconds;
use polars::series::Series;
use polars::frame::DataFrame;

use web3::contract::{Contract, Options};
use web3::types::{Address, U256};

use plotly::common::Title;
use plotly::layout::{Axis, BarMode, Layout, Legend, TicksDirection};
use plotly::{Bar, Plot};

use egg_mode::{KeyPair, Token};
use egg_mode::media::{media_types, upload_media, get_status, ProgressInfo};
use egg_mode::tweet::DraftTweet;

type ResponseMap = HashMap<String, Vec<serde_json::Value>>;

pub struct EggToken {
    token: egg_mode::Token,
}

impl EggToken {
    pub fn new(con_key: String, con_secret: String, access_key: String, access_secret: String) -> EggToken {
        let connect_token = KeyPair::new(con_key, con_secret);
        let access_token = KeyPair::new(access_key, access_secret);

        EggToken { token: Token::Access {consumer: connect_token, access: access_token, } }
    }
}

/// Utility function that retrieves a list of ERC721 transfers using the Etherscan API 
async fn get_erc721_transfers(contract_addr: &str, es_key: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    info!("get_erc721_transfers|starting");

    let mut token_dec_vec: Vec<String> = vec![];
    let mut txn_index_vec: Vec<String> = vec![];
    let mut nonce_vec: Vec<String> = vec![];
    let mut token_id_vec: Vec<String> = vec![];

    let mut gas_vec: Vec<String> = vec![];
    let mut gas_price_vec: Vec<String> = vec![];
    let mut gas_used_vec: Vec<String> = vec![];
    let mut cum_gas_used_vec: Vec<String> = vec![];
    let mut confirms_vec: Vec<String> = vec![];
    let mut block_num_vec: Vec<String> = vec![];
    let mut ts_vec: Vec<String> = vec![];

    let mut hash_vec: Vec<String> = vec![];
    let mut block_hash_vec: Vec<String> = vec![];
    let mut contract_addr_vec: Vec<String> = vec![];
    let mut to_addr_vec: Vec<String> = vec![];
    let mut from_addr_vec: Vec<String> = vec![];
    let mut token_nm_vec: Vec<String> = vec![];
    let mut token_sym_vec: Vec<String> = vec![];

    let mut start_block = 0;
    let mut cur_block;
    let mut flag = true;

    while flag {
        let url = format!("https://api.etherscan.io/api?module=account&action=tokennfttx&contractaddress={contract_addr}&startblock={start_block}&sort=asc&apikey={api_key}",
                              contract_addr = contract_addr,
                              start_block = start_block,
                              api_key = es_key);
        
        let response = reqwest::get(url)
            .await?
            .text()
            .await?;

        let tmp: serde_json::Value = serde_json::from_str(&response)?;

        let status = &tmp["status"];
        let message = &tmp["message"];

        info!("get_erc721_transfers|status={}", status);
        info!("get_erc721_transfers|message={}", message);

        let transfers = match tmp["result"].as_array() {
            Some(x) => x,
            _ => panic!("error: unable to parse result"),
        };

        cur_block = match transfers.get(0) {
            Some(x) => i32::from_str(x["blockNumber"].as_str().unwrap()).unwrap(),
            None => 0,
        };

        for transfer in transfers {
                let block_num = String::from(transfer["blockNumber"].as_str().unwrap());
                let ts = String::from(transfer["timeStamp"].as_str().expect("error: timeStamp is invalid"));
                let hash = String::from(transfer["hash"].as_str().expect("error: hash is invalid"));
                let nonce = String::from(transfer["nonce"].as_str().expect("error: nonce is invalid"));
                let block_hash = String::from(transfer["blockHash"].as_str().expect("error: blockHash is invalid"));
                let con_addr = String::from(transfer["contractAddress"].as_str().expect("error: contractAddress is invalid"));
                let to_addr = String::from(transfer["to"].as_str().expect("error: to is invalid"));
                let from_addr = String::from(transfer["from"].as_str().expect("error: from is invalid"));
                let token_id = String::from(transfer["tokenID"].as_str().expect("error: tokenID is invalid"));
                let token_nm = String::from(transfer["tokenName"].as_str().expect("error: tokenName is invalid"));
                let token_sym = String::from(transfer["tokenSymbol"].as_str().expect("error: tokenSymbol is invalid"));
                let token_dec = String::from(transfer["tokenDecimal"].as_str().expect("error: tokenDecimal is invalid"));
                let txn_index = String::from(transfer["transactionIndex"].as_str().expect("error: transactionIndex is invalid"));
                let gas = String::from(transfer["gas"].as_str().expect("error: gas is invalid"));
                let gas_price = String::from(transfer["gasPrice"].as_str().expect("error: gasPrice is invalid"));
                let gas_used = String::from(transfer["gasUsed"].as_str().expect("error: gasUsed is invalid"));
                let cum_gas_used = String::from(transfer["cumulativeGasUsed"].as_str().expect("error: cumulativeGasUsed is invalid"));
                let confirms = String::from(transfer["confirmations"].as_str().expect("error: confirmations is invalid"));

                block_num_vec.push(block_num);
                ts_vec.push(ts);
                hash_vec.push(hash);
                nonce_vec.push(nonce);
                block_hash_vec.push(block_hash);
                contract_addr_vec.push(con_addr);
                to_addr_vec.push(to_addr);
                from_addr_vec.push(from_addr);
                token_id_vec.push(token_id);
                token_nm_vec.push(token_nm);
                token_sym_vec.push(token_sym);
                token_dec_vec.push(token_dec);
                txn_index_vec.push(txn_index);
                gas_vec.push(gas);
                gas_price_vec.push(gas_price);
                gas_used_vec.push(gas_used);
                cum_gas_used_vec.push(cum_gas_used);
                confirms_vec.push(confirms);
        }

        cur_block = i32::from_str(&block_num_vec[block_num_vec.len()-1]).unwrap();

        if start_block == cur_block { flag = false; }
        else { start_block = cur_block; }
    }

    if block_num_vec.len() == 0 { panic!("error: unable to create vectors. dataframe creation aborted"); }

    let mut df = DataFrame::new(vec![
        Series::new("block_num", &block_num_vec),
        Series::new("timestamp", &ts_vec),
        Series::new("hash", &hash_vec),
        Series::new("nonce", &nonce_vec),
        Series::new("block_hash", &block_hash_vec),
        Series::new("contract_address", &contract_addr_vec),
        Series::new("to_address", &to_addr_vec),
        Series::new("from_address", &from_addr_vec),
        Series::new("token_id", &token_id_vec),
        Series::new("token_name", &token_nm_vec),
        Series::new("token_symbol", &token_sym_vec),
        Series::new("token_decimal", &token_dec_vec),
        Series::new("transaction_index", &txn_index_vec),
        Series::new("gas", &gas_vec),
        Series::new("gas_price", &gas_price_vec),
        Series::new("gas_used", &gas_used_vec),
        Series::new("cumulative_gas_used", &cum_gas_used_vec),
        Series::new("confirms", &confirms_vec),
    ])?;

    df = df.distinct_stable(None, polars::frame::DistinctKeepStrategy::First)?;

    // format
    df.try_apply("block_num", |s: &Series| s.cast(&UInt64))?;

    df.try_apply("timestamp", |s: &Series| s.cast(&UInt64))?;
    df.try_apply("timestamp", |s: &Series| Ok(s.u64()?.apply(|x| x * 1000).into_series()));
    df.try_apply("timestamp", |s: &Series| s.cast(&Datetime(Milliseconds, None)))?;

    df.try_apply("nonce", |s: &Series| s.cast(&UInt32))?;
    df.try_apply("token_id", |s: &Series| s.cast(&UInt32))?;
    df.try_apply("token_decimal", |s: &Series| s.cast(&Float64))?;
    df.try_apply("gas_price", |s: &Series| s.cast(&Float64))?;
    df.try_apply("gas_used",|s: &Series| s.cast(&UInt32))?;
    df.try_apply("cumulative_gas_used", |s: &Series| s.cast(&UInt64))?;
    df.try_apply("confirms", |s: &Series| s.cast(&UInt64))?;

    info!("get_erc721_transfers|completed");
    Ok(df)
}

/// Utility method that calculates the daily mint activity
async fn mint_act(
    target_addr: &str,
    es_key: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {

    info!("mint_act|starting");

    let mut df = get_erc721_transfers(&target_addr, &es_key).await?;

    // filter
    let mask: BooleanChunked = df.column("from_address")?
        .utf8()?
        .into_iter()
        .map(|opt_val| {
             match opt_val == Some("0x0000000000000000000000000000000000000000") {
                 true => true,
                 false => false,
             }}).collect();

    let mint: Vec<i32> = df.column("from_address")?
        .utf8()?
        .into_iter()
        .map(|opt_value| {
                match opt_value == Some("0x0000000000000000000000000000000000000000") {
                    true => 1,
                    false => 0,
                }}).collect();

    let mint_s = Series::new("mint", &mint).cast(&UInt32)?;
    df = df.with_column(mint_s)?.filter(&mask)?;

    let mut out = df.select(vec!["timestamp", "token_name", "to_address", "from_address", "mint"])?;
    out.try_apply("timestamp", |s: &Series| s.cast(&Date));

    out = out.groupby(vec!["timestamp", "token_name"])?
            .select(vec!["mint"])
            .sum()?
            .sort(vec!["timestamp"], vec![false])?;

    info!("mint_act|completed");
    Ok(out)
}

/// Utility method that calls the Honey Hives Deluxe contract to retrieve the MAX_SUPPLY
async fn get_honey_supply(
    http_provider: &str,
    abi_path: &str,
) -> Result<U256, Box<dyn std::error::Error>> {
    const target_addr: &str = &"0x5df89cC648a6bd179bB4Db68C7CBf8533e8d796e";

    let http_transport = web3::transports::Http::new(http_provider)?;
    let web3 = web3::Web3::new(http_transport);
    let address_test = Address::from_str(&target_addr).unwrap();
    let contract = Contract::from_json(web3.eth(), address_test, include_bytes!("HoneyHiveDeluxeAbi.json")).unwrap();

    let max_supply: U256 = contract
        .query("MAX_SUPPLY", (), None, Options::default(), None)
        .await
        .unwrap();

    Ok(max_supply)
}

/// Utility method that calls the Bees Deluxe contract to retrieve the MAX_SUPPLY
async fn get_bee_supply(
    http_provider: &str,
    abi_path: &str,
) -> Result<U256, Box<dyn std::error::Error>> {
    const target_addr: &str = &"0x1c2CD50f9Efb463bDd2ec9E36772c14A8D1658B3";

    let http_transport = web3::transports::Http::new(http_provider)?;
    let web3 = web3::Web3::new(http_transport);
    let address_test = Address::from_str(&target_addr).unwrap();
    let contract = Contract::from_json(web3.eth(), address_test, include_bytes!("BeesDeluxeAbi.json")).unwrap();

    let max_supply: U256 = contract
        .query("MAX_SUPPLY", (), None, Options::default(), None)
        .await
        .unwrap();

    Ok(max_supply)
}

/// Utility method that takes a DataFrame object and creates a HTML plotly bar chart
fn create_hist(df: DataFrame, title: &str) -> Result<(), Box<dyn std::error::Error>>{
    info!("create_hist|starting");
    info!("create_hist|title={}", title);

    let date_col = df.column("timestamp")?
        .date()?
        .strftime("%Y-%m-%d");

    let mut domain_vec: Vec<String> = vec![];
    for date in &date_col {
        domain_vec.push(String::from(date.unwrap()));
    }

    let mint_col = df.column("mint_sum")?
        .u32()?;

    let mut mint_vec: Vec<i32> = vec![];
    for mint in mint_col {
        mint_vec.push(mint.unwrap_or(0) as i32);
    }

    let layout = Layout::new()
        .title(Title::new(title))
        .x_axis(Axis::new().title(Title::new("Date")))
        .y_axis(Axis::new().title(Title::new("Mint Activity")));

    let t = Bar::new(domain_vec, mint_vec);

    let mut plot = Plot::new();
    plot.set_layout(layout);
    plot.add_trace(t);
    plot.show();

    info!("create_hist|completed");

    Ok(())
}

/// Utility method to calculate the Bears Deluxe migration progress
/// and POST status to twitter, if -t flag is enabled
pub async fn bear_mint_act(
    config: &BTreeMap<String, String>,
    cli_args: clap::ArgMatches,
) -> Result<(), Box<dyn std::error::Error>> {

    info!("bear_mint_act|starting");

    const ADDR: &str = "0x4BB33f6E69fd62cf3abbcC6F1F43b94A5D572C2B";
    const MAX_SUPPLY: i32 = 6900;

    let es = config.get("es_key").expect("error: es_key is invalid");

    let df = mint_act(ADDR, &es).await?;

    let minted_today = df.column("mint_sum")?
        .tail(Some(1))
        .u32()?
        .into_iter()
        .next()
        .unwrap()
        .unwrap();

    let total_mint: i32 = df.column("mint_sum")?
        .sum()
        .unwrap();

    let remaining = MAX_SUPPLY - total_mint;

    let cent = (total_mint as f32/MAX_SUPPLY as f32)*100.0;

    let status = format!("- Bears Deluxe Migration -
Progress: {:.2}%
Migrated Today: {}
Supply: {}/{}\n
Remaining: {}",
        cent,
        minted_today,
        total_mint,
        MAX_SUPPLY,
        remaining);

    println!("{}", status);

    let post_flg = i32::from_str(cli_args.value_of("post").unwrap_or("0")).unwrap();
    match post_flg {
        1 => { 
            info!("bear_mint_act|post_flg={}", post_flg); 

            let egg_token = EggToken::new(
                String::from(config.get("con_key").expect("config: con_key is invalid")),
                String::from(config.get("con_secret").expect("config: con_secret is invalid")),
                String::from(config.get("acc_key").expect("config: acc_key is invalid")),
                String::from(config.get("acc_secret").expect("config: acc_secret is invalid")),
            );

            send_tweet(&egg_token.token, status, None).await?;
        },
        _ => info!("bear_mint_act|skipping status POST"),
    }

    info!("bear_mint_act|completed");
    Ok(())
}

pub async fn bee_mint_act(
    config: &BTreeMap<String, String>,
    cli_args: clap::ArgMatches,
) -> Result<(), Box<dyn std::error::Error>> {

    info!("bee_mint_act|starting");

    let target_addr = cli_args
        .value_of("addr")
        .unwrap_or("0x1c2CD50f9Efb463bDd2ec9E36772c14A8D1658B3");

    let es = config.get("es_key").expect("error: es_key is invalid");

    let mut df = mint_act(&target_addr, &es).await?;

    /*
    let minted_today = df.column("mint_sum")?
        .tail(Some(1))
        .u32()?
        .into_iter()
        .next()
        .unwrap()
        .unwrap();

    let total_mint: i32 = df.column("mint_sum")?
        .sum()
        .unwrap();

    let max_supply: U256 = get_bee_supply(config.get("alchemy_url").unwrap(), "BeesDeluxeAbi.json").await?;

    let remaining = max_supply - total_mint;

    let cent = (total_mint as f32/max_supply.low_u32() as f32)*100.0;
    */

    let lookback = i32::from_str(cli_args.value_of("lookback")
        .unwrap_or("0"))
        .unwrap();

    let title = match lookback {
        0 => {
            info!("hive_mint_act|lookback={}", lookback);
            String::from("Bees Deluxe Historical Mint Activity")
        },
        1..=180 => { 
            info!("hive_mint_act|lookback={}", lookback);
            df = df.tail(Some(lookback as usize));
            format!("Bees Deluxe {}D Mint Activity", lookback)
        },
        _ => panic!("error: lookback_days is invalid, lookback > 180"),
    };

    create_hist(df, &title);

    info!("bee_mint_act|completed");
    Ok(())
}

pub async fn hive_mint_act(
    config: &BTreeMap<String, String>,
    cli_args: clap::ArgMatches,
) -> Result<(), Box<dyn std::error::Error>> {

    info!("hive_mint_act|starting");

    let target_addr = cli_args
        .value_of("addr")
        .unwrap_or("0x5df89cC648a6bd179bB4Db68C7CBf8533e8d796e");

    let es = config.get("es_key").expect("error: es_key is invalid");

    let mut df = mint_act(&target_addr, &es).await?;

    /*
    let minted_today = df.column("mint_sum")?
        .tail(Some(1))
        .u32()?
        .into_iter()
        .next()
        .unwrap()
        .unwrap();

    let total_mint: i32 = df.column("mint_sum")?
        .sum()
        .unwrap();

    let max_supply: U256 = get_honey_supply(config.get("alchemy_url").unwrap(), "HoneyHiveDeluxeAbi.json").await?;

    let remaining = max_supply - total_mint;

    let cent = (total_mint as f32/max_supply.low_u32() as f32)*100.0;
    */

    let lookback = i32::from_str(cli_args.value_of("lookback")
        .unwrap_or("0"))
        .unwrap();

    let title = match lookback {
        0 => {
            info!("hive_mint_act|lookback={}", lookback);
            String::from("Honey Hives Deluxe Historical Mint Activity")
        },
        1..=180 => { 
            info!("hive_mint_act|lookback={}", lookback);
            df = df.tail(Some(lookback as usize));
            format!("Honey Hives Deluxe {}D Mint Activity", lookback)
        },
        _ => panic!("error: lookback_days is invalid, lookback > 180"),
    };

    create_hist(df, &title);
    
    info!("hive_mint_act|completed");
    Ok(())
}

pub async fn erc721_mint_act(
    config: &BTreeMap<String, String>,
    cli_args: clap::ArgMatches,
) -> Result<(), Box<dyn std::error::Error>> {

    info!("erc721_mint_act|starting");

    let target_addr = cli_args.value_of("addr").expect("error: addr is invalid");
    let es = config.get("es_key").expect("error: es_key is invalid");

    let mut df = mint_act(&target_addr, &es).await?;

    let project_name = String::from(df.column("token_name")?
        .utf8()?
        .into_iter()
        .next()
        .unwrap()
        .unwrap());

    let lookback = i32::from_str(cli_args.value_of("lookback")
        .unwrap_or("0"))
        .unwrap();

    let title = match lookback {
        0 => {
            info!("erc721_mint_act|lookback={}", lookback);
            format!("{} Historical Mint Activity", project_name)
        },
        1..=180 => { 
            info!("erc721_mint_act|lookback={}", lookback);
            df = df.tail(Some(lookback as usize));
            format!("{} {}D Mint Activity", project_name, lookback)
        },
        _ => panic!("error: lookback_days is invalid, lookback > 180"),
    };

    create_hist(df, &title);

    info!("erc721_mint_act|completed");

    Ok(())
}

/// Utility method to POST a tweet using egg_mode
async fn send_tweet(
    egg_token: &egg_mode::Token,
    status: String,
    file_path: Option<&String>,
) -> Result<(), Box<dyn std::error::Error>>{
    
    info!("send_tweet|starting");
    //info!("send_tweet|status={}", status);
    //info!("send_tweet|file_path={}", file_path);

    let mut tweet = DraftTweet::new(status);
    
    if file_path != None {
        info!("send_tweet|uploading file: {}", file_path.unwrap());

        let typ = media_types::image_png();
        let bytes = std::fs::read(file_path.unwrap())?;

        let handle = upload_media(&bytes, &typ, &egg_token).await?;
        tweet.add_media(handle.id.clone());

        info!("send_tweet|media upload processing...");
        sleep(Duration::from_secs(30));

        match get_status(handle.id.clone(), &egg_token).await?.progress {
            None | Some(ProgressInfo::Success) => {
                info!("send_tweet|media sucessfully processed");
            }
            Some(ProgressInfo::Pending(_)) | Some(ProgressInfo::InProgress(_)) => {
                stdout().flush()?;
                sleep(Duration::from_secs(45));
            }
            Some(ProgressInfo::Failed(err)) => Err(err)?,
        }
    }
    
    tweet.send(&egg_token).await?; 

    info!("send_tweet|completed");

    Ok(())
}
