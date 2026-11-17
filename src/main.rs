use chrono::{DateTime, TimeDelta, Utc};
use grammers_client::{
    Client, InvocationError,
    sender::{ConnectionParams, NoRetries, SenderPool},
    session::{Session, storages::MemorySession},
    tl,
};
use reqwest::{Proxy, Url};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{sleep, timeout};

const CRYPTO: &str = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,the-open-network&vs_currencies=usd";
const FIAT: &str = "https://www.cbr-xml-daily.ru/daily_json.js";
const DEFAULT_POOL: &str = "socks5://xray:2079,socks5://xray:2078,socks5://xray:2080";

type StartupResult<T> = Result<T, &'static str>;

struct Config {
    data: PathBuf,
    api_id: i32,
    prefix: String,
    proxies: Vec<Option<String>>,
}

impl Config {
    fn load() -> StartupResult<Self> {
        let api_id = env::var("API_ID")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|id| *id > 0)
            .ok_or("API_ID must be a positive integer")?;
        let prefix = env::var("PROFILE_PREFIX").unwrap_or_else(|_| "TGTime".into());
        if prefix.trim().is_empty() || prefix.chars().count() > 55 {
            return Err("PROFILE_PREFIX must be 1–55 characters");
        }
        let pool = env::var("PROXY_POOL_URLS").unwrap_or_else(|_| DEFAULT_POOL.into());
        let legacy = env::var("PROXY_URL").unwrap_or_default();
        let mut proxies = Vec::new();
        for proxy in pool.split(',').chain(std::iter::once(legacy.as_str())) {
            let proxy = proxy.trim();
            if proxy.is_empty() {
                continue;
            }
            let proxy = normalize_proxy(proxy)?;
            if !proxies.contains(&Some(proxy.clone())) {
                proxies.push(Some(proxy));
            }
        }
        if proxies.is_empty() {
            proxies.push(None);
        }
        Ok(Self {
            data: env::var_os("DATA_DIR").map_or_else(|| "/app/data".into(), Into::into),
            api_id,
            prefix,
            proxies,
        })
    }
}

fn normalize_proxy(proxy: &str) -> StartupResult<String> {
    let url = Url::parse(proxy).map_err(|_| "Invalid SOCKS5 proxy configuration")?;
    if !matches!(url.scheme(), "socks5" | "socks5h")
        || url.host_str().is_none()
        || url.port().is_none()
    {
        return Err("Expected a SOCKS5 proxy with host and port");
    }
    Ok(proxy.replacen("socks5h://", "socks5://", 1))
}

struct LegacySession {
    dc: i32,
    key: [u8; 256],
    account: i64,
}

fn read_session(path: &Path, expected_api: i32) -> StartupResult<LegacySession> {
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|_| "Existing Pyrogram SQLite session is required")?;
    let (dc, api, test, key, account, bot): (i32, Option<i32>, bool, Vec<u8>, i64, bool) = db
        .query_one(
            "SELECT dc_id, api_id, test_mode, auth_key, user_id, is_bot FROM sessions",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .map_err(|_| "Session does not contain exactly one authorized Telegram account")?;
    if test
        || bot
        || account <= 0
        || !(1..=5).contains(&dc)
        || api.is_some_and(|id| id != expected_api)
    {
        return Err("Session does not match the configured production Telegram user");
    }
    let key = key
        .try_into()
        .map_err(|_| "Invalid Telegram authorization key length")?;
    Ok(LegacySession { dc, key, account })
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct Quote {
    at: f64,
    values: Vec<f64>,
}

#[derive(Default, Deserialize, Serialize)]
struct Cache {
    #[serde(default)]
    crypto: Quote,
    #[serde(default)]
    fiat: Quote,
}

impl Quote {
    fn valid(&self, count: usize) -> bool {
        self.values.len() == count && self.values.iter().all(|&n| n > 0.0 && n < 1e9)
    }
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

fn write_json(path: &Path, value: &impl Serialize) -> StartupResult<()> {
    let tmp = path.with_extension("tmp");
    let data = serde_json::to_vec(value).map_err(|_| "Cannot serialize state")?;
    fs::write(&tmp, data)
        .and_then(|_| fs::rename(tmp, path))
        .map_err(|_| "Cannot persist state")
}

fn extract_prices(kind: &str, body: &Value) -> Option<Vec<f64>> {
    let values = if kind == "crypto" {
        ["bitcoin", "ethereum", "the-open-network"]
            .iter()
            .map(|key| body[*key]["usd"].as_f64())
            .collect::<Option<Vec<_>>>()?
    } else {
        ["USD", "EUR", "CNY", "JPY"]
            .iter()
            .map(|key| body["Valute"][*key]["Value"].as_f64())
            .collect::<Option<Vec<_>>>()?
    };
    values.iter().all(|&n| n > 0.0 && n < 1e9).then_some(values)
}

async fn refresh(
    kind: &str,
    old: &Quote,
    clients: &[reqwest::Client],
    preferred: usize,
) -> Option<Quote> {
    let (url, ttl, count) = if kind == "crypto" {
        (CRYPTO, 300.0, 3)
    } else {
        (FIAT, 3600.0, 4)
    };
    let age = now() - old.at;
    if old.valid(count) && (0.0..ttl).contains(&age) {
        return None;
    }
    for index in std::iter::once(preferred).chain((0..clients.len()).filter(|&i| i != preferred)) {
        let Some(response) = clients[index]
            .get(url)
            .send()
            .await
            .ok()
            .and_then(|r| r.error_for_status().ok())
        else {
            continue;
        };
        if let Ok(body) = response.json().await
            && let Some(values) = extract_prices(kind, &body)
        {
            return Some(Quote { at: now(), values });
        }
    }
    eprintln!("[Prices] {kind}: upstream unavailable");
    None
}

fn amount(quote: &Quote, count: usize, index: usize, decimals: usize) -> String {
    if quote.valid(count) {
        format!("{:.*}", decimals, quote.values[index])
    } else {
        "—".into()
    }
}

fn profile(cache: &Cache, premium: bool, time: DateTime<Utc>, prefix: &str) -> (String, String) {
    let clock = |offset| {
        (time + TimeDelta::hours(offset))
            .format("%H:%M")
            .to_string()
    };
    let name = format!("{prefix} {}", clock(3));
    let fields = [
        format!("🇷🇺MSK {}", clock(3)),
        format!("🇯🇵TYO {}", clock(9)),
        format!("🇫🇷PAR {}", clock(2)),
        format!("🟠BTC ${}", amount(&cache.crypto, 3, 0, 0)),
        format!("🟣ETH ${}", amount(&cache.crypto, 3, 1, 0)),
        format!("🔷TON ${}", amount(&cache.crypto, 3, 2, 2)),
        format!("💵USD {}₽", amount(&cache.fiat, 4, 0, 2)),
        format!("💶EUR {}₽", amount(&cache.fiat, 4, 1, 2)),
        format!("💴CNY {}₽", amount(&cache.fiat, 4, 2, 2)),
    ];
    let limit = if premium { 140 } else { 70 };
    let mut bio = String::new();
    for field in fields {
        if bio.chars().count() + field.chars().count() + usize::from(!bio.is_empty()) <= limit {
            if !bio.is_empty() {
                bio.push('┊');
            }
            bio.push_str(&field);
        }
    }
    (name, bio)
}

enum Failure {
    Reconnect,
    Flood(u32),
    Fatal(&'static str),
}

fn telegram_failure(error: InvocationError) -> Failure {
    match error {
        InvocationError::Rpc(error) if error.code == 420 => error.value.map_or(
            Failure::Fatal("Telegram rate limit has no duration"),
            Failure::Flood,
        ),
        InvocationError::Rpc(error) if error.code == 401 => {
            Failure::Fatal("Telegram session is unauthorized")
        }
        _ => Failure::Reconnect,
    }
}

async fn connected(
    client: &Client,
    account: i64,
    config: &Config,
    cache: &mut Cache,
    prices: &[reqwest::Client],
    index: usize,
    check: bool,
) -> Result<(), Failure> {
    let user = timeout(Duration::from_secs(35), client.get_me())
        .await
        .map_err(|_| Failure::Reconnect)?
        .map_err(telegram_failure)?;
    let tl::enums::User::User(user) = user.raw else {
        return Err(Failure::Fatal("Telegram returned no account"));
    };
    if user.id != account || user.bot {
        return Err(Failure::Fatal("Telegram returned the wrong account"));
    }
    if check {
        println!("[OK] Existing Telegram user session verified");
        return Ok(());
    }
    loop {
        let (crypto, fiat) = tokio::join!(
            refresh("crypto", &cache.crypto, prices, index),
            refresh("fiat", &cache.fiat, prices, index),
        );
        let changed = crypto.is_some() || fiat.is_some();
        if let Some(crypto) = crypto {
            cache.crypto = crypto;
        }
        if let Some(fiat) = fiat {
            cache.fiat = fiat;
        }
        if changed {
            write_json(&config.data.join("prices.json"), cache).map_err(Failure::Fatal)?;
        }
        let (first_name, about) = profile(cache, user.premium, Utc::now(), &config.prefix);
        let result = timeout(
            Duration::from_secs(15),
            client.invoke(&tl::functions::account::UpdateProfile {
                first_name: Some(first_name),
                last_name: None,
                about: Some(about),
            }),
        )
        .await
        .map_err(|_| Failure::Reconnect)?;
        match result.map_err(telegram_failure) {
            Ok(_) => {}
            Err(Failure::Flood(seconds)) => {
                honor_flood(&config.data, seconds)
                    .await
                    .map_err(Failure::Fatal)?;
                continue;
            }
            Err(error) => return Err(error),
        }
        write_json(
            &config.data.join("health.json"),
            &json!({"at": now(), "profile_updated": true}),
        )
        .map_err(Failure::Fatal)?;
        println!("[OK] Profile updated");
        sleep(Duration::from_secs(
            60 - Utc::now().timestamp().rem_euclid(60) as u64,
        ))
        .await;
    }
}

async fn honor_flood(data: &Path, seconds: u32) -> StartupResult<()> {
    let seconds = u64::from(seconds) + 1;
    write_json(&data.join("flood.json"), &(now() + seconds as f64))?;
    println!("[FloodWait] Waiting {seconds}s");
    sleep(Duration::from_secs(seconds)).await;
    Ok(())
}

fn price_client(proxy: Option<&str>) -> StartupResult<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .user_agent(concat!("TGTime/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(6));
    if let Some(url) = proxy {
        builder = builder.proxy(
            Proxy::all(url.replacen("socks5://", "socks5h://", 1))
                .map_err(|_| "Invalid price proxy configuration")?,
        );
    }
    builder
        .build()
        .map_err(|_| "Cannot initialize price client")
}

async fn run(config: Config, check: bool) -> StartupResult<()> {
    fs::create_dir_all(&config.data).map_err(|_| "Cannot open TGTime data directory")?;
    let lock = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(config.data.join("tgtime.lock"))
        .map_err(|_| "Cannot open TGTime process lock")?;
    lock.try_lock()
        .map_err(|_| "Another Rust TGTime process is running")?;
    let source = read_session(&config.data.join("my_account.session"), config.api_id)?;
    let prices = config
        .proxies
        .iter()
        .map(|url| price_client(url.as_deref()))
        .collect::<StartupResult<Vec<_>>>()?;
    let mut cache = read_json(&config.data.join("prices.json")).unwrap_or_default();
    if let Some(deadline) = read_json::<f64>(&config.data.join("flood.json"))
        && deadline > now()
    {
        if check {
            return Err("Telegram rate limit is still active");
        }
        sleep(Duration::from_secs_f64(deadline - now())).await;
    }
    let mut index = 0;
    loop {
        let session = Arc::new(MemorySession::default());
        let mut dc = session
            .dc_option(source.dc)
            .map_err(|_| "Cannot read Telegram datacenter")?
            .ok_or("Invalid datacenter")?;
        dc.auth_key = Some(source.key);
        session
            .set_dc_option(&dc)
            .await
            .map_err(|_| "Cannot initialize datacenter")?;
        session
            .set_home_dc_id(source.dc)
            .await
            .map_err(|_| "Cannot initialize home datacenter")?;
        let pool = SenderPool::with_configuration(
            session,
            config.api_id,
            ConnectionParams {
                proxy_url: config.proxies[index].clone(),
                retry_policy: Box::new(NoRetries),
                ..Default::default()
            },
        );
        let client = Client::new(pool.handle);
        let runner = tokio::spawn(pool.runner.run());
        println!("[Connect] Pool {}/{}", index + 1, config.proxies.len());
        let result = connected(
            &client,
            source.account,
            &config,
            &mut cache,
            &prices,
            index,
            check,
        )
        .await;
        client.disconnect();
        runner.abort();
        let _ = runner.await;
        match result {
            Ok(()) => return Ok(()),
            Err(Failure::Fatal(message)) => return Err(message),
            Err(Failure::Flood(seconds)) if !check => {
                honor_flood(&config.data, seconds).await?;
                continue;
            }
            Err(Failure::Flood(_)) => return Err("Telegram rate limit is active"),
            Err(Failure::Reconnect) => eprintln!("[Reconnect] Telegram connection failed"),
        }
        index = (index + 1) % config.proxies.len();
        if check && index == 0 {
            return Err("Could not verify Telegram session on the configured routes");
        }
        sleep(Duration::from_secs(if index == 0 { 30 } else { 5 })).await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let check = match env::args().nth(1).as_deref() {
        None => false,
        Some("--check") => true,
        Some("--help") => {
            println!("Usage: tgtime [--check|--health]");
            return;
        }
        Some("--health") => {
            let directory =
                env::var_os("DATA_DIR").map_or_else(|| PathBuf::from("/app/data"), Into::into);
            if !healthy(read_json(&directory.join("health.json")).as_ref(), now()) {
                eprintln!("No recent confirmed Telegram update");
                std::process::exit(1);
            }
            return;
        }
        _ => {
            eprintln!("Usage: tgtime [--check|--health]");
            std::process::exit(2);
        }
    };
    let result = match Config::load() {
        Ok(config) => run(config, check).await,
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        eprintln!("[TGTime] {error}");
        std::process::exit(1);
    }
}

fn healthy(value: Option<&Value>, current: f64) -> bool {
    value.is_some_and(|value| {
        value["profile_updated"].as_bool() == Some(true)
            && value["at"]
                .as_f64()
                .is_some_and(|at| (0.0..=180.0).contains(&(current - at)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn price_requests_identify_the_application() {
        use std::io::{BufRead, BufReader, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let headers = BufReader::new(&socket)
                .lines()
                .map_while(Result::ok)
                .take_while(|line| !line.is_empty())
                .collect::<Vec<_>>();
            assert!(
                headers
                    .iter()
                    .any(|value| value.to_lowercase().starts_with("user-agent: tgtime/"))
            );
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
        });
        let response = price_client(None).unwrap().get(url).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        server.join().unwrap();
    }

    #[test]
    fn matches_the_legacy_clock_and_bio() {
        let time = "2026-11-17T13:00:00Z".parse().unwrap();
        let cache = Cache {
            crypto: Quote {
                at: 0.0,
                values: vec![100.0, 20.0, 1.25],
            },
            fiat: Quote {
                at: 0.0,
                values: vec![95.0, 105.0, 12.5, 0.5],
            },
        };
        let (name, bio) = profile(&cache, true, time, "TGTime");
        assert_eq!(name, "TGTime 16:00");
        assert_eq!(
            bio,
            "🇷🇺MSK 16:00┊🇯🇵TYO 22:00┊🇫🇷PAR 15:00┊🟠BTC $100┊🟣ETH $20┊🔷TON $1.25┊💵USD 95.00₽┊💶EUR 105.00₽┊💴CNY 12.50₽"
        );
        assert!(profile(&cache, false, time, "TGTime").1.chars().count() <= 70);
        assert!(
            profile(&Cache::default(), true, time, "TGTime")
                .1
                .contains("—")
        );
    }

    #[test]
    fn validates_prices_and_keeps_legacy_cache_shape() {
        let mut response =
            json!({"bitcoin":{"usd":100},"ethereum":{"usd":20},"the-open-network":{"usd":1.25}});
        assert_eq!(
            extract_prices("crypto", &response),
            Some(vec![100.0, 20.0, 1.25])
        );
        response["ethereum"]["usd"] = json!(true);
        assert_eq!(extract_prices("crypto", &response), None);
        let cache: Cache =
            serde_json::from_value(json!({"crypto":{"at":100,"values":[100,20,1.25]}})).unwrap();
        assert!(cache.crypto.valid(3));
        assert!(!cache.fiat.valid(4));
    }

    #[test]
    fn health_requires_a_confirmed_recent_update() {
        assert!(healthy(
            Some(&json!({"at":820,"profile_updated":true})),
            1000.0
        ));
        assert!(!healthy(
            Some(&json!({"at":819,"profile_updated":true})),
            1000.0
        ));
        assert!(!healthy(
            Some(&json!({"at":1001,"profile_updated":true})),
            1000.0
        ));
        assert!(!healthy(
            Some(&json!({"at":999,"profile_updated":false})),
            1000.0
        ));
    }

    #[test]
    fn requires_socks5_with_an_explicit_port() {
        assert_eq!(
            normalize_proxy("socks5h://proxy:1000").unwrap(),
            "socks5://proxy:1000"
        );
        assert!(normalize_proxy("http://proxy:1000").is_err());
        assert!(normalize_proxy("socks5://proxy").is_err());
    }
}
