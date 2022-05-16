use crate::protos::registry::DiscoveryMessage;
use crate::ResultType;
use anyhow::anyhow;
use bytes::{BufMut, BytesMut};
use chrono;
use env_logger::{fmt::Color, Builder, Env};
use nats::asynk::{Connection, Options};
use protobuf::Message;
use rand::Rng;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use time::Duration;

pub fn parse_timestamp_secs(time: SystemTime) -> u64 {
    let since_the_epoch = time
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_secs()
}

pub fn random_string(str_len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789";
    let mut rng = rand::thread_rng();
    let randomo_str: String = (0..str_len)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();

    log::info!("{:?}", randomo_str);
    return randomo_str;
}

pub fn marshal(message: DiscoveryMessage) -> ResultType<Vec<u8>> {
    Ok(message.write_to_bytes()?)
}

pub fn unmarshal(data: Vec<u8>) -> ResultType<DiscoveryMessage> {
    let mut bytes = BytesMut::new();
    bytes.put_slice(&data);
    Ok(DiscoveryMessage::parse_from_bytes(&bytes)?)
}

pub fn init_log() {
    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", "trace")
        // Normally using a pipe as a target would mean a value of false, but this forces it to be true.
        .write_style_or("MY_LOG_STYLE", "always");

    Builder::from_env(env)
        .format(|buf, record| {
            let mut style = buf.style();
            style.set_bg(Color::Yellow).set_bold(true);
            writeln!(
                buf,
                "[{}] {} {}:{} - {}",
                record.level(),
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S.%3f"),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                style.value(record.args())
            )
        })
        .is_test(true)
        .format_timestamp_nanos()
        .init();
}

pub async fn create_nats_connection(
    nats_uri: &str,
    user: Option<&str>,
    password: Option<&str>,
) -> ResultType<Connection> {
    let opts = setup_conn_options(user, password);
    let nc = match opts.connect(nats_uri).await {
        Ok(nc) => nc,
        Err(e) => {
            return Err(anyhow!("Connect the nats network failed.., error: {:?}", e));
        }
    };
    Ok(nc)
}

fn setup_conn_options(user: Option<&str>, password: Option<&str>) -> Options {
    let mut opts = Options::new();

    if user.is_some() && password.is_some() {
        opts = Options::with_user_pass(user.unwrap(), password.unwrap());
    }

    let reconnect_delay = Duration::seconds(1);
    opts = opts
        .with_name("discover-node")
        .retry_on_failed_connect()
        .disconnect_callback(move || {
            log::warn!(
                "Disconnected, will attempt reconnects for {:?}m",
                reconnect_delay.whole_seconds()
            )
        })
        .reconnect_delay_callback(move |c| {
            log::warn!("Reconnected..");
            std::time::Duration::from_millis(std::cmp::min(
                (c * 100) as u64,
                reconnect_delay.whole_milliseconds() as u64,
            ))
        })
        .close_callback(|| log::info!("the nats net closed!"));
    opts
}
