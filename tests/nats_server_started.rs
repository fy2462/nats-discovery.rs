use registry::{anyhow, client::Client, tokio, utils::init_log, ResultType, log};

#[tokio::test(flavor = "multi_thread")]
async fn test_nats_server_started() -> ResultType<()> {
    init_log();
    let dc_client = Client::new(
        &vec!["127.0.0.1:44129".to_owned()],
        Some("nats_client"),
        Some("YourNatsPassword"),
    )
    .await;
    if let Err(e) = dc_client {
        log::error!("Create discovery client failed: {:?}", e);
        return Err(anyhow::anyhow!("Create discovery client failed: {:?}", e));
    }
    log::info!("nats server started");
    Ok(())
}
