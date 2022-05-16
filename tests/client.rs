use bytes::{BufMut, BytesMut};
use protobuf::Message;
use registry::{anyhow, client::Client, protos::registry::*, tokio, utils::init_log, ResultType};
use std::collections::HashMap;
use std::time;

#[tokio::test(flavor = "multi_thread")]
async fn test_node_client() -> ResultType<()> {
    init_log();
    let server_name = "signaling".to_string();
    let dc_client = Client::new(
        &vec!["127.0.0.1:4222".to_owned()],
        Some("nats_client"),
        Some("YourNatsPassword"),
    )
    .await;
    if let Err(e) = dc_client {
        return Err(anyhow::anyhow!("Create discovery client failed: {:?}", e));
    }
    let mut dc_client = dc_client.ok().unwrap();

    let nid = "1234576abc";

    // create local node
    let node = NodeDescription {
        data_center: "dc1".to_string(),
        service: server_name.clone(),
        nid: nid.to_string(),
        ..Default::default()
    };

    // Start dicovery node, and than receive the node state and event message.
    let (mut node_state_receiver, mut node_event_receiver) = dc_client.start(node).await.unwrap();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                // node state callback.
                Some((state, node)) = node_state_receiver.recv() => {
                    match state {
                        NodeState::NODE_UP => {
                            log::info!("NodeUp => {:?}", &node);
                        }
                        NodeState::NODE_DOWN => {
                            log::info!("NodeDown => {:?}", &node);
                        }
                    };
                }
                // node event callback.
                Some(event_msg) = node_event_receiver.recv() => {
                    let mut bytes = BytesMut::new();
                    bytes.put_slice(&event_msg.data);
                    if let Ok(req) = Request::parse_from_bytes(&bytes) {
                        log::info!("<<<<<<<<<---------Get req message: {:?}", req);
                        let out_msg = Response {
                            success: false,
                            reason: "response false".to_owned(),
                            ..Default::default()
                        };
                        let out_data = out_msg.write_to_bytes().ok().unwrap();
                        event_msg.respond(out_data).await.ok();
                        log::info!("response done!!!");
                    }

                    if let Ok(bradcast_response) = Response::parse_from_bytes(&bytes) {
                        log::info!("<<<<<<<<<---------Get broadcast response message: {:?}", bradcast_response);
                    }
                }
            }
        }
    });

    // Fetch nodes list of specified service name
    tokio::time::sleep(time::Duration::from_secs(2)).await;
    let mut params = HashMap::new();
    params.insert("dc".to_string(), "dc1".to_string());
    params.insert("nid".to_string(), "1234576abc".to_string());
    // get nodes by service type, dc or nid.
    match dc_client.get_nodes(&server_name, params).await {
        Some(nodes) => {
            // send to event message to first node
            let first_node = &nodes.nodes_item[0];
            // log::info!("Fetched nodes info: {:?}", first_node);
            let out_msg = Request {
                service: "test".to_string(),
                ..Default::default()
            };
            let data = out_msg.write_to_bytes();
            match dc_client
                .send_event(&first_node, data.unwrap())
                .await
                .unwrap()
            {
                Some(reply) => {
                    // Get response from first node.
                    let mut bytes = BytesMut::new();
                    bytes.put_slice(&reply.data);
                    let reply_msg = Response::parse_from_bytes(&bytes).ok().unwrap();
                    log::info!("---------------Get response: {:?}", reply_msg);
                }
                None => log::info!("---------------Not found any reply"),
            }
        }
        None => log::info!("Not found target nodes."),
    };
    tokio::time::sleep(time::Duration::from_secs(5)).await;
    let broadcast_req = Request {
        action: Action::BROADCAST.into(),
        ..Default::default()
    };
    let payload = broadcast_req.write_to_bytes().ok().unwrap();
    let mut params = HashMap::new();
    params.insert("dc".to_string(), "*".to_string());
    dc_client
        .broadcast_event(&server_name, params, true, payload)
        .await
        .ok();
    tokio::time::sleep(time::Duration::from_secs(2)).await;
    // Stop the discovery client.
    dc_client.stop().await.ok();
    tokio::time::sleep(time::Duration::from_secs(1)).await;
    Ok(())
}
