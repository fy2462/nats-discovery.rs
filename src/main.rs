use registry::{
    anyhow::anyhow, discovery, log, protos::registry::*, tokio, utils::init_log, ResultType,
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[tokio::main()]
async fn main() -> ResultType<()> {
    init_log();
    let nodes_map: Arc<RwLock<HashMap<String, NodeDescription>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let registry = discovery::Registry::new_server(
        vec!["127.0.0.1:4222".to_owned()],
        5,
        Some("nats_client"),
        Some("YourNatsPassword"),
    )
    .await;
    if let Err(e) = registry {
        return Err(anyhow!("Create registry server failed: {:?}", e));
    }
    let registry = registry.ok().unwrap();
    let action_ref = nodes_map.clone();
    let service_ref = nodes_map.clone();

    let handle_action = move |action: Action, node: &NodeDescription| {
        log::info!("handleNodeAction: action {:?}, node {:?}", action, node);
        {
            match action {
                Action::SAVE => {
                    log::info!("Save node in main...");
                }
                Action::UPDATE => {
                    action_ref
                        .write()
                        .unwrap()
                        .insert(node.nid.clone(), node.clone());
                    log::info!("Update node in main...");
                }
                Action::DELETE => {
                    action_ref.write().unwrap().remove(&node.nid);
                    log::info!("Delete node in main...");
                }
                _ => {}
            }
        }
        Ok(())
    };

    let handle_fetch_nodes = move |service: &str, params: &HashMap<String, String>| {
        log::info!("handleGetNodes: service {:?}, params {:?}", service, params);
        let dc = params.get("dc");
        let nid = params.get("nid");
        let nodes: Vec<NodeDescription> = service_ref
            .read()
            .unwrap()
            .values()
            .filter(|item| item.service == service || service == "*")
            .filter(|item| match dc {
                Some(dc) => item.data_center == *dc || dc == "*",
                None => true,
            })
            .filter(|item| match nid {
                Some(nid) => item.nid == *nid || nid == "*",
                None => true,
            })
            .map(|item| item.clone())
            .collect();
        Ok(nodes)
    };

    registry.listen(handle_action, handle_fetch_nodes).await?;
    Ok(())
}
