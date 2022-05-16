use crate::utils::{create_nats_connection, marshal, parse_timestamp_secs, unmarshal};
use crate::{
    protos::registry::*, ResultType, DEFAULT_DISCOVERY_PRIFEX, DEFAULT_NODE_BROADCAST_PRIFEX,
    DEFAULT_PUBLISH_PRIFEX,
};
use anyhow::anyhow;
use log;
use nats::asynk::{Connection, Message as NatsMessage};
use protobuf::Message;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use time::Duration;
use tokio::{
    sync::mpsc::{self, UnboundedSender},
    time::interval,
};

pub struct Registry {
    nats_conn: Box<Connection>,
    expire_duration_secs: Duration,
}

impl Registry {
    pub async fn new_server(
        nats_addr: Vec<String>,
        expire_duration_secs: usize,
        user: Option<&str>,
        password: Option<&str>,
    ) -> ResultType<Self> {
        let nats_uri = nats_addr.join(",");
        let nc = create_nats_connection(&nats_uri, user, password).await?;
        let expire_duration_secs = Duration::seconds(expire_duration_secs as i64);
        Ok(Self {
            nats_conn: Box::new(nc),
            expire_duration_secs,
        })
    }

    pub async fn listen<AF, GF>(
        &self,
        mut handle_node_action: AF,
        mut handle_get_nodes: GF,
    ) -> ResultType<()>
    where
        AF: 'static + FnMut(Action, &NodeDescription) -> ResultType<()> + Send,
        GF: 'static
            + FnMut(&str, &HashMap<String, String>) -> ResultType<Vec<NodeDescription>>
            + Send,
    {
        let (publish_sender, mut publish_receiver) = mpsc::unbounded_channel::<NatsMessage>();
        let (broadcast_sender, mut broadcast_receiver) = mpsc::unbounded_channel::<NatsMessage>();
        let nats_conn = self.nats_conn.as_ref();
        self.sub_publish_message(nats_conn, publish_sender).await?;
        self.sub_broadcast_message(nats_conn, broadcast_sender)
            .await?;

        let listener = async move {
            let mut timer = interval(tokio::time::Duration::from_secs(1));
            let nodes: Arc<RwLock<HashMap<String, NodeItem>>> =
                Arc::new(RwLock::new(HashMap::new()));
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        let now = SystemTime::now();
                        self.check_expires(nodes.clone(), now, &mut handle_node_action).await;
                    }
                    Some(nats_msg) = publish_receiver.recv() => {
                        log::info!("Handle publish nats message... {:?}", nats_msg);
                        self.handle_nats_msg(nats_msg, nodes.clone(), &mut handle_node_action, &mut handle_get_nodes).await.ok();
                        log::info!("------------------------------------------");
                    }
                    Some(broadcast_msg) = broadcast_receiver.recv() => {
                        log::info!("Handle publish nats message... {:?}", broadcast_msg);
                        self.handle_broadcast_msg(broadcast_msg, &mut handle_get_nodes).await.ok();
                    }
                }
            }
        };
        tokio::join!(listener);
        Ok(())
    }

    pub async fn handle_broadcast_msg<GF>(
        &self,
        nats_msg: NatsMessage,
        handle_get_nodes: &mut GF,
    ) -> ResultType<()>
    where
        GF: 'static
            + FnMut(&str, &HashMap<String, String>) -> ResultType<Vec<NodeDescription>>
            + Send,
    {
        let msg = unmarshal(nats_msg.data.clone()).ok().unwrap();
        let req = match msg.union {
            Some(discovery_message::Union::Request(req)) => req,
            _ => {
                return Err(anyhow!("Not found request type message"));
            }
        };
        let nats_conn = self.nats_conn.as_ref();
        let action = req.action.unwrap();
        match action {
            Action::BROADCAST => {
                if let Ok(nodes) = handle_get_nodes(&req.service, &req.params) {
                    let raw_data = req.raw_data;
                    let source_nid = req.params.get("source_nid");
                    for node in nodes {
                        if source_nid.is_some() && source_nid.unwrap() == &node.nid {
                            log::warn!(
                                "Found source node id: {}, skip forward message!",
                                &node.nid
                            );
                            continue;
                        }
                        let event_topic = &node.event_topic;
                        if nats_msg.reply.is_some() {
                            if let Some(reply) =
                                nats_conn.request(event_topic, &raw_data).await.ok()
                            {
                                log::warn!(
                                    "Got braodcast reply and return to source node!, message: {:?}",
                                    &reply.data
                                );
                                nats_msg.respond(reply.data).await.ok();
                            }
                            continue;
                        }
                        nats_conn.publish(event_topic, &raw_data).await.ok();
                    }
                }
            }
            _ => {
                log::error!("Not match the broadcast request: {:?}", req);
            }
        }
        Ok(())
    }

    pub async fn handle_nats_msg<AF, GF>(
        &self,
        nats_msg: NatsMessage,
        nodes: Arc<RwLock<HashMap<String, NodeItem>>>,
        handle_node_action: &mut AF,
        handle_get_nodes: &mut GF,
    ) -> ResultType<()>
    where
        AF: 'static + FnMut(Action, &NodeDescription) -> ResultType<()> + Send,
        GF: 'static
            + FnMut(&str, &HashMap<String, String>) -> ResultType<Vec<NodeDescription>>
            + Send,
    {
        let expire_duration_secs = self.expire_duration_secs.whole_seconds() as u64;
        log::info!("handle storage key: {}", nats_msg.subject);
        let msg = unmarshal(nats_msg.data.clone()).ok().unwrap();
        let mut resp = DiscoveryMessage::new();
        resp.mut_response().success = true;
        let mut req = match msg.union {
            Some(discovery_message::Union::Request(req)) => req,
            _ => {
                return Err(anyhow!("Not found request type message"));
            }
        };
        let nats_conn = self.nats_conn.as_ref();
        let action = req.action.unwrap();
        match action {
            Action::GET => {
                log::info!("node.get");
                if let Ok(nodes) = handle_get_nodes(&req.service, &req.params) {
                    resp.set_nodes(Nodes {
                        nodes_item: nodes,
                        ..Default::default()
                    });
                    let repley_data = marshal(resp.clone()).ok().unwrap();
                    nats_msg.respond(repley_data).await.ok();
                }
                log::info!("node.get done");
            }
            Action::SAVE => {
                log::info!("node.save");
                let node = req.node.take().unwrap();
                if let Err(error) = handle_node_action(action, &node) {
                    log::info!("aciont {:?}, rejected {:?}", action, error);
                    let data = Response {
                        success: false,
                        reason: error.to_string(),
                        ..Default::default()
                    };
                    resp.set_response(data);
                }

                let discovery_subj = nats_msg
                    .subject
                    .replace(DEFAULT_PUBLISH_PRIFEX, DEFAULT_DISCOVERY_PRIFEX);
                log::info!("node.save publish topic: {}", &discovery_subj);
                nats_conn.publish(&discovery_subj, nats_msg.data).await.ok();
                nodes.write().unwrap().insert(
                    node.nid.clone(),
                    NodeItem {
                        expire: parse_timestamp_secs(SystemTime::now()) + expire_duration_secs,
                        description: Some(node.clone()).into(),
                        subj: nats_msg.subject,
                        ..Default::default()
                    },
                );
                log::info!("node.save done");
            }
            Action::UPDATE => {
                let node = req.node.take().unwrap();
                if !nodes.read().unwrap().contains_key(&node.nid) {
                    log::info!("Not found node id to update.");
                    log::info!("node.save");
                    let action = Action::SAVE;
                    if let Err(error) = handle_node_action(action, &node) {
                        log::info!("aciont {:?}, rejected {:?}", action, error);
                        let data = Response {
                            success: false,
                            reason: error.to_string(),
                            ..Default::default()
                        };
                        resp.set_response(data);
                    }

                    let discovery_subj = nats_msg
                        .subject
                        .replace(DEFAULT_PUBLISH_PRIFEX, DEFAULT_DISCOVERY_PRIFEX);
                    log::info!("node.update->save publish topic: {}", &discovery_subj);
                    nats_conn.publish(&discovery_subj, nats_msg.data).await.ok();
                    nodes.write().unwrap().insert(
                        node.nid.clone(),
                        NodeItem {
                            expire: parse_timestamp_secs(SystemTime::now()) + expire_duration_secs,
                            description: Some(node.clone()).into(),
                            subj: nats_msg.subject,
                            ..Default::default()
                        },
                    );
                    log::info!("node.save done");
                } else if let Some(item) = nodes.write().unwrap().get_mut(&node.nid) {
                    log::info!("node.update");
                    item.expire = parse_timestamp_secs(SystemTime::now()) + expire_duration_secs;
                    if let Err(error) = handle_node_action(action, &node) {
                        log::info!("aciont {:?}, rejected {:?}", action, error);
                        let data = Response {
                            success: false,
                            reason: error.to_string(),
                            ..Default::default()
                        };
                        resp.set_response(data);
                    }
                    log::info!("node.update done");
                }
            }
            Action::DELETE => {
                log::info!("node.delete");
                let node = req.node.take().unwrap();
                if nodes.read().unwrap().contains_key(&node.nid) {
                    if let Err(error) = handle_node_action(action, &node) {
                        log::info!("aciont {:?}, rejected {:?}", action, error);
                        let data = Response {
                            success: false,
                            reason: error.to_string(),
                            ..Default::default()
                        };
                        resp.set_response(data);
                    } else {
                        let discovery_subj = nats_msg
                            .subject
                            .replace(DEFAULT_PUBLISH_PRIFEX, DEFAULT_DISCOVERY_PRIFEX);
                        log::info!("node.delete publish topic: {}", &discovery_subj);
                        nats_conn.publish(&discovery_subj, nats_msg.data).await.ok();
                    }
                    nodes.write().unwrap().remove(&node.nid);
                }
                log::info!("node.delete done");
            }
            _ => {
                log::error!("Not match the sepecified request: {:?}", req);
            }
        }
        let out_msg = marshal(resp).ok().unwrap();
        nats_conn.publish(&nats_msg.reply.unwrap(), out_msg).await?;
        Ok(())
    }

    async fn check_expires<AF>(
        &self,
        nodes: Arc<RwLock<HashMap<String, NodeItem>>>,
        now: SystemTime,
        handle_node_action: &mut AF,
    ) where
        AF: 'static + FnMut(Action, &NodeDescription) -> ResultType<()> + Send,
    {
        let mut removed_key: Vec<String> = Vec::new();
        {
            let now_timestamp = parse_timestamp_secs(now);
            let r_lock = nodes.read().unwrap();
            for (node_key, item) in r_lock.iter() {
                if item.expire <= now_timestamp {
                    log::info!("Found expire node... {:?}", item);
                    let new_subj = item
                        .subj
                        .replace(DEFAULT_PUBLISH_PRIFEX, DEFAULT_DISCOVERY_PRIFEX);
                    let mut dc = DiscoveryMessage::new();
                    dc.set_request(Request {
                        action: Action::DELETE.into(),
                        node: item.description.clone(),
                        ..Default::default()
                    });
                    let msg_out = dc.write_to_bytes().ok();
                    let nc = self.nats_conn.as_ref();
                    nc.publish(new_subj.as_str(), msg_out.unwrap()).await.ok();
                    handle_node_action(Action::DELETE, &item.description.clone().unwrap()).ok();
                    removed_key.push(node_key.clone());
                }
            }
        }

        let mut nodes = nodes.write().unwrap();
        for key in removed_key {
            nodes.remove(&key);
        }
    }

    async fn sub_publish_message(
        &self,
        conn: &Connection,
        sender: UnboundedSender<NatsMessage>,
    ) -> ResultType<()> {
        let subj = format!("{}.>", DEFAULT_PUBLISH_PRIFEX);
        let subj_pub = subj.as_str();
        log::info!("listen subj: {}", &subj_pub);
        let sub = conn.subscribe(subj_pub).await?;
        let _ = tokio::spawn(async move {
            let handler = move |msg| -> ResultType<()> {
                sender.send(msg)?;
                Ok(())
            };
            loop {
                tokio::select! {
                    Some(msg) = sub.next() => {
                        if let Err(e) = handler(msg) {
                            log::info!("Error in callback! {:?}", e);
                        }
                    }
                }
            }
        });
        Ok(())
    }

    async fn sub_broadcast_message(
        &self,
        conn: &Connection,
        sender: UnboundedSender<NatsMessage>,
    ) -> ResultType<()> {
        let subj = format!("{}.>", DEFAULT_NODE_BROADCAST_PRIFEX);
        let subj_pub = subj.as_str();
        let queue_group = "discovery_cluster";
        log::info!("listen subj: {}", &subj_pub);
        let sub = conn.queue_subscribe(subj_pub, queue_group).await?;
        let _ = tokio::spawn(async move {
            let handler = move |msg| -> ResultType<()> {
                sender.send(msg)?;
                Ok(())
            };
            loop {
                tokio::select! {
                    Some(msg) = sub.next() => {
                        if let Err(e) = handler(msg) {
                            log::info!("Error in callback! {:?}", e);
                        }
                    }
                }
            }
        });
        Ok(())
    }
}
