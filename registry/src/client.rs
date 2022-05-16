use crate::utils::{self, create_nats_connection, marshal, unmarshal};
use crate::{
    protos::registry::*, ResultType, DEFAULT_DISCOVERY_PRIFEX, DEFAULT_NODE_BROADCAST_PRIFEX,
    DEFAULT_NODE_EVENT_PRIFEX, DEFAULT_PUBLISH_PRIFEX,
};
use anyhow::anyhow;
use log;
use nats::asynk::{Connection, Message as NatsMessage};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::{self, task::JoinHandle, time::interval};

static DISCOVERY_SERVICE_NAME: &str = "discovery";

struct ActionHandler {
    receiver: mpsc::Receiver<(Action, NodeDescription)>,
    nats_conn: Arc<RwLock<Connection>>,
}

impl ActionHandler {
    pub fn new(
        nats_conn: Arc<RwLock<Connection>>,
        receiver: mpsc::Receiver<(Action, NodeDescription)>,
    ) -> Self {
        Self {
            receiver,
            nats_conn,
        }
    }

    pub async fn run(&mut self) {
        while let Some((action, node)) = self.receiver.recv().await {
            self.send_action(&node, action).await.unwrap_or_else(|e| {
                log::info!("Get action reply error: {:?}", e);
            });
        }
    }

    pub async fn send_action(&self, node: &NodeDescription, action: Action) -> ResultType<()> {
        let mut out_msg = DiscoveryMessage::new();
        out_msg.set_request(Request {
            action: action.into(),
            node: Some(node.clone()).into(),
            ..Default::default()
        });
        let subj = format!("{}.{}.{}", DEFAULT_PUBLISH_PRIFEX, &node.service, &node.nid);
        log::debug!("request: {}, action: {:?}", &subj, action);
        let data = utils::marshal(out_msg).ok().unwrap();
        let nat_conn = self.nats_conn.read().await;
        let reply = nat_conn
            .request_timeout(&subj, data, Duration::from_secs(5))
            .await;
        match reply {
            Ok(rep_msg) => {
                log::debug!(
                    "Got response data from discovery service->{:?}",
                    rep_msg.data
                );
                let reply_message = utils::unmarshal(rep_msg.data).ok().unwrap();
                match reply_message.union {
                    Some(discovery_message::Union::Response(response)) => {
                        log::debug!("Action response: {:?}", &response);
                        if !response.success {
                            return Err(anyhow!(format!(
                                "[{:?}] response error: {:?}",
                                action, response.reason
                            )));
                        }
                    }
                    _ => {
                        return Err(anyhow!(format!(
                            "Not found nodes response data! {:?}",
                            reply_message.union
                        )));
                    }
                }
            }
            Err(e) => {
                return Err(anyhow!("Node start error: err={}, id={:?}", e, &node.nid));
            }
        }
        Ok(())
    }
}

pub struct Client {
    nats_conn: Arc<RwLock<Connection>>,
    action_sender: mpsc::Sender<(Action, NodeDescription)>,
    current_node: Arc<RwLock<Option<NodeDescription>>>,
    keep_alive_handler: Option<JoinHandle<()>>,
    reply_topic: Option<String>,
}

impl Client {
    pub async fn new(
        nats_addr: &Vec<String>,
        user: Option<&str>,
        password: Option<&str>,
    ) -> ResultType<Self> {
        let nats_uri = nats_addr.join(",");
        let nat_client = create_nats_connection(&nats_uri, user, password).await?;
        let nats_conn = Arc::new(RwLock::new(nat_client));
        let (action_sender, action_receiver) = mpsc::channel(10);
        let mut action_handler = ActionHandler::new(nats_conn.clone(), action_receiver);
        let _ = tokio::spawn(async move {
            action_handler.run().await;
        });
        Ok(Self {
            nats_conn,
            action_sender,
            current_node: Default::default(),
            keep_alive_handler: None,
            reply_topic: None,
        })
    }

    pub async fn start(
        &mut self,
        mut node: NodeDescription,
    ) -> ResultType<(
        mpsc::Receiver<(NodeState, NodeDescription)>,
        mpsc::UnboundedReceiver<NatsMessage>,
    )> {
        self.current_node = Arc::new(RwLock::new(Some(node.clone())));

        let (node_state_sender, node_state_receiver) = mpsc::channel(10);
        let (node_event_sender, node_event_receiver) = mpsc::unbounded_channel();
        self.watch(node.service.clone(), node_state_sender).await?;
        self.listen_event(&mut node, node_event_sender).await?;
        self.keep_alive().await?;

        Ok((node_state_receiver, node_event_receiver))
    }

    pub async fn stop(&self) -> ResultType<()> {
        if let Some(handler) = &self.keep_alive_handler {
            log::info!("Stop keep alive");
            handler.abort();
        }

        if let Some(node) = self.current_node.read().await.as_ref() {
            log::info!("Unregister current node.");
            self.send_action(node, Action::DELETE).await.ok();
        }
        Ok(())
    }

    async fn send_action(&self, node: &NodeDescription, action: Action) -> ResultType<()> {
        log::info!("Send to action {:?}", action);
        self.action_sender.send((action, node.clone())).await?;
        Ok(())
    }

    async fn watch(
        &self,
        service: String,
        state_sender: mpsc::Sender<(NodeState, NodeDescription)>,
    ) -> ResultType<()> {
        let subj = format!("{}.{}.>", DEFAULT_DISCOVERY_PRIFEX, &service);
        log::info!("sub: {}", &subj);
        let sub = self.nats_conn.read().await.subscribe(&subj).await?;
        tokio::spawn(async move {
            log::info!("Start node message subscriber...");
            loop {
                tokio::select! {
                    Some(msg) = sub.next() => {
                        log::info!("Got nat message, topic:{}, msg: {:?}", &service, msg);
                        if let Ok(state) = Client::handle_nat_message(msg) {
                            state_sender.send(state).await.ok();
                            log::info!("Node state callback done...");
                        }
                    }
                }
            }
        });
        Ok(())
    }

    async fn listen_event(
        &mut self,
        node: &mut NodeDescription,
        state_sender: mpsc::UnboundedSender<NatsMessage>,
    ) -> ResultType<()> {
        let event_topic = format!(
            "{}.{}.{}.{}",
            DEFAULT_NODE_EVENT_PRIFEX, &node.data_center, &node.service, &node.nid
        );
        log::info!("event sub topic: {}", &event_topic);
        let sub = self.nats_conn.read().await.subscribe(&event_topic).await?;
        node.event_topic = event_topic.clone();
        self.reply_topic = Some(event_topic);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                   Some(msg) = sub.next() => {
                        log::info!("Got node event message: {:?}", msg);
                        state_sender.send(msg).ok();
                    }
                }
            }
        });
        Ok(())
    }

    async fn keep_alive(&mut self) -> ResultType<()> {
        if let Some(node) = self.current_node.read().await.as_ref() {
            self.action_sender
                .send((Action::SAVE, node.clone()))
                .await
                .ok();
        }

        log::info!("Keep alive...");
        let mut timer = interval(Duration::from_secs(2));
        let action_sender = self.action_sender.clone();
        let node = self.current_node.clone();
        let handler = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        log::debug!("Sedning update...");
                        if let Some(node) = node.read().await.as_ref() {
                            action_sender.send((Action::UPDATE, node.clone())).await.ok();
                        }
                    }
                }
            }
        });
        self.keep_alive_handler = Some(handler);
        Ok(())
    }

    pub async fn get_nodes(&self, service: &str, params: HashMap<String, String>) -> Option<Nodes> {
        let mut out_msg = DiscoveryMessage::new();
        out_msg.set_request(Request {
            action: Action::GET.into(),
            service: service.to_string(),
            params: params,
            ..Default::default()
        });
        let data = marshal(out_msg).ok().unwrap();
        let pubj = format!("{}.{}", DEFAULT_PUBLISH_PRIFEX, &service);
        log::info!("Get: pubj={}", pubj);
        let nc = self.nats_conn.read().await;
        match nc
            .request_timeout(&pubj, data, Duration::from_secs(5))
            .await
        {
            Ok(response) => {
                let resp_msg = unmarshal(response.data).ok().unwrap();
                match resp_msg.union {
                    Some(discovery_message::Union::Nodes(nodes)) => {
                        log::info!("Nodes: {:?}", nodes);
                        return Some(nodes);
                    }
                    _ => {
                        log::info!("Not found nodes response data!");
                    }
                }
            }
            Err(e) => {
                log::info!("Get: service={}, err={:?}", &service, e);
            }
        }
        None
    }

    pub async fn send_event(
        &self,
        node: &NodeDescription,
        payload: Vec<u8>,
    ) -> ResultType<Option<NatsMessage>> {
        let pub_topic = format!(
            "{}.{}.{}.{}",
            DEFAULT_NODE_EVENT_PRIFEX, &node.data_center, &node.service, &node.nid
        );
        log::info!("send event on topic: {:?}", &pub_topic);
        let relpay = self
            .nats_conn
            .read()
            .await
            .request_timeout(&pub_topic, payload, Duration::from_secs(5))
            .await
            .ok();
        Ok(relpay)
    }

    pub async fn broadcast_event(
        &self,
        service: &str,
        mut params: HashMap<String, String>,
        need_reley: bool,
        pyload: Vec<u8>,
    ) -> ResultType<()> {
        if let Some(current_node) = self.current_node.read().await.as_ref() {
            params.insert("source_nid".to_string(), current_node.nid.clone());
        }
        let mut out_msg = DiscoveryMessage::new();
        let pubj = format!(
            "{}.{}",
            DEFAULT_NODE_BROADCAST_PRIFEX, DISCOVERY_SERVICE_NAME
        );
        out_msg.set_request(Request {
            action: Action::BROADCAST.into(),
            service: service.to_string(),
            params,
            raw_data: pyload,
            ..Default::default()
        });
        let data = marshal(out_msg).ok().unwrap();
        log::info!("Get: pubj={}", pubj);
        let nc = self.nats_conn.read().await;
        if need_reley {
            if let Some(reply_topic) = &self.reply_topic {
                nc.publish_request(&pubj, reply_topic, data).await.ok();
            }
        } else {
            nc.publish(&pubj, data).await.ok();
        }
        Ok(())
    }

    fn handle_nat_message(msg: NatsMessage) -> ResultType<(NodeState, NodeDescription)> {
        log::info!("Handle discovery message: {:?}", &msg.subject);
        let resp_msg = unmarshal(msg.data).ok().unwrap();
        match resp_msg.union {
            Some(discovery_message::Union::Request(event)) => {
                log::info!("Event: {:?}", &event);
                let node = event.node.clone().unwrap();
                let nid = &node.nid;
                match event.action.unwrap() {
                    Action::SAVE => {
                        log::info!("Node.save: {}", nid);
                        return Ok((NodeState::NODE_UP, node));
                    }
                    Action::DELETE => {
                        log::info!("Node.delete: {}", nid);
                        return Ok((NodeState::NODE_DOWN, node));
                    }
                    _ => {
                        return Err(anyhow!(format!(
                            "handleNatsMsg: err => unkonw message, data: {:?}",
                            event
                        )));
                    }
                }
            }
            _ => {
                return Err(anyhow!(format!("Not found nodes response data!")));
            }
        }
    }

    pub async fn modify_current_node(&mut self, key: String, value: String) {
        self.current_node
            .write()
            .await
            .as_mut()
            .map(|node| node.extra_info.insert(key, value));
    }
}
