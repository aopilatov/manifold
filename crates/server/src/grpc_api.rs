//! Server API — gRPC адаптер (tonic). Тонкая обёртка над теми же `ApiService::api_*`.
//! Auth — API-ключ в metadata `authorization: apikey <key>`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use socket_protocol::server_api_server::ServerApi;
use socket_protocol::*;
use socket_core::api::ApiService;
use tonic::{Request, Response, Status, Streaming};

pub struct GrpcApi {
    pub api: Arc<ApiService>,
}

fn check(req: &tonic::metadata::MetadataMap, method: &str, api: &ApiService) -> Result<(), Status> {
    let key = req
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("apikey "))
        .unwrap_or("");
    if !key.is_empty()
        && api.cfg.api_keys.iter().any(|k| k.key == key && (k.allow.is_empty() || k.allow.iter().any(|a| a == method)))
    {
        Ok(())
    } else {
        Err(Status::unauthenticated("invalid api key"))
    }
}

fn opt(s: &str) -> Option<&str> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

fn broker_error(e: socket_broker::BrokerError) -> Error {
    Error { code: 110, message: e.to_string(), temporary: true }
}

#[tonic::async_trait]
impl ServerApi for GrpcApi {
    async fn publish(&self, req: Request<PublishApiRequest>) -> Result<Response<PublishApiResponse>, Status> {
        check(req.metadata(), "publish", &self.api)?;
        let r = req.into_inner();
        Ok(Response::new(match self.api.api_publish(&r.channel, r.data, opt(&r.idempotency_key)).await {
            Ok(pos) => PublishApiResponse { error: None, offset: pos.offset, epoch: pos.epoch },
            Err(e) => PublishApiResponse { error: Some(broker_error(e)), offset: 0, epoch: String::new() },
        }))
    }

    async fn broadcast(&self, req: Request<BroadcastApiRequest>) -> Result<Response<BroadcastApiResponse>, Status> {
        check(req.metadata(), "broadcast", &self.api)?;
        let r = req.into_inner();
        let offsets = self.api.api_broadcast(&r.channels, r.data).await;
        let responses: HashMap<String, PublishApiResponse> = offsets
            .into_iter()
            .map(|(ch, offset)| (ch, PublishApiResponse { error: None, offset, epoch: String::new() }))
            .collect();
        Ok(Response::new(BroadcastApiResponse { error: None, responses }))
    }

    async fn presence(&self, req: Request<PresenceApiRequest>) -> Result<Response<PresenceApiResponse>, Status> {
        check(req.metadata(), "presence", &self.api)?;
        let r = req.into_inner();
        Ok(Response::new(match self.api.api_presence(&r.channel).await {
            Ok(presence) => PresenceApiResponse { error: None, presence },
            Err(e) => PresenceApiResponse { error: Some(broker_error(e)), presence: HashMap::new() },
        }))
    }

    async fn presence_stats(&self, req: Request<PresenceStatsApiRequest>) -> Result<Response<PresenceStatsApiResponse>, Status> {
        check(req.metadata(), "presence", &self.api)?;
        let r = req.into_inner();
        match self.api.api_presence(&r.channel).await {
            Ok(p) => {
                let users: std::collections::HashSet<&str> = p.values().map(|i| i.user.as_str()).collect();
                Ok(Response::new(PresenceStatsApiResponse {
                    error: None,
                    num_clients: p.len() as u32,
                    num_users: users.len() as u32,
                }))
            }
            Err(e) => Ok(Response::new(PresenceStatsApiResponse {
                error: Some(broker_error(e)),
                num_clients: 0,
                num_users: 0,
            })),
        }
    }

    async fn history(&self, req: Request<HistoryApiRequest>) -> Result<Response<HistoryApiResponse>, Status> {
        check(req.metadata(), "history", &self.api)?;
        let r = req.into_inner();
        let limit = if r.limit > 0 { r.limit as usize } else { 100 };
        Ok(Response::new(match self.api.api_history(&r.channel, limit).await {
            Ok((publications, position)) => HistoryApiResponse { error: None, publications, position: Some(position) },
            Err(e) => HistoryApiResponse { error: Some(broker_error(e)), publications: vec![], position: None },
        }))
    }

    async fn history_remove(&self, req: Request<HistoryRemoveApiRequest>) -> Result<Response<HistoryRemoveApiResponse>, Status> {
        check(req.metadata(), "history", &self.api)?;
        // TODO(impl): очистка истории канала в брокере.
        Ok(Response::new(HistoryRemoveApiResponse { error: None }))
    }

    async fn subscribe(&self, req: Request<SubscribeApiRequest>) -> Result<Response<SubscribeApiResponse>, Status> {
        check(req.metadata(), "subscribe", &self.api)?;
        // TODO(impl): сервер-инициированная подписка (нужен control-канал subscribe).
        Err(Status::unimplemented("server-side subscribe: TODO"))
    }

    async fn unsubscribe(&self, req: Request<UnsubscribeApiRequest>) -> Result<Response<UnsubscribeApiResponse>, Status> {
        check(req.metadata(), "unsubscribe", &self.api)?;
        let r = req.into_inner();
        self.api.api_unsubscribe(&r.user, &r.channel).await;
        Ok(Response::new(UnsubscribeApiResponse { error: None }))
    }

    async fn disconnect(&self, req: Request<DisconnectApiRequest>) -> Result<Response<DisconnectApiResponse>, Status> {
        check(req.metadata(), "disconnect", &self.api)?;
        let r = req.into_inner();
        self.api.api_disconnect(&r.user, &r.client, r.code, &r.reason).await;
        Ok(Response::new(DisconnectApiResponse { error: None }))
    }

    async fn user_online(&self, req: Request<UserOnlineApiRequest>) -> Result<Response<UserOnlineApiResponse>, Status> {
        check(req.metadata(), "user_online", &self.api)?;
        let r = req.into_inner();
        let (online, n) = self.api.api_user_online(&r.user);
        Ok(Response::new(UserOnlineApiResponse { error: None, online, num_connections: n as u32 }))
    }

    async fn channels(&self, req: Request<ChannelsApiRequest>) -> Result<Response<ChannelsApiResponse>, Status> {
        check(req.metadata(), "channels", &self.api)?;
        let r = req.into_inner();
        let channels = self.api.api_channels(opt(&r.pattern));
        Ok(Response::new(ChannelsApiResponse { error: None, channels }))
    }

    async fn info(&self, req: Request<InfoApiRequest>) -> Result<Response<InfoApiResponse>, Status> {
        check(req.metadata(), "info", &self.api)?;
        let s = self.api.api_info();
        Ok(Response::new(InfoApiResponse {
            error: None,
            nodes: vec![NodeInfo {
                name: s.node,
                num_clients: s.num_connections as u32,
                num_channels: s.num_channels as u32,
                uptime_s: 0,
            }],
        }))
    }

    async fn batch(&self, req: Request<BatchApiRequest>) -> Result<Response<BatchApiResponse>, Status> {
        check(req.metadata(), "publish", &self.api)?;
        // TODO(impl): применить пачку команд.
        Err(Status::unimplemented("batch: TODO"))
    }

    type PublishStreamStream =
        Pin<Box<dyn futures::Stream<Item = Result<PublishApiResponse, Status>> + Send + 'static>>;

    async fn publish_stream(
        &self,
        req: Request<Streaming<PublishApiRequest>>,
    ) -> Result<Response<Self::PublishStreamStream>, Status> {
        check(req.metadata(), "publish", &self.api)?;
        // TODO(impl): bidi-стрим публикаций (high-throughput).
        Err(Status::unimplemented("publish_stream: TODO"))
    }
}
