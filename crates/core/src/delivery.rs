//! Мост брокер → hub: брокер отдаёт пришедшие (в т.ч. с других нод) Reply локальным подписчикам.

use crate::hub::Hub;
use socket_broker::{ControlCommand, Delivery};
use socket_protocol::Reply;
use std::sync::Arc;

pub struct HubDelivery {
    pub hub: Arc<Hub>,
}

impl HubDelivery {
    pub fn new(hub: Arc<Hub>) -> Arc<Self> {
        Arc::new(Self { hub })
    }
}

impl Delivery for HubDelivery {
    fn deliver(&self, channel: &str, reply: Reply) {
        self.hub.fan_out(channel, reply);
    }

    fn control(&self, cmd: ControlCommand) {
        match cmd {
            ControlCommand::Disconnect { user, client, code, reason } => {
                self.hub.disconnect_matching(&user, &client, code, &reason);
            }
            ControlCommand::Unsubscribe { user, channel } => {
                self.hub.unsubscribe_matching(&user, &channel);
            }
        }
    }
}
