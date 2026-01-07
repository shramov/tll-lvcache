use tll::scheme::native::Scheme;
use tll::scheme::serde::{DataMessage, Serialize};

#[derive(Debug, Default)]
pub struct JSON {
    pub scheme: Scheme,
    pub buf: Vec<u8>,
}

impl JSON {
    pub fn new(scheme: Scheme) -> Self {
        JSON { scheme, buf: Vec::new() }
    }

    pub fn encode(&mut self, msg: &tll::channel::Message) -> tll::Result<&'_ [u8]> {
        let message = self
            .scheme
            .messages
            .iter()
            .find(|x| x.msgid == msg.msgid())
            .ok_or(tll::Error::from("Message not found"))?;
        self.buf.clear();
        let mut ser = serde_json::Serializer::new(&mut self.buf);
        DataMessage {
            data: msg.data(),
            desc: message.clone(),
        }
        .serialize(&mut ser)
        .map_err(|e| tll::Error::from(format!("JSON serialization failed: {:?}", e)))?;
        Ok(&self.buf)
    }
}
