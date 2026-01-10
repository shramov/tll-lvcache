use tll::channel::Message;
use tll::channel::*;

use tll::channel::base::*;
use tll::config::Config;
use tll::scheme::native::Scheme;
use tll::{Error, Result};

use std::ops::Deref;

pub mod block_scheme;
pub mod cache;
pub mod http_scheme;
pub mod json;
pub mod rest;
pub mod store;

use crate::cache::*;
use crate::rest::LVRest;

type IterType = std::iter::Flatten<std::collections::hash_map::IntoValues<i32, Map>>;

#[derive(Default)]
struct LVCache {
    base: Base,
    seq: i64,
    storage: Option<store::Storage>,
    messages: std::collections::HashMap<i32, Map>,
    messages_init: Vec<(String, Vec<String>)>,
    master: Option<Channel>,
    iter: IterType,
}

impl Extension for LVCache {
    type Inner = Base;

    fn inner(&self) -> &Self::Inner {
        &self.base
    }
    fn inner_mut(&mut self) -> &mut Self::Inner {
        &mut self.base
    }
}

impl ChannelImpl for LVCache {
    fn channel_protocol() -> &'static str {
        "lvcache"
    }
    fn process_policy() -> ProcessPolicy {
        ProcessPolicy::Never
    }

    fn init(&mut self, url: &Config, master: Option<Channel>, context: &Context) -> Result<()> {
        self.inner_mut().init(url, master, context)?;

        if self.base.caps() & Caps::InOut == Caps::empty() {
            self.base.set_caps(Caps::Input);
        }
        if self.base.caps() & Caps::InOut == Caps::InOut {
            return Err(Error::from(format!("LVCache channel can be either in or out, not both")));
        }
        if self.base.caps() & Caps::Input != Caps::empty() {
            let mchannel = master.ok_or(Error::from("LVCache reader requires master channel"))?;
            channel_cast::<LVCache>(&mchannel).ok_or(Error::from("LVCache reader requires LVCache master channel"))?;
            self.master = master;
            return Ok(());
        }

        for (key, cfg) in url.browse("message.*") {
            let name = &key["message.".len()..];
            if let Some(v) = cfg.get_self() {
                let fields: Vec<String> = v.split(",").map(|x| x.trim().to_owned()).collect();
                for f in &fields {
                    if f == "" {
                        return Err(Error::from(format!("Empty field name for message '{}'", name)));
                    }
                }
                self.messages_init.push((name.to_owned(), fields));
            } else {
                return Err(Error::from(format!("Can not get value for {}", name)));
            }
        }

        if self.base().scheme_url.is_none() {
            return Err(Error::from("Channel needs scheme"));
        }

        let chain = self.config_chain(url);
        if let Some(filename) = chain.get("filename") {
            self.storage = Some(store::Storage::new(filename, &self.base)?);
        }

        self.base_mut().scheme_control = Some(tll::scheme::Scheme::new(block_scheme::SCHEME_STRING)?);
        Ok(())
    }

    fn open(&mut self, cfg: &Config) -> Result<()> {
        self.inner_mut().open(cfg)?;

        if self.base.caps() & Caps::Input != Caps::empty() {
            let master = channel_cast::<LVCache>(self.master.as_ref().unwrap()).unwrap();
            let scheme = master.base.scheme_data.as_ref().ok_or(Error::from("Master has no data scheme"))?;
            self.base.scheme_data = Some(scheme.copy());
            let messages = match cfg.get("filter") {
                Some(f) => {
                    if f != "" {
                        if let Some(m) = scheme.messages().find(|m| m.name() == f) {
                            let mut r = std::collections::HashMap::<i32, Map>::new();
                            if let Some(i) = master.messages.get(&m.msgid()) {
                                r.insert(m.msgid(), i.clone());
                            }
                            Some(r)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            }
            .unwrap_or(master.messages.clone());
            let total: i64 = messages.values().map(|e| e.map.len() as i64).sum();
            self.seq = master.seq - total;
            self.iter = messages.into_values().flatten();
            self.update_dcaps(DCaps::Process | DCaps::Pending, DCaps::Process | DCaps::Pending);
            return Ok(());
        }

        self.seq = -1;
        if let Some(scheme) = self.base().scheme_data.as_ref() {
            self.messages = self.build_map(&Scheme::from(scheme))?;
        } else {
            return Err(Error::from("Scheme is mandatory for aggregation channel"));
        }
        if let Some(s) = self.storage.as_mut() {
            self.seq = s.read(&mut self.messages).map_err(|e| format!("Failed to load messagess: {e}"))?;
        }
        Ok(())
    }

    fn close(&mut self, _force: bool) -> () {
        for (_, e) in &self.messages {
            tll::info!(self, "Buckets for {}", e.name);
            for (_, v) in &e.map {
                tll::info!(self, "Message {}", v.seq);
            }
        }
        self.messages.clear();
        self.iter = IterType::default();
    }

    #[inline(always)]
    fn post(&mut self, msg: &Message) -> Result<()> {
        match MsgType::from(msg.type_) {
            MsgType::Data => self.post_data(msg),
            MsgType::Control => self.post_control(msg),
            _ => Ok(()),
        }
    }

    fn process(&mut self) -> Result<i32> {
        if let Some((_, v)) = self.iter.next() {
            self.base.callback_data(&v);
        } else {
            self.set_state(State::Closing);
            self.close(true);
            self.set_state(State::Closed);
        }
        Ok(0)
    }
}

impl LVCache {
    fn post_data(&mut self, msg: &Message) -> Result<()> {
        if self.base.caps() & Caps::Input != Caps::empty() {
            return Err(Error::from("Can not post in reading mode"));
        }
        if let Some(entry) = self.messages.get_mut(&msg.msgid()) {
            self.seq = std::cmp::max(self.seq, msg.seq);
            entry.update(msg)
        } else {
            Ok(())
        }
    }

    fn post_control(&mut self, msg: &Message) -> Result<()> {
        if msg.msgid() != block_scheme::BLOCK_MSGID {
            return Ok(());
        }

        if self.seq >= 0 {
            if let Some(s) = self.storage.as_mut() {
                let total: i64 = self.messages.values().map(|e| e.map.len() as i64).sum();
                let iter = self.messages.values().flatten().map(|(_, v)| v.msg.deref());
                if let Err(e) = s.write(self.seq - total + 1, iter) {
                    tll::error!(self, "Failed to write data: {e}")
                }
            }
        }
        Ok(())
    }

    fn build_map(&self, scheme: &tll::scheme::native::Scheme) -> Result<std::collections::HashMap<i32, Map>> {
        let mut r = std::collections::HashMap::<i32, Map>::new();
        for (name, fields) in &self.messages_init {
            tll::debug!(self, "Prepare map for {}", name);
            let m =
                Map::new(scheme, name, fields).map_err(|e| Error::from(format!("Failed to build message '{}': {}", name, e)))?;
            r.insert(m.0, m.1);
        }
        Ok(r)
    }
}

tll::declare_channel_impl!(LVCache);
tll::declare_channel_module!(LVCache, LVRest);
