use std::ops::Deref;
use std::str::FromStr;

use bytes::BufMut;

use tll::channel::Message;
use tll::channel::*;

use tll::bind::Binder;
use tll::channel::base::*;
use tll::channel::logic::Logic;
use tll::config::{Config, ConfigValue};
use tll::scheme::native::{Field, Scheme, Type};
use tll::{Error, Result};

use crate::cache::*;
use crate::http_scheme;
use crate::json::JSON;
use crate::store::Storage;

type IterType = std::iter::Flatten<std::collections::hash_map::IntoValues<i32, Map>>;

#[derive(Default, Debug, Clone)]
struct Filter {
    msgid: i32,
    offset: usize,
    blob: Vec<u8>,
    field: Option<Field>,
}

#[derive(Default)]
struct Client {
    addr: u64,
    seq: i64,
    sent: usize,
    iter: IterType,
    filter: Option<Filter>,
}

#[repr(transparent)]
#[derive(Debug, Default, Copy, Clone)]
struct OpenMode(i64);

impl ToString for OpenMode {
    fn to_string(&self) -> String {
        if self.0 == -1 { "initial" } else { "seq-data" }.to_owned()
    }
}

#[derive(Default)]
pub struct LVRest {
    base: Base,
    seq: ConfigValue<i64>,
    open_mode: ConfigValue<OpenMode>,
    messages: std::collections::HashMap<i32, Map>,
    messages_init: Vec<(String, Vec<String>)>,
    filters: std::collections::HashMap<String, Filter>,
    filters_init: Vec<(String, (String, String))>,
    clients: std::collections::HashMap<u64, Client>,
    clients_drop: Vec<u64>,
    json: JSON,
    msgid_connect: i32,
    msgid_disconnect: i32,
    input: Logic,
    output: Logic,
    storage: Option<Storage>,
    buf: Vec<u8>,
}

impl Extension for LVRest {
    type Inner = Base;

    fn inner(&self) -> &Self::Inner {
        &self.base
    }
    fn inner_mut(&mut self) -> &mut Self::Inner {
        &mut self.base
    }
}

enum InputCallback {}
enum OutputCallback {}

impl CallbackMut<InputCallback> for LVRest {
    fn message_callback_mut(&mut self, _c: &Channel, m: &Message) -> i32 {
        match self.on_input(m) {
            Ok(_) => 0,
            Err(e) => {
                tll::error!(self, "Callback failed: {e}");
                self.set_state(State::Error);
                EINVAL
            }
        }
    }
}

impl CallbackMut<OutputCallback> for LVRest {
    fn message_callback_mut(&mut self, _c: &Channel, m: &Message) -> i32 {
        match self.on_output(m) {
            Ok(_) => 0,
            Err(e) => {
                tll::error!(self, "Callback failed: {e}");
                EINVAL
            }
        }
    }
}

impl ChannelImpl for LVRest {
    fn channel_protocol() -> &'static str {
        "lvrest"
    }
    fn process_policy() -> ProcessPolicy {
        ProcessPolicy::Never
    }

    fn init(&mut self, url: &Config, master: Option<Channel>, context: &Context) -> Result<()> {
        self.inner_mut().init(url, master, context)?;
        self.open_mode = unsafe { &*(&self.seq as *const ConfigValue<i64> as *const ConfigValue<OpenMode>) }.clone();

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

        for (key, cfg) in url.browse("filter.*") {
            let name = &key["filter.".len()..];
            if let Some(v) = cfg.get_self() {
                let flt = v
                    .trim()
                    .split_once(".")
                    .ok_or(Error::from(format!("Filter must be Message.field.path format, got '{}'", v)))?;
                self.filters_init.push((name.to_owned(), (flt.0.to_owned(), flt.1.to_owned())))
            } else {
                return Err(Error::from(format!("Can not get value for {}", name)));
            }
        }

        if self.base().scheme_url.is_none() {
            return Err(Error::from("Channel needs scheme"));
        }

        self.input = Logic::new(context, url, "input")?
            .at_least(1)?
            .at_most(1)?
            .with_callback_mut::<Self, InputCallback>(self, MsgMask::All)?;

        self.output = Logic::new(context, url, "output")?
            .at_least(1)?
            .at_most(1)?
            .with_callback_mut::<Self, OutputCallback>(self, MsgMask::Control)?;

        if let Some(name) = url.get("filename") {
            self.storage = Some(Storage::new(name, &self.base)?);
        }

        Ok(())
    }

    fn open(&mut self, cfg: &Config) -> Result<()> {
        self.inner_mut().open(cfg)?;
        self.base.config.set_ptr("info.open.seq", &self.seq)?;
        self.base.config.set_ptr("info.open.mode", &self.open_mode)?;

        self.seq.set(-1);
        if let Some(scheme) = self.base.scheme_data.as_ref().map(Scheme::from) {
            self.messages = self.build_map(&scheme)?;
            self.filters = self.build_filters(&scheme)?;
            self.json = JSON::new(scheme);
        } else {
            return Err(Error::from("Scheme is mandatory for aggregation channel"));
        }
        if let Some(s) = self.output[0].scheme_control() {
            self.msgid_connect = s.messages().find(|x| x.name() == "Connect").ok_or(Error::from("No Connect message"))?.msgid();
            self.msgid_disconnect =
                s.messages().find(|x| x.name() == "Disconnect").ok_or(Error::from("No Disconnect message"))?.msgid();
        } else {
            return Err(Error::from("Control scheme not found on output"));
        }

        if let Some(s) = self.storage.as_mut() {
            self.seq.set(s.read(&mut self.messages).map_err(|e| format!("Failed to load messagess: {e}"))?);
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
        if self.seq.get() >= 0 {
            if let Some(s) = self.storage.as_mut() {
                let total: i64 = self.messages.values().map(|e| e.map.len() as i64).sum();
                let iter = self.messages.values().flatten().map(|(_, v)| v.msg.deref());
                if let Err(e) = s.write(self.seq.get() - total + 1, iter) {
                    tll::error!(self, "Failed to write data: {e}")
                }
            }
        }
        self.messages.clear();
        self.clients.clear();
        self.clients_drop.clear();
        self.base.config.set("info.open.seq", &self.seq.get().to_string());
        self.base.config.set("info.open.mode", &self.open_mode.get().to_string());
    }

    fn free(&mut self) -> () {
        self.input.clear();
        self.output.clear();
        self.base.free()
    }

    fn process(&mut self) -> Result<i32> {
        let mut output = self.output[0];

        for addr in self.clients_drop.drain(..) {
            tll::debug!(&self.base, "Drop client {}", addr);
            self.clients.remove(&addr);
        }

        for client in self.clients.values_mut() {
            self.buf.clear();
            let mut last = PostFlags::More;
            for _ in 0..10 {
                if let Some((_, mut v)) = client.iter.next() {
                    if let Some(flt) = client.filter.as_ref() {
                        let data = v.msg.data();
                        if data.len() < flt.offset + flt.blob.len() {
                            continue;
                        }
                        if &data[flt.offset..flt.offset + flt.blob.len()] != &flt.blob {
                            continue;
                        }
                    }

                    self.buf.put_u8(if client.sent == 0 { b'[' } else { b',' });
                    client.sent += 1;
                    client.seq += 1;
                    if v.json.is_none() {
                        let json = self.json.encode(&v.msg)?.to_vec();
                        unsafe { get_mut_unchecked(&mut v) }.json = Some(json);
                    }
                    if let Some(j) = v.json.as_ref() {
                        self.buf.reserve(j.len());
                        self.buf.put_slice(j);
                    }
                } else {
                    last = PostFlags::empty();
                    self.clients_drop.push(client.addr);
                    self.buf.put_u8(b']');
                    break;
                }
            }
            let mut msg = Message::new();
            msg.set_addr(client.addr);
            msg.set_data(&self.buf);
            output.post_flags(&msg, last)?;
        }

        for addr in self.clients_drop.drain(..) {
            tll::debug!(&self.base, "Drop client {}", addr);
            self.clients.remove(&addr);
        }

        if self.clients.len() == 0 {
            self.update_dcaps(DCaps::empty(), DCaps::Process | DCaps::Pending);
        }
        Ok(0)
    }
}

// std::rc::Rc::get_mut_unchecked
unsafe fn get_mut_unchecked<T>(this: &mut std::rc::Rc<T>) -> &mut T {
    &mut *(std::rc::Rc::as_ptr(this) as *mut T)
}

impl LVRest {
    fn on_input(&mut self, msg: &Message) -> Result<()> {
        if msg.get_type() != MsgType::Data {
            return Ok(());
        }
        if let Some(entry) = self.messages.get_mut(&msg.msgid()) {
            self.seq.set(msg.seq());
            entry.update(msg)
        } else {
            Ok(())
        }
    }

    fn on_output(&mut self, msg: &Message) -> Result<()> {
        if msg.get_type() != MsgType::Control {
            return Ok(());
        }
        let addr = msg.addr();
        let m = msg.msgid();
        if m == self.msgid_connect {
            if self.clients.get(&addr).is_some() {
                return Err(Error::from("Client already exists"));
            }
            let data = http_scheme::Connect::bind(msg.data())?;
            let path = data.get_path()?;
            let mut flt: Option<Filter> = None;
            let messages = if let Some((_, req)) = path.split_once('?') {
                let (name, value) = req.split_once('=').ok_or("Expected '=' in filter=value request")?;
                let filter = self.filters.get(name).ok_or("Filter not found")?;
                flt = Some(filter.fill(value)?);
                self.messages.clone()
            } else {
                self.messages.clone()
            };
            let total: i64 = messages.values().map(|e| e.map.len() as i64).sum();
            tll::info!(self, "New client {addr}, messages: {total}");
            self.clients.insert(
                addr,
                Client {
                    addr,
                    seq: self.seq.get() - total + 1,
                    iter: messages.into_values().flatten(),
                    filter: flt,
                    sent: 0,
                },
            );
            self.update_dcaps(DCaps::Process | DCaps::Pending, DCaps::Process | DCaps::Pending);
        } else if m == self.msgid_disconnect {
            if self.clients.get(&addr).is_some() {
                self.clients_drop.push(addr);
                self.update_dcaps(DCaps::Process | DCaps::Pending, DCaps::Process | DCaps::Pending);
            }
        }
        Ok(())
    }

    fn build_map(&self, scheme: &Scheme) -> Result<std::collections::HashMap<i32, Map>> {
        let mut r = std::collections::HashMap::<i32, Map>::new();
        for (name, fields) in &self.messages_init {
            tll::debug!(self, "Prepare map for {}", name);
            let m =
                Map::new(scheme, name, fields).map_err(|e| Error::from(format!("Failed to build message '{}': {}", name, e)))?;
            r.insert(m.0, m.1);
        }
        Ok(r)
    }

    fn build_filters(&self, scheme: &Scheme) -> Result<std::collections::HashMap<String, Filter>> {
        let mut r = std::collections::HashMap::<String, Filter>::new();
        for (name, (mname, fname)) in &self.filters_init {
            tll::debug!(self, "Prepare filter for {}: {}", name, mname);
            let msg = scheme
                .messages
                .iter()
                .find(|x| x.name == *mname)
                .ok_or(Error::from(format!("Message {} not found", mname)))?;
            let f = lookup_field(msg, fname)?;
            check_field(&f)?;
            r.insert(
                name.clone(),
                Filter {
                    msgid: msg.msgid,
                    offset: f.offset,
                    blob: vec![],
                    field: Some(f),
                },
            );
        }
        Ok(r)
    }
}

fn map_int_err(_: std::num::ParseIntError) -> Error {
    Error::from("Parse int error")
}

impl Filter {
    fn fill(&self, value: &str) -> Result<Filter> {
        let f = self.field.as_ref().ok_or(Error::from("Filter without field"))?;
        let blob = match &f.field_type {
            Type::Int8 => i8::from_str(value).map_err(map_int_err)?.to_ne_bytes()[..].to_vec(),
            Type::Int16 => i16::from_str(value).map_err(map_int_err)?.to_ne_bytes()[..].to_vec(),
            Type::Int32 => i32::from_str(value).map_err(map_int_err)?.to_ne_bytes()[..].to_vec(),
            Type::Int64 => i64::from_str(value).map_err(map_int_err)?.to_ne_bytes()[..].to_vec(),
            Type::UInt8 => u8::from_str(value).map_err(map_int_err)?.to_ne_bytes()[..].to_vec(),
            Type::UInt16 => u16::from_str(value).map_err(map_int_err)?.to_ne_bytes()[..].to_vec(),
            Type::UInt32 => u32::from_str(value).map_err(map_int_err)?.to_ne_bytes()[..].to_vec(),
            Type::UInt64 => u64::from_str(value).map_err(map_int_err)?.to_ne_bytes()[..].to_vec(),
            Type::Bytes(capacity) => {
                if value.as_bytes().len() <= *capacity {
                    let mut blob = value.as_bytes().to_vec();
                    blob.resize(*capacity, 0);
                    blob
                } else {
                    Err(Error::from("String overflow"))?
                }
            }
            _ => Err(Error::from("Invalid type"))?,
        };
        Ok(Filter {
            msgid: self.msgid,
            offset: f.offset,
            blob,
            field: None,
        })
    }
}

tll::declare_channel_impl!(LVRest);
