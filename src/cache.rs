use std::ops::Deref;
use std::rc::Rc;

use bytes::BufMut;
use tll::channel::{Message, OwnedMessage};
use tll::scheme::native::*;
use tll::{Error, Result};

pub type Key = Vec<u8>;

#[derive(Debug, Clone)]
pub struct Body {
    pub msg: OwnedMessage,
    pub json: Option<Vec<u8>>,
}

impl Deref for Body {
    type Target = OwnedMessage;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.msg
    }
}

impl Body {
    pub fn new(msg: OwnedMessage) -> Self {
        Self { msg, json: None }
    }
}

pub type MsgMap = im_rc::HashMap<Key, Rc<Body>>;

#[derive(Debug, Clone)]
pub struct Map {
    pub map: MsgMap,
    pub key: Vec<Field>,
    pub key_size: usize,
    pub name: String,
}

impl std::iter::IntoIterator for Map {
    type Item = <MsgMap as IntoIterator>::Item;
    type IntoIter = <MsgMap as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

impl Map {
    pub fn new(scheme: &Scheme, name: &str, fields: &Vec<String>) -> Result<(i32, Map)> {
        let message = scheme.messages.iter().find(|m| m.name == name).ok_or(Error::from("Not found in scheme"))?;
        if message.msgid == 0 {
            return Err(Error::from("Message without msgid"));
        }
        let mut r = Map {
            map: MsgMap::new(),
            key: Vec::<Field>::with_capacity(fields.len()),
            key_size: 0,
            name: message.name.clone(),
        };
        for fname in fields {
            let f = lookup_field(message, fname)?;
            check_field(&f)?;
            r.key_size += f.size;
            r.key.push(f);
        }
        Ok((message.msgid, r))
    }

    pub fn update(&mut self, msg: &Message) -> Result<()> {
        let mut key = Vec::<u8>::with_capacity(self.key_size);
        for f in &self.key {
            key.put_slice(extract_field(f, msg.data())?);
        }
        self.map.insert(key, Rc::new(Body::new(msg.into())));
        Ok(())
    }
}

pub fn extract_field<'a>(f: &Field, data: &'a [u8]) -> Result<&'a [u8]> {
    if f.offset + f.size > data.len() {
        return Err(Error::from(format!(
            "Key {} out of message: {}+{} > message size {}",
            f.name,
            f.offset,
            f.size,
            data.len()
        )));
    }
    Ok(&data[f.offset..f.offset + f.size])
}

pub fn check_field(field: &Field) -> Result<()> {
    match &field.field_type {
        Type::Int8 => Ok(()),
        Type::Int16 => Ok(()),
        Type::Int32 => Ok(()),
        Type::Int64 => Ok(()),
        Type::UInt8 => Ok(()),
        Type::UInt16 => Ok(()),
        Type::UInt32 => Ok(()),
        Type::UInt64 => Ok(()),
        Type::Double => Ok(()),
        Type::Bytes(_) => Ok(()),
        Type::Pointer { .. } => Err(Error::from("Offset pointers are not supported")),
        t => Err(Error::from(format!("Unsupported type: {:?}", t))),
    }
}

pub fn lookup_field(message: &tll::scheme::native::Message, fname: &str) -> Result<Field> {
    let mut m = Some(message);
    let mut field: Option<&Field> = None;
    let mut offset = 0usize;
    for p in fname.split(".") {
        let f = m
            .ok_or(Error::from("Non-message field in path"))?
            .fields
            .iter()
            .find(|f| f.name == p)
            .ok_or(Error::from(format!("Field '{}' not found", p)))?;
        m = if let Type::Message(fm) = &f.field_type {
            offset += f.offset;
            Some(fm)
        } else {
            None
        };

        field = Some(f);
    }
    let mut r = field.ok_or(Error::from("Field not found"))?.clone();
    r.name = fname.to_owned();
    r.offset += offset;
    Ok(r)
}
