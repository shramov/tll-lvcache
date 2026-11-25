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
pub type MapMap = std::collections::HashMap<i32, Map>;

#[derive(Debug, Clone)]
pub struct MapParam {
    pub name: String,
    pub key: Vec<String>,
    pub erase: Option<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct EraseKey {
    pub field: Field,
    pub key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Map {
    pub map: MsgMap,
    pub key: Vec<Field>,
    pub key_size: usize,
    pub erase: Option<EraseKey>,
    pub name: String,
}

impl<'a> std::iter::IntoIterator for &'a Map {
    type Item = <&'a MsgMap as IntoIterator>::Item;
    type IntoIter = <&'a MsgMap as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter()
    }
}

impl std::iter::IntoIterator for Map {
    type Item = <MsgMap as IntoIterator>::Item;
    type IntoIter = <MsgMap as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

impl Map {
    pub fn new(scheme: &Scheme, param: &MapParam) -> Result<(i32, Map)> {
        let message = scheme.messages.iter().find(|m| m.name == param.name).ok_or(Error::from("Not found in scheme"))?;
        if message.msgid == 0 {
            return Err(Error::from("Message without msgid"));
        }
        let mut r = Map {
            map: MsgMap::new(),
            key: Vec::<Field>::with_capacity(param.key.len()),
            key_size: 0,
            erase: None,
            name: message.name.clone(),
        };
        for fname in &param.key {
            let f = lookup_field(message, fname)?;
            check_field(&f)?;
            r.key_size += f.size;
            r.key.push(f);
        }
        if let Some((ename, evalue)) = &param.erase {
            let f = lookup_field(message, &ename)?;
            let buf = string2field(&f, evalue)?;
            r.erase = Some(EraseKey { field: f, key: buf });
        }
        Ok((message.msgid, r))
    }

    pub fn update(&mut self, msg: &Message) -> Result<()> {
        let mut key = Vec::<u8>::with_capacity(self.key_size);
        for f in &self.key {
            key.put_slice(extract_field(f, msg.data())?);
        }

        if let Some(erase) = &self.erase {
            let ekey = extract_field(&erase.field, msg.data())?;
            if ekey == erase.key {
                self.map.remove(&key);
                return Ok(());
            }
        }

        self.map.insert(key, Rc::new(Body::new(msg.into())));
        Ok(())
    }
}

pub fn parse_params(config: &tll::config::Config) -> Result<Vec<MapParam>> {
    config
        .browse("message.*")
        .into_iter()
        .map(|(key, cfg)| {
            let name = &key["message.".len()..];
            let (fstring, erase) = if let Some(v) = cfg.get_self() {
                (v, None)
            } else {
                (
                    cfg.get("key").ok_or(format!("Empty key for message {}", name))?,
                    if let Some(ekey) = cfg.get("erase-key") {
                        Some((
                            ekey,
                            cfg.get("erase-value").ok_or(format!("Empty erase-value for message {}", name))?,
                        ))
                    } else {
                        None
                    },
                )
            };
            let fields: Vec<String> = fstring.split(",").map(|x| x.trim().to_owned()).collect();
            for f in &fields {
                if f == "" {
                    return Err(Error::from(format!("Empty field name for message '{}'", name)));
                }
            }
            Ok(MapParam {
                name: name.to_owned(),
                key: fields,
                erase: erase,
            })
        })
        .collect()
}

pub fn string2field(f: &Field, value: &str) -> Result<Vec<u8>> {
    match &f.sub_type {
        SubType::Enum(e) => {
            let value = e
                .values
                .iter()
                .find(|v| v.name == value)
                .ok_or(format!("Value {} not found for enum {}", value, e.name))?;
            string2field_raw(f, &value.value.to_string())
        }
        _ => string2field_raw(f, value),
    }
}

pub fn string2field_raw(f: &Field, value: &str) -> Result<Vec<u8>> {
    Ok(match &f.field_type {
        Type::Int8 => value.parse::<i8>()?.to_le_bytes().to_vec(),
        Type::Int16 => value.parse::<i16>()?.to_le_bytes().to_vec(),
        Type::Int32 => value.parse::<i32>()?.to_le_bytes().to_vec(),
        Type::Int64 => value.parse::<i64>()?.to_le_bytes().to_vec(),
        Type::UInt8 => value.parse::<u8>()?.to_le_bytes().to_vec(),
        Type::UInt16 => value.parse::<u16>()?.to_le_bytes().to_vec(),
        Type::UInt32 => value.parse::<u32>()?.to_le_bytes().to_vec(),
        Type::UInt64 => value.parse::<u64>()?.to_le_bytes().to_vec(),
        Type::Double => value.parse::<f64>()?.to_le_bytes().to_vec(),
        _ => return Err(Error::from("Invalid field type")),
    })
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
