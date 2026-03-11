use std::path::PathBuf;
use tll::channel::base::Base;
use tll::channel::*;

#[derive(Debug)]
pub struct Storage {
    pub path: PathBuf,
    pub writer: OwnedChannel,
    pub reader: OwnedChannel,
}

struct MapRef<'a> {
    seq: i64,
    map: &'a mut crate::MapMap,
}

impl<'a> CallbackMut<()> for MapRef<'a> {
    fn message_callback_mut(&mut self, _c: &Channel, m: &Message) -> i32 {
        if m.get_type() == MsgType::Data {
            if let Some(entry) = self.map.get_mut(&m.msgid()) {
                self.seq = m.seq();
                let _ = entry.update(m);
            }
        }
        0
    }
}

impl Storage {
    pub fn new(filename: String, base: &Base) -> Result<Self, tll::Error> {
        let mut cfg = tll::config::Config::new();
        cfg.set("tll.proto", "file");
        cfg.set("tll.host", &filename);
        cfg.set("tll.internal", "yes");
        cfg.set("name", &format!("{}/reader", base.name()));
        cfg.set("dump", "yes");
        let reader = base.context().channel_url(&cfg).map_err(|_| "Failed to create reader channel")?;
        cfg.set("scheme", &base.scheme_url.as_ref().unwrap());
        cfg.set("name", &format!("{}/writer", base.name()));
        cfg.set("dir", "w");
        let writer = base.context().channel_url(&cfg).map_err(|_| "Failed to create writer channel")?;
        Ok(Storage {
            path: filename.into(),
            writer,
            reader,
        })
    }

    pub fn write<'a, I: Iterator<Item = &'a Message>>(&mut self, mut seq: i64, iter: I) -> tll::result::Result<()> {
        let w = &mut self.writer;
        if w.state() != State::Closed {
            w.close();
        }
        let mut m = Message::new();
        w.open(Some("overwrite=yes"))?;
        for v in iter {
            unsafe { std::ptr::copy(v, &mut m, 1) };
            w.post(m.set_seq(seq))?;
            seq += 1;
        }
        w.close();
        Ok(())
    }

    pub fn read<'a>(&mut self, messages: &'a mut crate::MapMap) -> Result<i64, tll::Error> {
        if !self.path.exists() {
            return Ok(-1);
        }
        let mut mapref = MapRef { seq: -1, map: messages };
        let _cb = self.reader.callback_add_mut::<MapRef<'a>, ()>(&mut mapref, MsgMask::Data)?;
        self.reader.open(None)?;
        while self.reader.state() == State::Active {
            self.reader.process()?;
        }
        self.reader.close();
        Ok(mapref.seq)
    }
}
