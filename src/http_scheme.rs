#![allow(dead_code, non_camel_case_types, non_upper_case_globals, non_snake_case)]

pub use tll::bind::*;

pub const SCHEME_STRING : &str = "yamls+gz://eJyNUFFPgzAYfN+v6FsTA4mbE5U3AlWWKCyOPRsCn9JktA3tTJTw3/06cDK3Rd/u613ueucSkdfgE0onhEhluBTaJy0tlHIto1VeAEW+Mka96KKCGmiHWhDbWvsICKFPYCpZoqo1HwrNuDC3zk5hrcI0SViYIe05hEbskWUMj2s8Hph9niKKWRAhnCFMl9kiTVZ43eC1DLIwRnxncbqy+isL1xbNEWXPQWj9MJGuk4jdLxJmrS67buJ+t4shL6GxHV85bMrh4y5pB77qeYf0Bag2DRdvu6Jj2Xu+2cIJ1T4nlEJAYWwQL7HZ2cC6n2xvNUz4O7CQ5SgPd516RxrNPw813vxIo3JT/dmuH0H/6C6G2UYFI66Lw46zsx3/83loGnlq9y9TNbxS";

#[repr(i8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    UNDEFINED = 0,
    GET = 1,
    HEAD = 2,
    POST = 3,
    PUT = 4,
    DELETE = 5,
    CONNECT = 6,
    OPTIONS = 7,
    TRACE = 8,
    PATCH = 9,
}
impl BinderCopy for Method {}

#[derive(Debug)]
pub struct Header<Buf: MemRead> {
    data: MemOffset<Buf>,
}
impl<Buf: MemRead + Copy> Binder<Buf> for Header<Buf> {
    fn bind_view(data: MemOffset<Buf>) -> Result<Self, BindError> {
        if data.mem_size() < 16 {
            return Err(BindError::new_size(16));
        }
        // Pointer
        // Pointer
        Ok(Self { data })
    }

    fn bind_unchecked(data: MemOffset<Buf>) -> Self {
        Self { data }
    }
}

impl<Buf: MemRead + Copy> Header<Buf> {
    pub fn get_header(&self) -> Result<&'_ str, StringBindError> {
        tll::bind::offset_str::<tll::bind::OffsetPtrDefault, Buf>(&self.data, 0)
    }
    pub fn get_value(&self) -> Result<&'_ str, StringBindError> {
        tll::bind::offset_str::<tll::bind::OffsetPtrDefault, Buf>(&self.data, 8)
    }
}
#[derive(Debug)]
pub struct Connect<Buf: MemRead> {
    data: MemOffset<Buf>,
}
impl<Buf: MemRead + Copy> Binder<Buf> for Connect<Buf> {
    fn bind_view(data: MemOffset<Buf>) -> Result<Self, BindError> {
        if data.mem_size() < 27 {
            return Err(BindError::new_size(27));
        }
        // Pointer
        // Pointer
        Ok(Self { data })
    }

    fn bind_unchecked(data: MemOffset<Buf>) -> Self {
        Self { data }
    }
}

impl<Buf: MemRead + Copy> Connect<Buf> {
    pub fn get_method(&self) -> Method {
        self.data.mem_get_primitive::<Method>(0)
    }
    pub fn get_code(&self) -> i16 {
        self.data.mem_get_primitive::<i16>(1)
    }
    pub fn get_size(&self) -> i64 {
        self.data.mem_get_primitive::<i64>(3)
    }
    pub fn get_path(&self) -> Result<&'_ str, StringBindError> {
        tll::bind::offset_str::<tll::bind::OffsetPtrDefault, Buf>(&self.data, 11)
    }
    pub fn get_headers(&self) -> Result<tll::bind::OffsetPtr<Header<Buf>, tll::bind::OffsetPtrDefault, Buf>, BindError> {
        tll::bind::OffsetPtr::<Header<Buf>, tll::bind::OffsetPtrDefault, Buf>::new(self.data.view(19))
    }
}
impl<Buf: MemRead> MsgId for Connect<Buf> {
    const MSGID: i32 = 1;
}
#[derive(Debug)]
pub struct Disconnect<Buf: MemRead> {
    data: MemOffset<Buf>,
}
impl<Buf: MemRead + Copy> Binder<Buf> for Disconnect<Buf> {
    fn bind_view(data: MemOffset<Buf>) -> Result<Self, BindError> {
        if data.mem_size() < 10 {
            return Err(BindError::new_size(10));
        }
        // Pointer
        Ok(Self { data })
    }

    fn bind_unchecked(data: MemOffset<Buf>) -> Self {
        Self { data }
    }
}

impl<Buf: MemRead + Copy> Disconnect<Buf> {
    pub fn get_code(&self) -> i16 {
        self.data.mem_get_primitive::<i16>(0)
    }
    pub fn get_error(&self) -> Result<&'_ str, StringBindError> {
        tll::bind::offset_str::<tll::bind::OffsetPtrDefault, Buf>(&self.data, 2)
    }
}
impl<Buf: MemRead> MsgId for Disconnect<Buf> {
    const MSGID: i32 = 2;
}
