use crate::redis::{
    self,
    Client,
    Cmd,
    Connection,
    RedisResult,
    RedisError,
};

use std::{
    collections::HashMap,
    error::Error,
    fmt::{
        Display,
        Formatter,
    }
};

use std::fmt::Result as FmtResult;

fn format_address(ipaddr: String, port: Option<u16>, db: Option<String>) -> String {
    let mut ret = format!("redis://{}", ipaddr);
    if let Some(p) = port {
        ret.push(':');
        ret.push_str(p.to_string().as_str());

        if let Some(d) = db {
            ret.push('/');
            ret.push_str(d.as_str());
        }
    }
    return ret;
}

#[derive(Clone, Debug)]
struct IdKeyMissingError{
    id_key: String,
    row_count: usize,
}

impl IdKeyMissingError {
    pub fn create(id_key: String, row_count: usize) -> IdKeyMissingError {
        IdKeyMissingError{ id_key, row_count }
    }
}

impl Error for IdKeyMissingError {
    fn description(&self) -> &str {
        "Could not find id key in row!"
    }

    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

impl Display for IdKeyMissingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.description())
    }
}

fn try_get_redirect_address(err: RedisError) -> RedisResult<String> {
    if let redis::ErrorKind::ExtensionError = err.kind() {
        let text = format!("{}", err);
        let tokens: Vec<&str> = text.as_str().split_whitespace().collect();

        if tokens.len() >= 3 && tokens[0] == "MOVED:" {
            let redirect_addr = tokens[2];
            return Ok(redirect_addr.to_string());
        }
    }
    Err(err)
}

fn catch_redirect(cmd: &mut Cmd, mut con: Connection) -> RedisResult<Connection> {
    let res : RedisResult<()> = cmd.query(&mut con);
    return match res {
        Ok(_) => Ok(con),
        Err(err) => {
            let addr = try_get_redirect_address(err)?;
            let client = Client::open(format_address(addr, None, None).as_str())?;
            let mut con = client.get_connection()?;
            cmd.query(&mut con)?;
            Ok(con)
        },
    };
}

pub fn import(addr: String, port: Option<u16>, db: Option<String>, name: String, id_key: String, data: Vec<HashMap<String, String>>) -> Result<(), Box<dyn Error>> {
    let client = Client::open(format_address(addr, port, db).as_str())?;
    let mut con = client.get_connection()?;

    for (row, i) in data.iter().zip(0..data.len()) {
        // add id to set
        let id = row.get(&id_key).ok_or(IdKeyMissingError::create(id_key.clone(), i))?.clone();
        con = import_id(con, name.clone(), id.clone())?;
        // import key-value pairs into a hash
        con = import_hash(con, format!("{}:{}", name.clone(), id), row.clone())?;

        // progress info output
        let cnt = i + 1;
        if cnt % 100 == 0 {
            println!("{} rows done", cnt);
        }
    }
    Ok( () )
}

fn import_id(con: Connection, name: String, id: String) -> RedisResult<Connection> {
    /*let _ : () = redis::cmd("SADD").arg(name).arg(id).query(con)?;
    Ok( () )*/
    let mut cmd: Cmd = redis::cmd("SADD");
    catch_redirect(cmd.arg(name).arg(id), con)
}

fn import_hash(con: Connection, name: String, hash: HashMap<String, String>) -> RedisResult<Connection> {
    let mut cmd: Cmd = redis::cmd("HSET");
    catch_redirect( append_hash_args(cmd.arg(name), hash), con )
}

fn append_hash_args(cmd: &mut Cmd, args: HashMap<String, String>) -> &mut Cmd {
    let mut ret = cmd;
    for (key, value) in args {
        ret = ret.arg(key).arg(value);
    }
    return ret;
}
