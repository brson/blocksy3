use serde::{Serialize, Deserialize};
use anyhow::Result;
use std::io::{Read, Write};

pub fn write<Io, Cmd>(io: &mut Io, cmd: &Cmd) -> Result<()>
where Io: Write,
      Cmd: Serialize,
{
    panic!()
}

pub fn read<Io, Cmd>(io: &mut Io) -> Result<Cmd>
where Io: Read,
      Cmd: for <'de> Deserialize<'de>,
{
    panic!()
}
