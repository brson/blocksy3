use serde::{Serialize, Deserialize};
use anyhow::{Result, anyhow};
use std::io::{Read, Write, BufRead};
use std::convert::TryFrom;

pub fn write<Io, Cmd>(io: &mut Io, cmd: &Cmd) -> Result<()>
where Io: Write,
      Cmd: Serialize,
{
    let body = toml::to_string_pretty(cmd)?;
    let length = u64::try_from(body.len()).expect("u64");
    let length = length.checked_add(2).expect("overflow"); // + 2 newlines
    let header = Header { length };
    let header = toml::to_string_pretty(&header)?;
    let frame = format!(
        "{}\n\
         \n{}\n\
         {}\n\
         \n{}\n",
        FRAME_HEADER_MARKER,
        header,
        FRAME_BODY_MARKER,
        body);

    io.write_all(frame.as_bytes())?;

    Ok(())
}

pub fn read<Io, Cmd>(io: &mut Io) -> Result<Cmd>
where Io: Read + BufRead,
      Cmd: for <'de> Deserialize<'de>,
{
    // Verify FRAME_HEADER_MARKER
    {
        let mut probable_header = String::new();
        io.read_line(&mut probable_header)?;

        if probable_header.is_empty() {
            return Err(anyhow!("missing frame header"));
        }

        // Remove trailing newline
        let probable_header_marker = &probable_header[..probable_header.len() - 1];
        if probable_header_marker != FRAME_HEADER_MARKER {
            return Err(anyhow!("incorrect frame header"));
        }
    }


    // Read header bytes until FRAME_BODY_MARKER
    let body_length;
    {
        let mut header = String::new();
        let mut line = String::new();

        loop {
            line.truncate(0);
            io.read_line(&mut line)?;

            if line.is_empty() {
                return Err(anyhow!("broken frame header"));
            }

            let maybe_body_marker = &line[..line.len() - 1];
            if maybe_body_marker == FRAME_BODY_MARKER {
                break;
            }

            header.push_str(&line);
        }

        let header: Header = toml::from_str(&header)?;
        body_length = header.length;
    }

    // Read the body
    let body_length = usize::try_from(body_length).expect("usize");
    let mut buf = vec![0; body_length];
    io.read_exact(&mut buf)?;

    let cmd: Cmd = toml::from_slice(&buf)?;

    Ok(cmd)
}

static FRAME_HEADER_MARKER: &'static str = "# HEADER";
static FRAME_BODY_MARKER: &'static str = "# BODY";

#[derive(Serialize, Deserialize)]
struct Header {
    length: u64,
}
