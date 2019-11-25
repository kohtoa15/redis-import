use std::{
    io::{self, BufReader, BufRead},
    fs::File,
};

pub fn read_csv(filepath: &str, mut header: bool) -> Result< Vec<Vec<String>>, io::Error> {
    let file = File::open(filepath)?;
    let mut buf_reader = BufReader::new(file);

    let mut ret: Vec<Vec<String>> = Vec::new();
    loop {
        let mut line = String::new();
        match buf_reader.read_line(&mut line) {
            Err(_) => break,
            Ok(length) => {
                if length != 0 {
                    // Skipping first line if header line is configured
                    if header {
                        header = false;
                        continue;
                    } else {
                        let line = line.trim();
                        ret.push( line.split(";").map(|s| String::from(s)).collect::<Vec<String>>() );
                    }
                } else {
                    break;
                }
            },
        };
    }

    return Ok(ret);
}

pub fn read_template(file: &str) -> Result<Vec<String>, io::Error> {
    let data = read_csv(file, false)?;
    data.into_iter().next().ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))
}
