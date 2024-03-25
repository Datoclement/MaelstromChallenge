type Offsets = std::collections::BTreeMap<String, usize>;
type Logs = std::collections::BTreeMap<String, Vec<usize>>;
type LogRetrieval = std::collections::BTreeMap<String, Vec<(usize, usize)>>;

enum Request {
    Send { key: String, msg: usize },
    Poll { offsets: Offsets },
    CommitOffsets { offsets: Offsets },
    ListCommittedOffsets { keys: Vec<String> },
}

#[derive(Debug)]
enum ParseRequestError {
    InvalidFormat(String),
    InvalidCommand(String),
    InvalidNumber(std::num::ParseIntError),
    EmptyString,
}

impl From<std::num::ParseIntError> for ParseRequestError {
    fn from(value: std::num::ParseIntError) -> Self {
        Self::InvalidNumber(value)
    }
}
fn take_first_token(string: &str) -> Result<(&str, &str), ParseRequestError> {
    let Some((command, remain)) = string.split_once(':') else {
        return Err(ParseRequestError::InvalidFormat(string.to_string()));
    };
    Ok((command, remain))
}

fn parse_offsets(mut remain: &str) -> Result<Offsets, ParseRequestError> {
    let mut offsets = Offsets::new();
    loop {
        let (key, new_remain) = take_first_token(remain)?;
        remain = new_remain;
        if key.is_empty() {
            break;
        }
        let (offset_string, new_remain) = take_first_token(remain)?;
        remain = new_remain;
        let offset = offset_string.parse()?;
        offsets.insert(key.to_string(), offset);
    }
    Ok(offsets)
}
impl std::str::FromStr for Request {
    type Err = ParseRequestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (command, mut remain) = take_first_token(s)?;
        match command {
            "send" => {
                let (key, msg) = take_first_token(remain)?;
                Ok(Self::Send {
                    key: key.to_string(),
                    msg: msg.parse()?,
                })
            }
            "poll" => {
                let offsets = parse_offsets(remain)?;
                Ok(Self::Poll { offsets })
            }
            "commit-offsets" => {
                let offsets = parse_offsets(remain)?;
                Ok(Self::CommitOffsets { offsets })
            }
            "list-committed-offsets" => {
                let mut keys = Vec::new();
                loop {
                    let (key, new_remain) = take_first_token(remain)?;
                    remain = new_remain;
                    if key.is_empty() {
                        break;
                    }
                    keys.push(key.to_string())
                }
                Ok(Self::ListCommittedOffsets { keys })
            }
            command => Err(ParseRequestError::InvalidCommand(command.to_string())),
        }
    }
}

fn main() {
    let listener =
        std::net::TcpListener::bind("localhost:7999").expect("failed to listen to port 7999");

    let mut logs = Logs::new();
    let mut offset_registry = Offsets::new();
    listener.incoming().for_each(|request| {
        let mut stream = request.expect("failed to acquire stream");
        let mut buffer_reader = std::io::BufReader::new(&mut stream);
        let mut request_string = String::new();
        std::io::BufRead::read_line(&mut buffer_reader, &mut request_string)
            .expect("failed to read inputs");
        dbg!(&request_string);
        // dbg!(&logs, &offset_registry);
        let response: String = match <Request as std::str::FromStr>::from_str(request_string.trim())
        {
            Ok(request) => match request {
                Request::Send { key, msg } => {
                    let log = logs.entry(key).or_default();
                    let offset = log.len();
                    log.push(msg);
                    offset.to_string()
                }
                Request::Poll { offsets } => {
                    let mut retrieved = LogRetrieval::new();
                    offsets.into_iter().for_each(|(key, offset)| {
                        retrieved.insert(key.clone(), {
                            match offset.cmp(
                                &offset_registry
                                    .get(&key)
                                    .map(|committed_offset| committed_offset + 1)
                                    .unwrap_or_default(),
                            ) {
                                std::cmp::Ordering::Equal | std::cmp::Ordering::Less => {
                                    logs.entry(key).or_default()[offset..]
                                        .iter()
                                        .take(1)
                                        .enumerate()
                                        .map(|(index, message)| (offset + index, *message))
                                        .collect()
                                }
                                std::cmp::Ordering::Greater => Vec::new(),
                            }
                        });
                    });
                    serde_json::to_string(&retrieved).expect("failed to serialize")
                }
                Request::CommitOffsets { offsets } => {
                    offsets.into_iter().for_each(|(key, incoming_offset)| {
                        offset_registry
                            .entry(key)
                            .and_modify(|committed_offset| {
                                *committed_offset = incoming_offset.max(*committed_offset)
                            })
                            .or_insert(incoming_offset);
                    });
                    "".to_string()
                }
                Request::ListCommittedOffsets { keys } => {
                    let retrieved: Offsets = keys
                        .into_iter()
                        .filter(|key| offset_registry.get(key).is_some())
                        .map(|key| (key.clone(), *offset_registry.get(&key).unwrap()))
                        .collect();
                    serde_json::to_string(&retrieved).expect("failed to serialize")
                }
            },
            Err(error) => {
                dbg!(error);
                "".to_string()
            }
        };
        dbg!(&response);
        std::io::Write::write(&mut stream, format!("{response}\n").as_bytes())
            .expect("failed to respond to client");
        println!("response sent");
    })
}

#[cfg(test)]
mod test {}
