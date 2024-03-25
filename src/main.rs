#![feature(absolute_path)]

mod contexts;
mod utils;

#[derive(serde::Deserialize)]
struct InputBody {
    r#type: RequestType,
    msg_id: usize,
    #[serde(flatten)]
    other: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct Input {
    id: usize,
    src: String,
    dest: String,
    body: InputBody,
}

#[derive(Clone, Copy, serde::Deserialize)]
enum RequestType {
    #[serde(rename = "init")]
    Init,
    #[serde(rename = "echo")]
    Echo,
    #[serde(rename = "generate")]
    Generate,
    #[serde(rename = "broadcast")]
    Broadcast,
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "topology")]
    Topology,
    #[serde(rename = "add")]
    Add,
    #[serde(rename = "send")]
    Send,
    #[serde(rename = "poll")]
    Poll,
    #[serde(rename = "commit_offsets")]
    CommitOffsets,
    #[serde(rename = "list_committed_offsets")]
    ListCommitOffsets,
}

struct InitRequest {
    node_id: String,
    node_ids: Vec<String>,
}

struct EchoRequest {
    echo: String,
}

struct BroadcastRequest {
    message: usize,
}

struct TopologyRequest {
    graph: std::collections::BTreeMap<String, Vec<String>>,
}

enum TypedRequest {
    Init(InitRequest),
    Echo(EchoRequest),
    Generate,
    Broadcast(BroadcastRequest),
    Read,
    Topology(TopologyRequest),
    Add(usize),
    Send {
        key: String,
        msg: usize,
    },
    Poll {
        offsets: std::collections::BTreeMap<String, usize>,
    },
    CommitOffsets(std::collections::BTreeMap<String, usize>),
    ListCommittedOffsets(Vec<String>),
}

struct TypedInput {
    id: usize,
    src: String,
    dest: String,
    msg_id: usize,
    typed_body: TypedRequest,
}

impl From<Input> for TypedInput {
    fn from(value: Input) -> Self {
        let typed_body = match value.body.r#type {
            RequestType::Init => {
                assert_eq!(value.body.other.len(), 2);
                TypedRequest::Init(InitRequest {
                    node_id: utils::extract_input::<String>(&value, "node_id"),
                    node_ids: value
                        .body
                        .other
                        .get("node_ids")
                        .expect("request of type init expect node_ids")
                        .as_array()
                        .expect("request of type init expect node_ids of array")
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .expect(
                                    "request of type init expect node_ids of type array of string",
                                )
                                .to_string()
                        })
                        .collect(),
                })
            }
            RequestType::Echo => {
                assert_eq!(value.body.other.len(), 1);
                TypedRequest::Echo(EchoRequest {
                    echo: value
                        .body
                        .other
                        .get("echo")
                        .expect("request of type echo expect echo")
                        .as_str()
                        .expect("request of type echo expect echo of type string")
                        .to_string(),
                })
            }
            RequestType::Generate => {
                assert_eq!(value.body.other.len(), 0);
                TypedRequest::Generate
            }
            RequestType::Broadcast => {
                assert_eq!(value.body.other.len(), 1);
                TypedRequest::Broadcast(BroadcastRequest {
                    message: utils::extract_input::<usize>(&value, "message"),
                })
            }
            RequestType::Read => {
                assert_eq!(value.body.other.len(), 0);
                TypedRequest::Read
            }
            RequestType::Topology => {
                assert_eq!(value.body.other.len(), 1);
                TypedRequest::Topology(TopologyRequest {
                    graph: value
                        .body
                        .other
                        .get("topology")
                        .expect("topology request expect topology")
                        .clone()
                        .as_object_mut()
                        .expect("topology field should be a map")
                        .into_iter()
                        .map(|(key, val)| (
                            key.to_string(),
                            val.as_array().expect("topology field should be a map of value of array")
                                .iter().map(|string|string.as_str().expect("topology field should be a map of value of array of string")
                                .to_string()).collect()
                        ))
                        .collect(),
                })
            }
            RequestType::Add => {
                assert_eq!(value.body.other.len(), 1);
                TypedRequest::Add(
                    value
                        .body
                        .other
                        .get("delta")
                        .expect("add request requires delta field")
                        .as_u64()
                        .expect("add request requires delta field to be a number")
                        as usize,
                )
            }
            RequestType::Send => {
                assert_eq!(value.body.other.len(), 2);
                TypedRequest::Send {
                    key: value
                        .body
                        .other
                        .get("key")
                        .expect("send request requires key field")
                        .as_str()
                        .expect("send request requires key as string")
                        .to_string(),
                    msg: value
                        .body
                        .other
                        .get("msg")
                        .expect("send request requires msg field")
                        .as_u64()
                        .expect("send request requires msg as usize")
                        as usize,
                }
            }
            RequestType::Poll => {
                assert_eq!(value.body.other.len(), 1);
                TypedRequest::Poll {
                    offsets: value
                        .body
                        .other
                        .get("offsets")
                        .expect("poll request requires offsets field")
                        .as_object()
                        .expect("poll request requires offsets as mapping")
                        .into_iter()
                        .map(|(key, value)| {
                            (
                                key.to_owned(),
                                value.as_u64().expect(
                                    "poll request requires offsets as mapping of value of number",
                                ) as usize,
                            )
                        })
                        .collect(),
                }
            }
            RequestType::CommitOffsets => {
                assert_eq!(value.body.other.len(), 1);
                TypedRequest::CommitOffsets(
                    value
                        .body
                        .other
                        .get("offsets")
                        .expect("commit_offsets request requires offsets field")
                        .as_object()
                        .unwrap()
                        .into_iter()
                        .map(|(key, value)| (key.to_owned(), value.as_u64().unwrap() as usize))
                        .collect(),
                )
            }
            RequestType::ListCommitOffsets => {
                assert_eq!(value.body.other.len(), 1);
                TypedRequest::ListCommittedOffsets(
                    value
                        .body
                        .other
                        .get("keys")
                        .expect("commit_offsets request requires offsets field")
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|value| value.as_str().unwrap().to_string())
                        .collect(),
                )
            }
        };
        TypedInput {
            id: value.id,
            src: value.src,
            dest: value.dest,
            msg_id: value.body.msg_id,
            typed_body,
        }
    }
}

#[derive(serde::Serialize)]
#[serde(tag = "type")]
enum TypedOutputBody {
    #[serde(rename = "init_ok")]
    Init,
    #[serde(rename = "echo_ok")]
    Echo { echo: String },
    #[serde(rename = "generate_ok")]
    Generate { id: String },
    #[serde(rename = "broadcast_ok")]
    Broadcast,
    #[serde(rename = "read_ok")]
    Read { value: usize },
    #[serde(rename = "topology_ok")]
    Topology,
    #[serde(rename = "add_ok")]
    Add,
    #[serde(rename = "send_ok")]
    Send { offset: usize },
    #[serde(rename = "poll_ok")]
    Poll {
        msgs: std::collections::BTreeMap<String, Vec<Vec<usize>>>,
    },
    #[serde(rename = "commit_offsets_ok")]
    CommitOffsets,
    #[serde(rename = "list_committed_offsets_ok")]
    ListCommittedOffsets {
        offsets: std::collections::BTreeMap<String, usize>,
    },
}

#[derive(serde::Serialize)]
struct OutputBody {
    msg_id: usize,
    in_reply_to: usize,
    #[serde(flatten)]
    typed_body: TypedOutputBody,
}
#[derive(serde::Serialize)]
struct Output {
    src: String,
    dest: String,
    body: OutputBody,
}

fn process(input: TypedInput, context: &mut contexts::Context) -> Output {
    let msg_id = context.read_counter_and_increment();
    let typed_output_body = match input.typed_body {
        TypedRequest::Init(InitRequest { node_id, node_ids }) => {
            context
                .initialize(node_id, node_ids)
                .expect("context already initialized");
            TypedOutputBody::Init
        }
        TypedRequest::Echo(EchoRequest { echo }) => TypedOutputBody::Echo { echo },
        TypedRequest::Generate => TypedOutputBody::Generate {
            id: {
                let mut me = context
                    .whoami()
                    .expect("expect init before generate")
                    .clone();
                me.push('-');
                me.push_str(msg_id.to_string().as_str());
                me
            },
        },
        TypedRequest::Broadcast(BroadcastRequest { message }) => {
            context.push_message(message);
            TypedOutputBody::Broadcast
        }
        TypedRequest::Read => TypedOutputBody::Read {
            value: context.read_global_counter(),
        },
        TypedRequest::Topology(_) => TypedOutputBody::Topology,
        TypedRequest::Add(delta) => {
            context.add_global_counter(delta);
            TypedOutputBody::Add
        }
        TypedRequest::Send { key, msg } => {
            let offset = context.send(key, msg);
            TypedOutputBody::Send { offset }
        }
        TypedRequest::Poll { offsets } => {
            let response = context.poll(offsets);
            TypedOutputBody::Poll {
                msgs: response
                    .into_iter()
                    .map(|(key, msgs)| {
                        (key, {
                            msgs.into_iter()
                                .map(|(offset, value)| vec![offset, value])
                                .collect()
                        })
                    })
                    .collect(),
            }
        }
        TypedRequest::CommitOffsets(offsets) => {
            context.commit_offsets(offsets);
            TypedOutputBody::CommitOffsets
        }
        TypedRequest::ListCommittedOffsets(keys) => {
            let offsets = context.list_committed_offsets(keys);
            TypedOutputBody::ListCommittedOffsets { offsets }
        }
    };
    Output {
        src: input.dest,
        dest: input.src,
        body: OutputBody {
            msg_id,
            in_reply_to: input.msg_id,
            typed_body: typed_output_body,
        },
    }
}

fn main() {
    let mut context = contexts::Context::new();
    loop {
        let mut input_string = String::new();
        let _ = &mut std::io::stdin()
            .read_line(&mut input_string)
            .expect("failed to read from stdin");
        // dbg!(&input_string);
        let input: Input =
            serde_json::from_str(input_string.as_str()).expect("failed to deserialize input");
        let typed_input: TypedInput = input.into();
        let output = process(typed_input, &mut context);
        // dbg!(context
        //     .whoami()
        //     .expect("should always be initialized at this point"),);
        let output_string = serde_json::to_string(&output).expect("failed to serialize output");
        // dbg!(&output_string);
        let write_size = std::io::Write::write(&mut std::io::stdout(), output_string.as_bytes())
            .expect("failed to write to stdout");
        assert_eq!(output_string.len(), write_size);
        let separator_write_size = std::io::Write::write(&mut std::io::stdout(), "\n".as_bytes())
            .expect("failed to write separator to stdout");
        assert_eq!(separator_write_size, 1);
    }
}
