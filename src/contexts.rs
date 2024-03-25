#[derive(Debug)]
struct NodeMetadata {
    node_id: String,
    node_ids: Vec<String>,
}

#[derive(Debug)]
pub struct Context {
    nodes: Option<NodeMetadata>,
    counter: usize,
    messages: Vec<usize>,
}

#[derive(Debug)]
pub enum ContextInitializationError {
    AlreadyInitialized,
}

#[derive(Debug)]
pub enum ContextWhoamiError {
    NotInitialized,
}

const SERVER_ADDRESS: &str = "localhost:7999";

impl Context {
    pub fn new() -> Self {
        Self {
            nodes: None,
            counter: 0,
            messages: Vec::new(),
        }
    }
    pub fn initialize(
        &mut self,
        node_id: String,
        node_ids: Vec<String>,
    ) -> Result<(), ContextInitializationError> {
        let None = &mut self.nodes else {
            return Err(ContextInitializationError::AlreadyInitialized);
        };
        self.nodes = Some(NodeMetadata { node_id, node_ids });
        Ok(())
    }
    pub fn whoami(&self) -> Result<&String, ContextWhoamiError> {
        let Some(nodes) = &self.nodes else {
            return Err(ContextWhoamiError::NotInitialized);
        };
        Ok(&nodes.node_id)
    }

    fn request(&self, inputs: String) -> String {
        let mut socket =
            std::net::TcpStream::connect(SERVER_ADDRESS).expect("failed to connect to server");
        dbg!(&inputs);
        std::io::Write::write(&mut socket, format!("{inputs}\r\n").to_string().as_bytes())
            .expect("failed to send read request");
        std::io::Write::flush(&mut socket).expect("failed to flush socket inputs");
        let mut response = String::new();
        std::io::Read::read_to_string(&mut socket, &mut response).expect("failed to read response");
        dbg!(&response);
        response.trim().to_string()
    }
    fn read(&self) -> usize {
        self.request(0.to_string())
            .parse()
            .expect("failed to parse response")
    }

    fn sync(&self, delta: usize) {
        self.request(delta.to_string());
    }

    pub fn read_counter_and_increment(&mut self) -> usize {
        let result = self.counter;
        self.counter += 1;
        result
    }

    pub fn read_global_counter(&self) -> usize {
        self.read()
    }

    pub fn add_global_counter(&self, delta: usize) {
        self.sync(delta)
    }
    pub fn push_message(&mut self, message: usize) {
        self.messages.push(message)
    }

    pub fn send(&self, key: String, msg: usize) -> usize {
        let request = format!("send:{key}:{msg}");
        self.request(request)
            .parse()
            .expect("failed to parse response")
    }

    fn serialize(offsets: &std::collections::BTreeMap<String, usize>) -> String {
        offsets
            .iter()
            .map(|(key, value)| format!("{key}:{value}"))
            .collect::<Vec<String>>()
            .join(":")
    }
    pub fn poll(
        &self,
        offsets: std::collections::BTreeMap<String, usize>,
    ) -> std::collections::BTreeMap<String, Vec<(usize, usize)>> {
        let arguments = Self::serialize(&offsets);
        let request = format!("poll:{arguments}::");
        serde_json::from_str(self.request(request).as_str()).expect("failed to parse poll response")
    }

    pub fn commit_offsets(&self, offsets: std::collections::BTreeMap<String, usize>) {
        let arguments = Self::serialize(&offsets);
        let request = format!("commit-offsets:{arguments}::");
        self.request(request);
    }

    pub fn list_committed_offsets(
        &self,
        keys: Vec<String>,
    ) -> std::collections::BTreeMap<String, usize> {
        let arguments = keys.join(":");
        let request = format!("list-committed-offsets:{arguments}::");
        serde_json::from_str(self.request(request).as_str())
            .expect("failed to parse list_commit_offsets response")
    }
}
