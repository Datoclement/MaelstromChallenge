use crate::Input;

pub trait ExtractFromInput
where
    Self: Sized,
{
    fn from_value(value: &serde_json::Value) -> Self;
}

impl ExtractFromInput for usize {
    fn from_value(value: &serde_json::Value) -> Self {
        value.as_u64().unwrap() as usize
    }
}

impl ExtractFromInput for String {
    fn from_value(value: &serde_json::Value) -> Self {
        value.as_str().unwrap().to_string()
    }
}

pub fn extract_input<OutputType>(input: &Input, field_name: &str) -> OutputType
where
    OutputType: ExtractFromInput,
{
    ExtractFromInput::from_value(
        input
            .body
            .other
            .get(field_name)
            .unwrap_or_else(|| panic!("request expect {field_name}")),
    )
}
