#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelField {
    pub label: String,
    pub dtype: polars::datatypes::DataType,
    pub constraints: Vec<String>, // TODO: Replace with constraints enum
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Model {
    pub name: String,
    pub fields: Vec<ModelField>,
}

impl Model {
    pub fn new(name: &str, fields: Vec<ModelField>) -> Model {
        Model {
            name: name.to_string(),
            fields: fields,
        }
    }
}

impl ModelField {
    pub fn new(label: &str, dtype: polars::datatypes::DataType, constraints: Option<Vec<&str>>) -> ModelField {
        ModelField {
            label: label.to_string(),
            dtype: dtype,
            constraints: constraints.unwrap_or(vec![]).iter().map(|x| x.to_string()).collect(),
        }
    }
}
