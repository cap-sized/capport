use base64::{Engine, prelude::BASE64_STANDARD};
use serde_json::json;

use super::mongo::{HasMongoClient, MongoClient};

pub struct DefaultSvcDistributor {
    pub mongo: Option<MongoClient>,
}

impl HasMongoClient for DefaultSvcDistributor {
    fn get_mongo_client(&self, _name: Option<&str>) -> Option<MongoClient> {
        self.mongo.clone()
    }
}

pub trait JsonTranscodable {
    fn into_json(self) -> serde_json::Value;
}

impl JsonTranscodable for bson::Bson {
    fn into_json(self) -> serde_json::Value {
        match self {
            bson::Bson::Double(v) if v.is_nan() => {
                let s = if v.is_sign_negative() { "-NaN" } else { "NaN" };

                s.into()
            }
            bson::Bson::Double(v) if v.is_infinite() => {
                let s = if v.is_sign_negative() { "-Infinity" } else { "Infinity" };

                s.into()
            }
            bson::Bson::Double(v) => json!(v),
            bson::Bson::String(v) => json!(v),
            bson::Bson::Array(v) => {
                serde_json::Value::Array(v.into_iter().map(bson::Bson::into_relaxed_extjson).collect())
            }
            bson::Bson::Document(v) => {
                serde_json::Value::Object(v.into_iter().map(|(k, v)| (k, v.into_relaxed_extjson())).collect())
            }
            bson::Bson::Boolean(v) => json!(v),
            bson::Bson::Null => serde_json::Value::Null,
            bson::Bson::RegularExpression(regex) => {
                let mut chars: Vec<_> = regex.options.chars().collect();
                chars.sort_unstable();

                let options: String = chars.into_iter().collect();

                json!({
                    "$regularExpression": {
                        "pattern": regex.pattern,
                        "options": options,
                    }
                })
            }
            bson::Bson::JavaScriptCode(code) => code.into(),
            bson::Bson::JavaScriptCodeWithScope(code_with_scope) => code_with_scope.code.into(),
            bson::Bson::Int32(v) => v.into(),
            bson::Bson::Int64(v) => v.into(),
            bson::Bson::Timestamp(timestamp) => json!({
                "$timestamp": {
                    "t": timestamp.time,
                    "i": timestamp.increment,
                }
            }),
            bson::Bson::Binary(bin) => {
                let tval: u8 = From::from(bin.subtype);
                json!({
                    "$binary": {
                        "base64": BASE64_STANDARD.encode(bin.bytes),
                        "subType": hex::encode([tval]),
                    }
                })
            }
            bson::Bson::ObjectId(v) => json!({"$oid": v.to_hex()}),
            bson::Bson::DateTime(v) if v.timestamp_millis() >= 0 => {
                json!({
                    // Unwrap safety: timestamps in the guarded range can always be formatted.
                    "$date": v.try_to_rfc3339_string().unwrap(),
                })
            }
            bson::Bson::DateTime(v) => json!({
                "$date": v.to_string(),
            }),
            bson::Bson::Symbol(v) => json!({ "$symbol": v }),
            bson::Bson::Decimal128(_) => panic!("Decimal128 extended JSON not implemented yet."),
            bson::Bson::Undefined => json!({ "$undefined": true }),
            bson::Bson::MinKey => json!({ "$minKey": 1 }),
            bson::Bson::MaxKey => json!({ "$maxKey": 1 }),
            bson::Bson::DbPointer(ptr) => json!({
                "$dbPointer": ptr
            }),
        }
    }
}
