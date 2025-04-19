// use std::{
//     collections::HashMap,
//     rc::Weak,
//     sync::{Arc, Mutex},
// };

// use serde::{Deserialize, Serialize};

// #[derive(Serialize, Deserialize)]
// pub struct NoopService {
//     name: String,
//     count: usize,
// }

// #[derive(Serialize, Deserialize)]
// pub struct OpService {
//     name: String,
//     count: usize,
// }

// impl NoopService {
//     pub fn new(name: &str) -> Self {
//         Self {
//             name: name.to_owned(),
//             count: 0,
//         }
//     }
// }

// impl OpService {
//     pub fn new(name: &str) -> Self {
//         Self {
//             name: name.to_owned(),
//             count: 0,
//         }
//     }
// }

// pub trait DistributeService<Svc> {
//     // fn get_service(name: &str) -> Arc<Mutex<Svc>>;
//     fn get_default_svc(&self) -> Arc<Mutex<Svc>>;
// }

// pub struct ExampleSvcDistributor {
//     noop: Arc<Mutex<NoopService>>,
//     op: Arc<Mutex<OpService>>,
// }

// impl ExampleSvcDistributor {
//     pub fn new() -> Self {
//         Self {
//             noop: Arc::new(Mutex::new(NoopService::new("Noop"))),
//             op: Arc::new(Mutex::new(OpService::new("Op"))),
//         }
//     }
// }

// impl DistributeService<OpService> for ExampleSvcDistributor {
//     fn get_default_svc(&self) -> Arc<Mutex<OpService>> {
//         self.op.clone()
//     }
// }

// impl DistributeService<NoopService> for ExampleSvcDistributor {
//     fn get_default_svc(&self) -> Arc<Mutex<NoopService>> {
//         self.noop.clone()
//     }
// }

// #[cfg(test)]
// mod tests {
//     use std::sync::{Arc, Mutex};

//     use super::{DistributeService, ExampleSvcDistributor, NoopService};

//     #[test]
//     fn amin() {
//         let dist = ExampleSvcDistributor::new();
//         let noop: Arc<Mutex<NoopService>> = dist.get_default_svc();
//     }
// }
