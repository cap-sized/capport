use std::fs;

use polars::prelude::*;
use polars_lazy::prelude::*;
use serde::{Deserialize, Serialize};
use yaml_rust2::Yaml;

use crate::{
    model::common::{Model, Reshape},
    pipeline::{
        common::{HasTask, PipelineOnceTask},
        context::Context,
    },
    task::common::{deserialize_arg_str, yaml_to_task_arg_str},
    util::error::{CpError, CpResult, SubResult},
};

#[derive(Serialize, Deserialize)]
pub struct CsvModel {
    pub filepath: String,
    pub model: String,   // model name
    pub save_df: String, // df name to save to in PipelineResults
}

pub struct CsvModelLoadTask {
    pub models: Vec<CsvModel>,
}

impl CsvModel {
    pub fn build(&self, model: &Model) -> CpResult<LazyFrame> {
        // TODO: Warn if filepath is not absolute
        let lf: LazyFrame = LazyCsvReader::new(&self.filepath).with_has_header(true).finish()?;
        match model.reshape(lf) {
            Ok(x) => Ok(x),
            Err(e) => Err(CpError::TaskError("CsvModel.reshape", e.to_string())),
        }
    }
}

pub fn csv_load(ctx: &mut Context, csv_models: &CsvModelLoadTask) -> CpResult<()> {
    for csv_model in &csv_models.models {
        let model: Model = ctx.get_model(&csv_model.model)?;
        let lf: LazyFrame = csv_model.build(&model)?;
        if let Some(x) = ctx.set_result(&csv_model.save_df, lf) {
            // TODO: correct warning if replace
            println!("previously contained the lazyframe of size:\n{:?}", x.count().collect());
        }
    }
    Ok(())
}

impl HasTask for CsvModelLoadTask {
    fn task(args: &Yaml) -> CpResult<PipelineOnceTask> {
        let csv_model_nodes = match args.as_vec() {
            Some(x) => x,
            None => {
                return Err(CpError::TaskError(
                    "CsvModelLoadTask",
                    "Not a list of objects with keys (`filepath`, `model` and `save_df`)".to_owned(),
                ));
            }
        };
        let mut models = vec![];

        for csv_model_node in csv_model_nodes {
            let arg_str = yaml_to_task_arg_str(csv_model_node, "CsvModelLoadTask")?;
            let csv_model: CsvModel = deserialize_arg_str(&arg_str, "CsvModelLoadTask")?;
            models.push(csv_model);
        }
        let csv_models = CsvModelLoadTask { models };
        Ok(Box::new(move |ctx| csv_load(ctx, &csv_models)))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Write};

    use polars::{
        df,
        prelude::{DataType, IntoLazy, LazyFrame},
    };

    use crate::{
        context::{
            common::Configurable,
            model::ModelRegistry,
            task::{TaskDictionary, generate_task},
            transform::TransformRegistry,
        },
        model::common::{Model, ModelField},
        pipeline::{common::HasTask, context::Context, results::PipelineResults},
        util::{common::yaml_from_str, tmp::TempFile},
    };

    use super::CsvModelLoadTask;

    fn create_equiv_lf() -> LazyFrame {
        df![
            "id" => 1..10,
            "name" => ["ab", "bc", "cd", "de", "ef", "aa", "bb", "cc", "Dd"]
        ]
        .unwrap()
        .lazy()
    }

    fn create_temp_csv() -> TempFile {
        let temp = TempFile::default();
        temp.get_mut()
            .unwrap()
            .write_all(
                b"
id,name
1,ab
2,bc
3,cd
4,de
5,ef
6,aa
7,bb
8,cc
9,Dd
",
            )
            .unwrap();
        temp
    }

    fn create_context() -> Context {
        let model = Model::new(
            "idname",
            &[
                ModelField::new("id", DataType::String, None),
                ModelField::new("name", DataType::String, None),
            ],
        );
        let mut model_reg = ModelRegistry::new();
        model_reg.insert(model);
        Context::new(
            model_reg,
            TransformRegistry::new(),
            TaskDictionary::new(vec![("load_csv", generate_task::<CsvModelLoadTask>())]),
        )
    }

    #[test]
    fn valid_load_csv() {
        let tmp = create_temp_csv();
        let mut ctx = create_context();
        let config = format!(
            "
---
- filepath: {}
  model: idname
  save_df: ID_NAME_MAP
",
            &tmp.filepath
        );
        let args = yaml_from_str(&config).unwrap();
        let t = CsvModelLoadTask::task(&args).unwrap();
        t(&mut ctx).unwrap();
        let mut expected_results = PipelineResults::new();
        expected_results.insert("ID_NAME_MAP", create_equiv_lf());
    }
}
