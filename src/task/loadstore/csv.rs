use std::fs::File;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use yaml_rust2::Yaml;

use crate::{
    model::common::{Model, Reshape},
    pipeline::{
        common::{HasTask, PipelineTask},
        context::PipelineContext,
    },
    task::common::{deserialize_arg_str, yaml_to_task_arg_str},
    util::error::{CpError, CpResult},
};

#[derive(Serialize, Deserialize)]
pub struct CsvModel {
    pub filepath: String,
    pub model: String,   // model name
    pub df_name: String, // df name to save to/load from in PipelineResults
}

pub struct CsvModelLoadTask {
    pub models: Vec<CsvModel>,
}

pub struct CsvModelSaveTask {
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

    pub fn build_vec(args: &Yaml) -> CpResult<Vec<Self>> {
        let csv_model_nodes = match args.as_vec() {
            Some(x) => x,
            None => {
                return Err(CpError::TaskError(
                    "CsvModelLoadTask",
                    "Not a list of objects with keys (`filepath`, `model` and `df_name`)".to_owned(),
                ));
            }
        };
        let mut models = vec![];

        for csv_model_node in csv_model_nodes {
            let arg_str = yaml_to_task_arg_str(csv_model_node, "CsvModelLoadTask")?;
            let csv_model: CsvModel = deserialize_arg_str(&arg_str, "CsvModelLoadTask")?;
            models.push(csv_model);
        }
        Ok(models)
    }
}

pub fn csv_load<S>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>, csv_models: &CsvModelLoadTask) -> CpResult<()> {
    for csv_model in &csv_models.models {
        let model: Model = ctx.get_model(&csv_model.model)?;
        let lf: LazyFrame = csv_model.build(&model)?;
        if let Some(x) = ctx.insert_result(&csv_model.df_name, lf)? {
            // TODO: correct warning if replace
            println!("previously contained the lazyframe of size:\n{:?}", x.count().collect());
        }
    }
    Ok(())
}

pub fn csv_save<S>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>, csv_models: &CsvModelLoadTask) -> CpResult<()> {
    for csv_model in &csv_models.models {
        let mut result: DataFrame = ctx.clone_result(&csv_model.df_name)?.collect()?;
        let mut output_file = File::create(&csv_model.filepath)?;
        let mut writer = CsvWriter::new(&mut output_file).include_header(true);
        writer.finish(&mut result)?;
    }
    Ok(())
}

impl HasTask for CsvModelSaveTask {
    fn lazy_task<S>(args: &Yaml) -> CpResult<PipelineTask<LazyFrame, S>> {
        let csv_models = CsvModelLoadTask {
            models: CsvModel::build_vec(args)?,
        };
        Ok(Box::new(move |ctx| csv_save(ctx, &csv_models)))
    }
}

impl HasTask for CsvModelLoadTask {
    fn lazy_task<S>(args: &Yaml) -> CpResult<PipelineTask<LazyFrame, S>> {
        let csv_models = CsvModelLoadTask {
            models: CsvModel::build_vec(args)?,
        };
        Ok(Box::new(move |ctx| csv_load(ctx, &csv_models)))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        sync::Arc,
    };

    use polars::{
        df,
        prelude::{DataType, IntoLazy, LazyFrame},
    };

    use crate::{
        context::{
            model::ModelRegistry,
            task::{TaskDictionary, generate_lazy_task},
            transform::TransformRegistry,
        },
        model::common::{Model, ModelField},
        pipeline::{
            common::HasTask,
            context::{DefaultContext, PipelineContext},
            results::PipelineResults,
        },
        util::{common::yaml_from_str, tmp::TempFile},
    };

    use super::{CsvModelLoadTask, CsvModelSaveTask};

    fn create_equiv_lf(force_str: bool) -> LazyFrame {
        let lf = df![
            "id" => 1..10,
            "name" => ["ab", "bc", "cd", "de", "ef", "aa", "bb", "cc", "Dd"]
        ]
        .unwrap()
        .lazy();
        if force_str {
            lf.cast_all(DataType::String, true)
        } else {
            lf
        }
    }

    fn check_temp_csvs_identical(a: &TempFile, b: &TempFile) {
        let mut astr = String::new();
        let mut bstr = String::new();
        a.get().unwrap().read_to_string(&mut astr).unwrap();
        b.get().unwrap().read_to_string(&mut bstr).unwrap();
        assert_eq!(astr.trim(), bstr.trim());
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

    fn create_context(force_str: bool) -> Arc<DefaultContext<LazyFrame, ()>> {
        let id_datatype = if force_str { DataType::String } else { DataType::Int32 };
        let model = Model::new(
            "idname",
            &[
                ModelField::new("id", id_datatype, None),
                ModelField::new("name", DataType::String, None),
            ],
        );
        let mut model_reg = ModelRegistry::new();
        model_reg.insert(model);
        Arc::new(DefaultContext::new(
            model_reg,
            TransformRegistry::new(),
            TaskDictionary::new(vec![
                ("load_csv", generate_lazy_task::<CsvModelLoadTask, ()>()),
                ("save_csv", generate_lazy_task::<CsvModelSaveTask, ()>()),
            ]),
        ))
    }

    #[test]
    fn valid_load_csv_force_str() {
        let tmp = create_temp_csv();
        let ctx = create_context(true);
        let config = format!(
            "
---
- filepath: {}
  model: idname
  df_name: ID_NAME_MAP
",
            &tmp.filepath
        );
        let args = yaml_from_str(&config).unwrap();
        let t = CsvModelLoadTask::lazy_task(&args).unwrap();
        t(ctx.clone()).unwrap();
        let mut expected_results = PipelineResults::<LazyFrame>::default();
        expected_results.insert("ID_NAME_MAP", create_equiv_lf(true));
        let actual_results = ctx.clone_results().unwrap();
        assert_eq!(expected_results, actual_results);
    }

    #[test]
    fn valid_load_csv_default() {
        let tmp = create_temp_csv();
        let ctx = create_context(false);
        let config = format!(
            "
---
- filepath: {}
  model: idname
  df_name: ID_NAME_MAP
",
            &tmp.filepath
        );
        let args = yaml_from_str(&config).unwrap();
        let t = CsvModelLoadTask::lazy_task(&args).unwrap();
        t(ctx.clone()).unwrap();
        let mut expected_results = PipelineResults::<LazyFrame>::default();
        expected_results.insert("ID_NAME_MAP", create_equiv_lf(false));
        let actual_results = ctx.clone_results().unwrap();
        assert_eq!(expected_results, actual_results);
    }

    #[test]
    fn valid_save_csv_default() {
        let intmp = create_temp_csv();
        let outtmp = create_temp_csv();
        let ctx = create_context(false);
        ctx.insert_result("ID_NAME_MAP", create_equiv_lf(false)).unwrap();
        let config = format!(
            "
---
- filepath: {}
  model: idname
  df_name: ID_NAME_MAP
",
            &outtmp.filepath
        );
        let args = yaml_from_str(&config).unwrap();
        let t = CsvModelSaveTask::lazy_task(&args).unwrap();
        t(ctx.clone()).unwrap();
        check_temp_csvs_identical(&intmp, &outtmp);
    }
}
