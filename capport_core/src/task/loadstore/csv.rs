use std::{
    fs::{self, File},
    path::Path,
};

use log::warn;
use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    model::common::{Model, Reshape},
    pipeline::{
        common::{HasTask, PipelineTask},
        context::PipelineContext,
    },
    util::{
        common::get_full_path,
        error::{CpError, CpResult},
    },
};

#[derive(Serialize, Deserialize)]
pub struct CsvModel {
    pub filepath: String,
    pub model: Option<String>, // model name
    pub df_name: String,       // df name to save to/load from in PipelineResults
}

pub struct CsvModelLoadTask {
    pub models: Vec<CsvModel>,
}

pub struct CsvModelSaveTask {
    pub models: Vec<CsvModel>,
}

impl CsvModel {
    pub fn build(&self, model: Option<Model>) -> CpResult<LazyFrame> {
        let fp = get_full_path(&self.filepath, true)?;
        let lf: LazyFrame = LazyCsvReader::new(fp).with_has_header(true).finish()?;
        match model {
            Some(model) => match model.reshape(lf) {
                Ok(x) => Ok(x),
                Err(e) => Err(CpError::TaskError("CsvModel.reshape", e.to_string())),
            },
            None => Ok(lf),
        }
    }

    pub fn collect_shape(lf: LazyFrame, model: Option<Model>) -> CpResult<DataFrame> {
        match model {
            Some(model) => match model.reshape(lf) {
                Ok(x) => Ok(x.collect()?),
                Err(e) => Err(CpError::TaskError("CsvModel.reshape", e.to_string())),
            },
            None => Ok(lf.collect()?),
        }
    }

    pub fn build_vec(args: serde_yaml_ng::Value) -> CpResult<Vec<Self>> {
        let models: Vec<CsvModel> = serde_yaml_ng::from_value(args)?;
        Ok(models)
    }
}

pub fn csv_load<S>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>, csv_models: &CsvModelLoadTask) -> CpResult<()> {
    for csv_model in &csv_models.models {
        let model: Option<Model> = match &csv_model.model {
            Some(model_name) => Some(ctx.get_model(model_name)?),
            None => None,
        };
        let lf: LazyFrame = csv_model.build(model)?;
        if let Some(x) = ctx.insert_result(&csv_model.df_name, lf)? {
            warn!("previously contained the lazyframe of size:\n{:?}", x.count().collect());
        }
    }
    Ok(())
}

pub fn csv_save<S>(ctx: Arc<dyn PipelineContext<LazyFrame, S>>, csv_models: &CsvModelLoadTask) -> CpResult<()> {
    for csv_model in &csv_models.models {
        let fp = get_full_path(&csv_model.filepath, false)?;
        let lf: LazyFrame = ctx.clone_result(&csv_model.df_name)?;
        let model: Option<Model> = match &csv_model.model {
            Some(model_name) => Some(ctx.get_model(model_name)?),
            None => None,
        };
        let mut df = CsvModel::collect_shape(lf, model)?;
        let dir_path = Path::new(&csv_model.filepath);
        if let Some(x) = dir_path.parent() {
            if !x.exists() {
                fs::create_dir_all(x)?;
            }
        };
        let mut output_file = File::create(fp)?;
        let mut writer = CsvWriter::new(&mut output_file).include_header(true);
        writer.finish(&mut df)?;
    }
    Ok(())
}

impl HasTask for CsvModelSaveTask {
    fn lazy_task<S>(args: &serde_yaml_ng::Value) -> CpResult<PipelineTask<LazyFrame, S>> {
        let csv_models = CsvModelLoadTask {
            models: CsvModel::build_vec(args.clone())?,
        };
        Ok(Box::new(move |ctx| csv_save(ctx, &csv_models)))
    }
}

impl HasTask for CsvModelLoadTask {
    fn lazy_task<S>(args: &serde_yaml_ng::Value) -> CpResult<PipelineTask<LazyFrame, S>> {
        let csv_models = CsvModelLoadTask {
            models: CsvModel::build_vec(args.clone())?,
        };
        Ok(Box::new(move |ctx| csv_load(ctx, &csv_models)))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Read, Write},
        sync::Arc,
    };

    use polars::{
        df,
        prelude::{DataType, IntoLazy, LazyFrame},
    };

    use crate::{
        context::{
            envvar::EnvironmentVariableRegistry,
            logger::LoggerRegistry,
            model::ModelRegistry,
            task::{TaskDictionary, generate_lazy_task},
            transform::TransformRegistry,
        },
        logger::common::{DEFAULT_KEYWORD_CONFIG_DIR, DEFAULT_KEYWORD_OUTPUT_DIR},
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

    fn create_temp_csv(dir: &str) -> TempFile {
        let temp = TempFile::default_in_dir(dir, "csv").unwrap();
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
        let bad_model = Model::new(
            "badidname",
            &[
                ModelField::new("id", DataType::String, None),
                ModelField::new("_name_", DataType::String, None),
            ],
        );
        let mut model_reg = ModelRegistry::new();
        model_reg.insert(model);
        model_reg.insert(bad_model);
        Arc::new(DefaultContext::new(
            model_reg,
            TransformRegistry::new(),
            TaskDictionary::new(vec![
                ("load_csv", generate_lazy_task::<CsvModelLoadTask, ()>()),
                ("save_csv", generate_lazy_task::<CsvModelSaveTask, ()>()),
            ]),
            (),
            LoggerRegistry::new(),
        ))
    }

    #[test]
    fn valid_no_model() {
        let mydir = "/tmp/capport_testing/csv_no_model/";
        fs::create_dir_all(mydir).unwrap();

        let tmp = create_temp_csv(mydir);
        let ctx = Arc::new(DefaultContext::default());
        let config = format!(
            "
---
- filepath: {}
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
        fs::remove_dir_all(mydir).unwrap();
    }

    #[test]
    fn valid_load_csv_force_str() {
        let mydir = "/tmp/capport_testing/csv_force_str/";
        fs::create_dir_all(mydir).unwrap();
        let tmp = create_temp_csv(mydir);
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
        fs::remove_dir_all(mydir).unwrap();
    }

    #[test]
    fn valid_load_csv_default() {
        let mydir = "/tmp/capport_testing/csv_load_csv_default/";
        fs::create_dir_all(mydir).unwrap();
        let tmp = create_temp_csv(mydir);
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
        fs::remove_dir_all(mydir).unwrap();
    }

    #[test]
    fn valid_load_csv_from_default_config_dir_env() {
        let mydir = "/tmp/capport_testing/csv_load_csv_from_default_config_dir_env/";
        fs::create_dir_all(mydir).unwrap();
        let tmp = create_temp_csv(mydir);
        let fp = std::path::Path::new(&tmp.filepath);
        let dir = fp.parent().unwrap();
        let child = fp.file_name().unwrap();
        let mut myenv = EnvironmentVariableRegistry::new();
        myenv
            .set_str(DEFAULT_KEYWORD_CONFIG_DIR, dir.to_str().unwrap().to_owned())
            .unwrap();
        let config = format!(
            "
---
- filepath: {}
  model: idname
  df_name: ID_NAME_MAP
",
            child.to_str().unwrap()
        );
        let ctx = create_context(false);
        let args = yaml_from_str(&config).unwrap();
        let t = CsvModelLoadTask::lazy_task(&args).unwrap();
        t(ctx.clone()).unwrap();
        let mut expected_results = PipelineResults::<LazyFrame>::default();
        expected_results.insert("ID_NAME_MAP", create_equiv_lf(false));
        let actual_results = ctx.clone_results().unwrap();
        assert_eq!(expected_results, actual_results);
        fs::remove_dir_all(mydir).unwrap();
    }

    #[test]
    fn valid_save_csv_default() {
        let mydir = "/tmp/capport_testing/csv_save_csv_default/";
        fs::create_dir_all(mydir).unwrap();
        let intmp = create_temp_csv(mydir);
        let outtmp = create_temp_csv(mydir);
        println!("{} {}", &intmp.filepath, &outtmp.filepath);
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
        fs::remove_dir_all(mydir).unwrap();
    }

    #[test]
    fn valid_save_csv_default_output_dir_env() {
        let mydir = "/tmp/capport_testing/csv_save_csv_default_output_dir_env/";
        fs::create_dir_all(mydir).unwrap();
        let intmp = create_temp_csv(mydir);
        let outtmp = create_temp_csv(mydir);
        let fp = std::path::Path::new(&outtmp.filepath);
        let dir = fp.parent().unwrap();
        let child = fp.file_name().unwrap();
        let mut myenv = EnvironmentVariableRegistry::new();
        myenv
            .set_str(DEFAULT_KEYWORD_OUTPUT_DIR, dir.to_str().unwrap().to_owned())
            .unwrap();
        let ctx = create_context(false);
        ctx.insert_result("ID_NAME_MAP", create_equiv_lf(false)).unwrap();
        let config = format!(
            "
---
- filepath: {}
  model: idname
  df_name: ID_NAME_MAP
",
            child.to_str().unwrap()
        );
        let args = yaml_from_str(&config).unwrap();
        let t = CsvModelSaveTask::lazy_task(&args).unwrap();
        t(ctx.clone()).unwrap();
        check_temp_csvs_identical(&intmp, &outtmp);
        fs::remove_dir_all(mydir).unwrap();
    }

    #[test]
    fn invalid_bad_args() {
        let mydir = "/tmp/capport_testing/csv_bad_args/";
        fs::create_dir_all(mydir).unwrap();
        let outtmp = create_temp_csv(mydir);
        let configs_templates = vec![
            format!(
                "
--- # df_name not present in context to save
- filepath: {}
  model: idname
  df_name: ID_NAME_MAP 
",
                &outtmp.filepath
            ),
            format!(
                "
--- # bad reshape
- filepath: {}
  model: badidname
  df_name: ID_NAME_MAP
",
                &outtmp.filepath
            ),
            format!(
                "
--- # non-existent model
- filepath: {}
  model: non_existent
  df_name: ID_NAME_MAP
",
                &outtmp.filepath
            ),
        ];
        for config in configs_templates {
            let ctx = create_context(false);
            let args = yaml_from_str(&config).unwrap();
            let t = CsvModelSaveTask::lazy_task(&args).unwrap();
            t(ctx.clone()).unwrap_err();
        }
        fs::remove_dir_all(mydir).unwrap();
    }

    #[test]
    fn invalid_args_not_list() {
        let mydir = "/tmp/capport_testing/csv_args_not_list/";
        fs::create_dir_all(mydir).unwrap();
        let outtmp = create_temp_csv(mydir);
        let configs_templates = vec![format!(
            "
--- # df_name not present in context to save
filepath: {}
model: idname
df_name: ID_NAME_MAP 
",
            &outtmp.filepath
        )];
        for config in configs_templates {
            let args = yaml_from_str(&config).unwrap();
            assert!(CsvModelSaveTask::lazy_task::<()>(&args).is_err());
        }
        fs::remove_dir_all(mydir).unwrap();
    }
}
