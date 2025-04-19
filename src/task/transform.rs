use serde::{Deserialize, Serialize};
use yaml_rust2::Yaml;

use crate::{
    context::transform::TransformRegistry,
    pipeline::{
        common::{HasTask, PipelineOnceTask},
        context::DefaultContext,
    },
    transform::common::{RootTransform, Transform},
    util::error::{CpError, CpResult},
};

use super::common::{deserialize_arg_str, yaml_to_task_arg_str};

#[derive(Debug, Serialize, Deserialize)]
pub struct TransformTask {
    pub name: String,
    pub input: String, // input df
    pub save_df: String,
}

pub fn run_transform(ctx: &mut DefaultContext, transform_task: &TransformTask) -> CpResult<()> {
    let trf = ctx.get_transform(&transform_task.name)?;
    let input = ctx.clone_result(&transform_task.input)?;
    let replaced = match trf.run(input, ctx.get_ro_results()) {
        Ok(x) => ctx.insert_result(&transform_task.save_df, x),
        Err(e) => {
            return Err(CpError::TaskError(
                "Transform task failed",
                format!("The transform task {:?} failed: {:?}", &transform_task, e),
            ));
        }
    };
    if let Ok(last) = replaced {
        if let Some(lf) = last {
            println!("Replaced lf:\n{:?}", lf.count().collect());
        }
    } else {
        return Err(CpError::TaskError(
            "Transform task: Inserting result failed",
            format!("The task {:?} failed to insert the result", &transform_task),
        ));
    }
    Ok(())
}

impl HasTask for TransformTask {
    fn task(args: &Yaml) -> CpResult<PipelineOnceTask> {
        let arg_str = yaml_to_task_arg_str(args, "TransformTask")?;
        let trf: TransformTask = deserialize_arg_str(&arg_str, "TransformTask")?;
        Ok(Box::new(move |ctx| run_transform(ctx, &trf)))
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::LazyFrame;

    use crate::{
        context::{
            model::ModelRegistry,
            task::{TaskDictionary, generate_task},
            transform::TransformRegistry,
        },
        pipeline::{common::HasTask, context::DefaultContext, results::PipelineResults},
        transform::{
            common::RootTransform,
            select::{SelectField, SelectTransform},
        },
        util::common::{DummyData, yaml_from_str},
    };

    use super::TransformTask;

    fn create_bad_transform() -> RootTransform {
        RootTransform::new(
            "player_data_to_full_name_id",
            vec![Box::new(SelectTransform::new(&[
                SelectField::new("first_name", "name.first"),
                SelectField::new("last_name", "name.last_one"),
                SelectField::new("id", "csid"),
            ]))],
        )
    }

    fn create_good_transform() -> RootTransform {
        RootTransform::new(
            "player_data_to_full_name_id",
            vec![Box::new(SelectTransform::new(&[
                SelectField::new("first_name", "name.first"),
                SelectField::new("last_name", "name.last"),
                SelectField::new("id", "csid"),
            ]))],
        )
    }

    fn create_identity_transform() -> RootTransform {
        RootTransform::new(
            "player_data_to_full_name_id",
            vec![Box::new(SelectTransform::new(&[
                SelectField::new("first_name", "first_name"),
                SelectField::new("last_name", "last_name"),
                SelectField::new("id", "id"),
            ]))],
        )
    }

    fn create_context(is_good: bool) -> DefaultContext {
        let transform = if is_good {
            create_good_transform()
        } else {
            create_bad_transform()
        };
        let mut transform_reg = TransformRegistry::new();
        transform_reg.insert(transform);
        let mut ctx = DefaultContext::new(
            ModelRegistry::new(),
            transform_reg,
            TaskDictionary::new(vec![("transform", generate_task::<TransformTask>())]),
        );
        ctx.insert_result("PLAYER_DATA", DummyData::player_data()).unwrap();
        ctx
    }

    fn create_identity_context() -> DefaultContext {
        let transform = create_identity_transform();
        let mut transform_reg = TransformRegistry::new();
        transform_reg.insert(transform);
        let mut ctx = DefaultContext::new(
            ModelRegistry::new(),
            transform_reg,
            TaskDictionary::new(vec![("transform", generate_task::<TransformTask>())]),
        );
        ctx.insert_result("ID_NAME_MAP", DummyData::id_name_map()).unwrap();
        ctx
    }

    #[test]
    fn valid_transform() {
        let mut ctx = create_context(true);
        let config = "
---
name: player_data_to_full_name_id
input: PLAYER_DATA
save_df: ID_NAME_MAP
";
        let args = yaml_from_str(config).unwrap();
        let t = TransformTask::task(&args).unwrap();
        t(&mut ctx).unwrap();
        let expected_results = DummyData::id_name_map().collect().unwrap();
        let actual_results = ctx.clone_result("ID_NAME_MAP").unwrap().collect().unwrap();
        assert_eq!(expected_results, actual_results);
    }

    #[test]
    fn valid_identity_transform() {
        let mut ctx = create_identity_context();
        let config = "
---
name: player_data_to_full_name_id
input: ID_NAME_MAP
save_df: ID_NAME_MAP
";
        let args = yaml_from_str(config).unwrap();
        let t = TransformTask::task(&args).unwrap();
        t(&mut ctx).unwrap();
        let expected_results = DummyData::id_name_map().collect().unwrap();
        let actual_results = ctx.clone_result("ID_NAME_MAP").unwrap().collect().unwrap();
        assert_eq!(expected_results, actual_results);
    }

    #[test]
    fn invalid_transform_input_wrong() {
        let mut ctx = create_context(true);
        let config = "
---
name: player_data_to_full_name_id
input: PLAYER_DAT
save_df: ID_NAME_MAP
";
        let args = yaml_from_str(config).unwrap();
        let t = TransformTask::task(&args).unwrap();
        t(&mut ctx).unwrap_err();
    }

    #[test]
    fn invalid_transform_bad_transform() {
        let mut ctx = create_context(false);
        let config = "
---
name: player_data_to_full_name_id
input: PLAYER_DATA
save_df: ID_NAME_MAP
";
        let args = yaml_from_str(config).unwrap();
        let t = TransformTask::task(&args).unwrap();
        t(&mut ctx).unwrap();
        ctx.clone_result("ID_NAME_MAP").unwrap().collect().unwrap_err();
    }
}
