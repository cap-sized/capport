use std::{str::FromStr, sync::Arc};

use serde::Deserialize;
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::{
    context::{
        connection::ConnectionRegistry, logger::LoggerRegistry, model::ModelRegistry, pipeline::PipelineRegistry,
        request::RequestRegistry, sink::SinkRegistry, source::SourceRegistry, transform::TransformRegistry,
    },
    logger::common::DEFAULT_CONSOLE_LOGGER_NAME,
    parser::{
        common::{pack_configs_from_files, read_configs},
        run_mode::RunModeEnum,
    },
    pipeline::{
        common::{Pipeline, PipelineConfig},
        context::{DefaultPipelineContext, PipelineContext},
        results::PipelineResults,
    },
    util::{args::RunPipelineArgs, error::CpResult},
};

const CHANNEL_BUFSIZE: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RunnerConfig {
    pub logger: String,
    pub mode: RunModeEnum,
    pub schedule: Option<String>,
    pub tz: Option<String>,
}

pub struct Runner {
    config: RunnerConfig,
    logger_registry: LoggerRegistry,
    pipeline_context: DefaultPipelineContext,
    pipeline_config: PipelineConfig,
    cli_args: RunPipelineArgs,
}

impl Runner {
    pub fn init(cli_args: RunPipelineArgs) -> CpResult<Runner> {
        let config_files = read_configs(&cli_args.config, &["yml", "yaml"])?;
        let mut pack = pack_configs_from_files(&config_files).unwrap();
        let pipeline_registry = PipelineRegistry::from(&mut pack)?;
        let logger_registry = LoggerRegistry::from(&mut pack)?;
        let model_registry = ModelRegistry::from(&mut pack)?;
        let transform_registry = TransformRegistry::from(&mut pack)?;
        let source_registry = SourceRegistry::from(&mut pack)?;
        let sink_registry = SinkRegistry::from(&mut pack)?;
        let request_registry = RequestRegistry::from(&mut pack)?;
        let connection_registry = ConnectionRegistry::from(&mut pack)?;
        let runner_raw = pack
            .get("runner")
            .map(|runners| runners.get(&cli_args.runner))
            .unwrap_or(None);
        let runner = if let Some(config) = runner_raw {
            serde_yaml_ng::from_value::<RunnerConfig>(config.clone())?
        } else {
            return Err(crate::util::error::CpError::ComponentError(
                "Input CLI Args",
                format!("runner `{}` not found", &cli_args.runner),
            ));
        };
        let results = PipelineResults::new();
        let pipeline_context = DefaultPipelineContext::from(
            results,
            model_registry,
            transform_registry,
            source_registry,
            sink_registry,
            request_registry,
            connection_registry,
            cli_args.execute,
        );
        let pipeline_config = match pipeline_registry.get_pipeline_config(&cli_args.pipeline) {
            Some(x) => x,
            None => {
                return Err(crate::util::error::CpError::ComponentError(
                    "Input CLI Args",
                    format!("pipeline `{}` not found", &cli_args.pipeline),
                ));
            }
        };
        Ok(Runner {
            config: runner,
            logger_registry,
            pipeline_context,
            pipeline_config,
            cli_args,
        })
    }
    pub fn start_log(&mut self) -> CpResult<()> {
        let logger_name = self.config.logger.as_str();
        let console_logger_name = if self.logger_registry.get_logger(logger_name).is_some() {
            "default"
        } else {
            DEFAULT_CONSOLE_LOGGER_NAME
        };
        let pipeline_name = &self.pipeline_config.label;
        self.logger_registry
            .start_logger(console_logger_name, pipeline_name, self.cli_args.console)
    }
    pub fn run(self) -> CpResult<()> {
        let mode = self.config.mode;
        let cron = self.config.schedule;
        let tz = self.config.tz;
        let pipeline = Pipeline::new(&self.pipeline_config);
        let pctx = if mode == RunModeEnum::Loop {
            self.pipeline_context.with_signal()
        } else {
            self.pipeline_context
        };
        let ctx = pipeline.prepare_results(pctx, CHANNEL_BUFSIZE)?;
        match mode {
            RunModeEnum::Debug => pipeline.linear(ctx)?,
            RunModeEnum::Once => pipeline.sync_exec(ctx)?,
            RunModeEnum::Loop => async_runner(pipeline, ctx, cron.as_deref(), tz.as_deref())?,
        }
        Ok(())
    }
}

pub fn async_runner(
    pipeline: Pipeline,
    ctx: Arc<DefaultPipelineContext>,
    cron: Option<&str>,
    tz: Option<&str>,
) -> CpResult<()> {
    let mut rt_builder = tokio::runtime::Builder::new_current_thread();
    rt_builder.enable_all();
    let rt = rt_builder.build().unwrap();
    let event_loop = async move {
        let scheduler = JobScheduler::new().await.expect("failed to create scheduler");
        if let Some(schedule) = cron {
            let timezone = if let Some(t) = tz {
                chrono_tz::Tz::from_str(t).expect("Bad timezone")
            } else {
                chrono_tz::UTC
            };
            let sctx = ctx.clone();
            scheduler
                .add(
                    Job::new_async_tz(schedule, timezone, move |_, _| {
                        let asctx = sctx.clone();
                        Box::pin(async move {
                            match asctx.clone().signal_replace().await {
                                Ok(_) => log::info!("signal_replace"),
                                Err(e) => log::error!("Failed to signal_replace: {}", e),
                            };
                        })
                    })
                    .expect("bad job"),
                )
                .await
                .expect("failed to add job");
        }

        // let src_trigger = async
        let run = async || pipeline.async_exec(ctx.clone()).await;
        let terminator = async || ctx.signal().sigterm_listen().await;
        tokio::join!(run(), terminator());
    };
    rt.block_on(event_loop);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{parser::run_mode::RunModeEnum, runner::common::RunnerConfig};

    #[test]
    fn valid_parse_runner_config() {
        let configs = [
            r#"
logger: mylog
mode: loop
schedule: "* * * * *"
tz: America/New_York
"#,
            r#"
logger: mylog
mode: debug
schedule: "* * * * *"
"#,
            r#"
logger: mylog
mode: once
"#,
        ];
        let runner: Vec<RunnerConfig> = configs
            .iter()
            .map(|config| serde_yaml_ng::from_str(config).unwrap())
            .collect();
        assert_eq!(
            runner,
            vec![
                RunnerConfig {
                    logger: "mylog".to_owned(),
                    mode: RunModeEnum::Loop,
                    schedule: Some("* * * * *".to_owned()),
                    tz: Some("America/New_York".to_owned())
                },
                RunnerConfig {
                    logger: "mylog".to_owned(),
                    mode: RunModeEnum::Debug,
                    schedule: Some("* * * * *".to_owned()),
                    tz: None
                },
                RunnerConfig {
                    logger: "mylog".to_owned(),
                    mode: RunModeEnum::Once,
                    schedule: None,
                    tz: None
                },
            ]
        )
    }
}
