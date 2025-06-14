# Logger

## Local log files

Configurable log filter and output path prefix.

```yml
logger:
    default: # name of the logger
        # stuff in brackets to be replaced by the actual printed string value.
        level: debug # allowed values: debug/info/warn/error/trace/off
        # if absolute path, then overrides global output folder. 
        # otherwise if relative path, relative to global output folder
        # support the templating of variables
        output: "{PIPELINE_NAME}/{REF_DATE}/{DATE_TIME_NOW}_{ANY_VAR_NAME}.log"

runner:
    logger: default
    run_with: sync_lazy # or async_lazy or sync_eager or sync_lazy
    schedule:
        timezone: UTC
        # 1d or 1w or 10s or 2M (month) or 1Y or 10m30s
        every: Monday at 15:00 # internally parse to .every(Monday).at("15:00")
```
