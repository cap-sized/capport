# Logger

### Todo summary

- [ ] Implement keyed logger configurations
- [ ] Setup global logging

## Local log files

Should require configuration:

```yml
logger:
    default: # name of the logger
        # stuff in brackets to be replaced by the actual printed string value.
        stderr: "/log/capport/{PIPELINE_NAME}/{DATE}/{TIME}_{ANY_VAR_NAME}.log"

runner:
    logger: default
    pipeline: mypipe
    runner: sync_lazy # or async_lazy or sync_eager or sync_lazy
    schedule:
        timezone: UTC
        # 1d or 1w or 10s or 2M (month) or 1Y or 10m30s
        every: Monday at 15:00 # internally parse to .every(Monday).at("15:00")
```
