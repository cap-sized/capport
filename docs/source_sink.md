# Sources and Sinks

The 2 other types of stages.

The root source stage is configured as

```yml
sources:
    my_source:
        name: $output
        custom_args: ...

pipelines:
    my_pipeline:
        - ...
        - label: use_my_source
          task: my_source
          args:
            output: ACTUAL_OUTPUT
            # other custom args
```

Sources are highly configurable, and can use connection templates extracted from the 
context (this implementation is up to the individual source type).

Sources must implement `run(ctx)`, a synchronous execution mode, and `fetch(ctx)`, 
an asynchronous execution mode.

## Modes
