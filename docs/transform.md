# Transform

One of the 3 types of stages.

The root transform stage is configured as

```yml
transforms:
    my_transform:
        input: $input # (or some actual name)
        output: $output # (or some actual name)
        stages: 
            - select:
                ...
            - join:
                ...
            - drop:
                ...
            - filter:
                ...
            - order:
                ...
            - sort:
                ...

pipelines:
    my_pipeline:
        - ...
        - label: use_my_transform
          task: my_transform
          args:
            input: ACTUAL_INPUT
            output: ACTUAL_OUTPUT
```
