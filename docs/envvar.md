# Environment Variables

## Overview

Capport provides the ability to set environment variables for this running instance that any
pipeline can access. The `EnvironmentVariableRegistry` can build and populate the default
argument inputs from the command line. 

- `CONFIG_DIR` [mandatory]
- `OUTPUT_DIR` [mandatory]
- `REF_DATE`
- `REF_DATETIME`

These can be referenced from any pipeline so long as the `EnvironmentVariableRegistry` is still
alive. The variables get deregistered when the registry is dropped.