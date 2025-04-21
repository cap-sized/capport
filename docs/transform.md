# Transform

### Todo summary

- Transform trait
    - #61
- Selection expression parsing
- Wishlist 
    - Full scale SQL syntax parsing

## Overview

Transform tasks start at the `RootTransform`. Each `RootTransform` holds a list of transform stages, 
which are the operations to be executed in order.

Every transform operation (including `RootTransform`) must implement the trait `Transform`.

TODO: `run_eager` is not yet implemented. 

```rs
pub trait Transform {
    fn run_lazy(&self, curr: LazyFrame, results: Arc<RwLock<PipelineResults<LazyFrame>>>) -> CpResult<LazyFrame>;
    fn run_eager(&self, curr: DataFrame, results: &PipelineResults<DataFrame>) -> SubResult<DataFrame>;
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result;
}
```

### Transform operations

Transform tasks are meant to act like SQL operations. Hence ideally we would like to support full SQL **queries**
in transform nodes, but due to the complexity of the above, we currently just support a minimal set 
of operations in 

- `select`: `SelectTransform`
- `join`: `JoinTransform`
- `drop`: `DropTransform` (which drops columns, not rows)

TODO: currently we only support column operations, we need to minimally add the following as well:

- `filter`: `FilterTransform`
- `order`: `OrderTransform` (sorting, with asc/desc toggles)
- `limit`: `LimitTransform` (limit, with option for offset)
- `group_by`: `GroupByTransform`

#### Selection Expressions

The selection expression parsing is also very limited right now. We support only the following syntax currently:

```
target_expr: COLNAME ('.' COLNAME)*
COLNAME: string_literal
```

TODO: We need to handle the selection across multiple columns

#### Selection Actions

To support column manipulations we also have `Action`s. Currently the only supported actions are 

- `concat`
- `format`

#### Filter expressions

In YML (not SQL) we want to 

```yml

# OR expressions:
[
    { columnA: value1 }, # 
]
```

## Config examples


## Roadmap