# Transform

### Todo summary

- Transform trait
    - #61
- Column operations
    - Select by index/first/last/range in columns of type list
- Row operations
    - Filter expression parsing
    - Order
    - Limit
- SQL support
    - Integrate [polars_sql](https://docs.rs/polars-sql/0.46.0/polars_sql/index.html)
        - In order to provide support for everything else (groupby, lag, sum/avg/cumsum etc.)

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

#### Column Selection Expressions

The column selection expression parsing is also very limited right now. We support only the following syntax currently:

```
target_expr: COLNAME ('.' COLNAME)*
COLNAME: string_literal
```

TODO: We need to handle the selection across multiple columns.

```
target_expr: COLNAME ('.' COLNAME | VECINDEX)*
VECINDEX: integer
COLNAME: string_literal
```

##### Selection Actions

To support column manipulations we also have `Action`s. Currently the only supported actions are 

- `concat`
- `format`

#### TODO: Row Filter expressions grammar

In YML (not SQL) we want to be able to string together conditional and relational operations.

Here are examples of the extent of the supported grammar (formal language syntax also a todo...)

This will have to be parsed with the `yaml_rust2` package that allows us to traverse nodes.

```yml
# default shorthand for AND expressions
{
    colA: { EQ : $colB },
    colB: { NEQ : string_literal },
    intC: 3, # shorthand for intC : { EQ : 3 }
    intD: [2, 7, 50, 88], # shorthand for intD : {IN : [2, 7, 50, 88]}
    floatE: { GTE: 2.3 }, # GTE (>=), LTE (<=), GT (>), LT (<)
    floatEE: { GTE: 2.3, LT: 5.0 }, # 2.3 <= floatEE < 5.0
    floatF: { NOTIN: [1.2, 3.4, 5.6] },
    colG: NULL, # or colG: NAN
    colH: NOTNULL, # or colH: NOTNAN
}

# explicit AND expressions:
{
    AND: [
        {last: 3},
        {first: $last.subfield.subsubfield}
    ]
}

# the above is equivalent to 
{
    AND: {last: 3, first: $last.subfield.subsubfield}
}

# OR expressions:
{
    OR: [
        { colA : {NEQ : 5.0} },
        { colB : 3 },
    ]
}

# the above is equivalent to 
{
    OR: { colA : {NEQ : 5.0}, colB : 3 },
}

# nested expressions
{
    OR: [
        { colA : {NEQ : 5.0}, colB : 3 }, # and expression
        { AND: [ {last: 3}, {first: $last} ] }, # EXPLICIT and expression
        { OR: [ { grp_id: { GT: 19, LT: 88 } }, { mvp: kuch } ] }, # EXPLICIT or expression
    ]
}
```

#### Row Order operation

```yml
- order:  # in order of descending priority
    - year: asc # or 1 or ASC
    - round: asc # or 1
    - pick: asc # or 1
    - standings: desc # or -1
```

#### Row Limit operation

```yml
# full form
- limit: 
    max: 50
    offset: 10

# or more shorthands:
- head : 10 # first 10
- tail : 10 # last 10
```

## Config examples

- [Enrich](https://github.com/cap-sized/capport/blob/main/config/example/transform.yml)
- [Formatting](https://github.com/cap-sized/capport/blob/main/config/transform.yml#L3)