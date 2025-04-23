# Transform

### Todo summary

- [ ] Implement `run_eager`
- [ ] Variable replacement
    - e.g. keyword for TODAY, or some domain specific keys
- [ ] Column operations
    - [ ] Select by index/first/last/range in columns of type list
    - [ ] Join to any table, not just the one literally defined in transform config
- [ ] Row operations
    - [ ] Filter expression parsing
    - [ ] Order
    - [ ] Limit
- [ ] SQL support
    - [ ] Integrate [polars_sql](https://docs.rs/polars-sql/0.46.0/polars_sql/index.html)
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
- `flatten`: `FlattenTransform`
- `extend`: `ExtendTransform` (extends the dataframe)

### Variable argument passing (kwargs) 

TODO: Borrows the term kwargs from python, we want to pass in keyword arguments through the args parameter

```yml
transform:
    mytask:
        ...
        # in transform
        - join:
            right: $$state_province_table_name # to get replaced with "STATE_PROVINCE" at runtime
            right_select: 
            birth_state_province_code: code
            birth_state_province_name: name
            left_on: birth_state_province_name
            right_on: birth_state_province_name
            how: left
pipeline:
    mypipeline:
        ...
        # in stage
        - label: stage_name
        task: transform
        args:
            name: mytask
            input: PERSON
            save_df: PERSON_BIRTHSTATE
            kwargs:
                state_province_table_name: STATE_PROVINCE
```

### Column Selection Expressions

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

#### Selection Actions

To support column manipulations we also have `Action`s. Currently the only supported actions are 

- `concat`
- `format`

#### `select` Example

```yml
    - select:
        id: csid # from the previous step
        first_name: firstName.default # handle subpaths
        last_name: lastName.default # handle subpaths
        full_name: 
          action: concat # str concat, default with space
          args: 
            cols: [ "firstName.default", "lastName.default" ] 
            separator : " "
        # by default, empty value for the key defaults to using the current key to select from the table.
        birthdate: 
        # the above is equivalent to {birthdate: birthdate}
        birth_city: birthCity.default
        birth_state_province_name: birthStateProvince.default
        birth_state_country_code: birthCountry.default
```

### Drop columns

Simply drops columns if they are present or labelled true. Explicitly disable dropping by providing the value 
`false`.

```yml
    - drop:
        birth_state_province_name: True 
        # equivalent to 
        # birth_state_province_name: true 
        # birth_state_province_name:
        # equivalent to 
        birth_state: False 
        # birth_state: false 
```

### Join table

The following fields are mandatory:

- `right` refers to an existing result from the context's currently available `PipelineResults`.
- `left_on` and `right_on` refers to the columns to match on
- `how` is the method of joining (left/full/cross/right/inner)

The following fields are optional:

- `right_select` refers to the columns to select from the right joined table.
    - if omitted, selects the whole right table.

```yml
    - join:
        right: STATE_PROVINCE # named result
        right_select: 
          birth_state_province_code: code
          birth_state_province_name: name
        left_on: birth_state_province_name
        right_on: birth_state_province_name
        how: left
```


#### Selection Actions

To support column manipulations we also have `Action`s. Currently the only supported actions are 

- `concat`
- `format`

### TODO: Flatten nested list

Example [data.json](https://json.org/example.html) from:

```json
{"menu": {
    "header": "SVG Viewer",
    "items": [
        {"id": "Open"},
        {"id": "OpenNew", "label": "Open New"},
        {"id": "ZoomIn", "label": "Zoom In"},
        {"id": "ZoomOut", "label": "Zoom Out"},
        {"id": "OriginalView", "label": "Original View"},
        {"id": "Quality"},
        {"id": "Pause"},
        {"id": "Mute"},
        {"id": "Find", "label": "Find..."},
        {"id": "FindAgain", "label": "Find Again"},
        {"id": "Copy"},
        {"id": "CopyAgain", "label": "Copy Again"},
        {"id": "CopySVG", "label": "Copy SVG"},
        {"id": "ViewSVG", "label": "View SVG"},
        {"id": "ViewSource", "label": "View Source"},
        {"id": "SaveAs", "label": "Save As"},
        {"id": "Help"},
        {"id": "About", "label": "About Adobe CVG Viewer..."}
    ]
}}
```

To: 
```yml
---
[
    {"header" : "SVG Viewer", "id": "Open"},
    {"header" : "SVG Viewer", "id": "OpenNew", "label": "Open New"},
    {"header" : "SVG Viewer", "id": "ZoomIn", "label": "Zoom In"},
    {"header" : "SVG Viewer", "id": "ZoomOut", "label": "Zoom Out"},
    {"header" : "SVG Viewer", "id": "OriginalView", "label": "Original View"},
]
```

To select every item's `id` and `label` (if present):

```yml
transform:
  - select:
      header: menu.header
      first_id: menu.items[0]id # explode each `items`'s list of ids
      label: menu.items[*]label # if label exists, do the same
```

### TODO: Row Filter expressions grammar

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

### Row Order operation

```yml
- order:  # in order of descending priority
    - year: asc # or 1 or ASC
    - round: asc # or 1
    - pick: asc # or 1
    - standings: desc # or -1
```

### Row Limit operation

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