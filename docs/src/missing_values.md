```@setup dv
using JuliaDB
```

# Missing Values

Julia has several different ways of representing missing data.  JuliaDB supports both `Union{T, Missing}` and `DataValue{T}` for missing data.

While `Union{T, Missing}` is the default representation in JuliaDB, functions that can generate missing values (e.g. [`join`](@ref)) have a `missingtype = Missing` keyword argument that can be set to `DataValue`.

#### The [`convertmissing`](@ref) function is used to switch the representation of missing values.
  
```@repl dv
using DataValues
convertmissing(table([1, NA]), Missing)
convertmissing(table([1, missing]), DataValue)
```
#### The [`dropmissing`](@ref) function will remove rows that contain `Missing` or missing `DataValue`s.

```@repl dv
dropmissing(table([1, NA]))
dropmissing(table([1, missing]))
```



