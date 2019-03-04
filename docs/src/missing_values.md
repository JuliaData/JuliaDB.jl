```@setup dv
using JuliaDB, Pkg
Pkg.add("DataValues")
```

# Missing Values

Julia has several different ways of representing missing data.  If a column of data may contain missing values, JuliaDB supports both missing value representations of `Union{T, Missing}` and `DataValue{T}`.

While `Union{T, Missing}` is the default representation, functions that generate missing values ([`join`](@ref)) have a `missingtype = Missing` keyword argument that can be set to `DataValue`.

- The [`convertmissing`](@ref) function is used to switch the representation of missing values.
  
```@repl dv
using DataValues
convertmissing(table([1, NA]), Missing)
convertmissing(table([1, missing]), DataValue)
```
- The [`dropmissing`](@ref) function will remove rows that contain `Missing` or missing `DataValue`s.

```@repl dv
dropmissing(table([1, NA]))
dropmissing(table([1, missing]))
```



