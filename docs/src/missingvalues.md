# Missing Values

Julia has several different ways of representing missing data.  If a column of data may contain missing values, JuliaDB supports both missing value representations of `Union{T, Missing}` and `DataValue{T}`.

While `Union{T, Missing}` is the default representation, functions that generate missing values ([`join`](@ref)) have a `missingtype = Missing` keyword argument that can be set to `DataValue`.

- See the [`convertmissing`](@ref) function for switching from `Missing` to `DataValue` or vice versa.
- The [`dropmissing`](@ref) function will remove rows that contain `Missing` or missing `DataValue`s.

## Differences between `Union{T, Missing}` and `DataValue{T}`

The differences between missing value types are best seen through example.  Here we will create vectors that are identical apart from how missing values are represented.

```@example dv
using DataValues

mask = [rand(Bool) for i in 1:1000]
data = randn(1000)

x = [mask[i] ? missing : data[i] for i in 1:1000]
y = DataValueArray(data, mask)
```



