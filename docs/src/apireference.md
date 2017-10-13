```@meta
CurrentModule = JuliaDB
```

# API documentation

## DNDSparse

```@docs
JuliaDB.DNDSparse
```

## Loading data

```@docs
ingest
ingest!
loadfiles
```

## Saving and Loading tables

Saving an existing `DNDSparse` can be accomplished through the use of the `save` function.  The `save` function has the following help string:

```@docs
save
```

Loading a previously saved `DNDSparse` from disk can be accomplished through use of the `load` function.  The `load` function has the following help string:

```@docs
load
```

## distributing an NDSparse

```@docs
distribute
```

## Compute and collect

Operations in JuliaDB are out-of-core in nature. They return `DNDSparse` objects which can contain parts that are not yet evaluated. `compute` and `collect` are ways to force evaluation.

```@docs
compute(t::DNDSparse)
```

```@docs
collect(t::DNDSparse)
```

## Indexing

```@docs
getindex(t::DNDSparse, idx...)
```

## Queries

```@docs
select(t::DNDSparse, conditions::Pair...)
```

```@docs
select(t::DNDSparse, which::JuliaDB.DimName...; agg)
```

```@docs
aggregate(f, t::DNDSparse)
```

```@docs
aggregate_vec(f, t::DNDSparse)
```

```@docs
filter(f, t::DNDSparse)
```

```@docs
convertdim(t::DNDSparse, d::DimName, xlate; agg::Function, name)
```

```@docs
reducedim(f, t::DNDSparse, dims)
```

```@docs
reducedim_vec(f, t::DNDSparse, dims)
```

## Joins

```@docs
naturaljoin(left::DNDSparse, right::DNDSparse)
```

```@docs
leftjoin{K,V}(left::DNDSparse{K,V}, right::DNDSparse)
```

```@docs
asofjoin(left::DNDSparse, right::DNDSparse)
```

```@docs
merge(left::DNDSparse, right::DNDSparse; agg)
```
