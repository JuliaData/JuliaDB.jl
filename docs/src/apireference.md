```@meta
CurrentModule = JuliaDB
```

# API documentation

## Loading data

```@docs
ingest
ingest!
loadfiles
```

## Saving and Loading tables

Saving an existing `DTable` can be accomplished through the use of the `save` function.  The `save` function has the following help string:

```@docs
save
```

Loading a previously saved `DTable` from disk can be accomplished through use of the `load` function.  The `load` function has the following help string:

```@docs
load
```

## Compute and gather

Operations in JuliaDB are out-of-core in nature. They return `DTable` objects which can contain parts that are not yet evaluated. `compute` and `gather` are ways to force evaluation.

```@docs
compute(t::DTable)
```

```@docs
gather(t::DTable)
```

## Indexing

```@docs
getindex(t::DTable, idx...)
```

## Queries

```@docs
select(t::DTable, conditions::Pair...)
```

```@docs
select(t::DTable, which::JuliaDB.DimName...; agg)
```

```@docs
aggregate(f, t::DTable)
```

```@docs
aggregate_vec(f, t::DTable)
```

```@docs
filter(f, t::DTable)
```

```@docs
convertdim(t::DTable, d::DimName, xlate; agg::Function, name)
```

```@docs
reducedim(f, t::DTable, dims)
```

```@docs
reducedim_vec(f, t::DTable, dims)
```

## Joins

```@docs
naturaljoin(left::DTable, right::DTable)
```

```@docs
leftjoin{K,V}(left::DTable{K,V}, right::DTable)
```

```@docs
asofjoin(left::DTable, right::DTable)
```

```@docs
merge(left::DTable, right::DTable; agg)
```
