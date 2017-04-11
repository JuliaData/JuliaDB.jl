```@meta
CurrentModule = JuliaDB
```
# JuliaDB

Parallel N-dimensional sparse data manipulation library.

## Installation

```julia
Pkg.clone("https://github.com/JuliaComputing/JuliaDB.jl")
```

### Loading data from CSV files

JuliaDB can load data from many CSV files in parallel.

`ingest` and `loadfiles` are two functions that let you do that. The difference between them is that `ingest` will also write the data to disk in a binary format that is quick to read, while `loadfiles` loads the data into memory.

```@docs
ingest
```

```@docs
loadfiles
```

## Saving and loading data

The result of any operation can be saved using the `save` function.

```@docs
save
```

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

## Appendix

```@docs
IndexedTable
```

```@docs
csvread
```
