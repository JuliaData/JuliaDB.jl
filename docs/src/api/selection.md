```@meta
CurrentModule = IndexedTables
DocTestSetup = quote
    using JuliaDB
end
```
# Selection

Select, transform and filter a table.

## Select

```@docs
select
```

## Map

```@docs
map(f, t::NextTable; kwargs...)
```

```@docs
map(f, t::NDSparse; kwargs...)
```
## Filter

```@docs
filter
```

```@docs
dropna
```

## AoS and SoA

This section describes extracting struct of arrays (`columns`) and array of structs (`rows`) from a table or an NDSparse. (Wikipedia entry on [AoS and SoA](https://en.wikipedia.org/wiki/AOS_and_SOA))

```@docs
columns
```

```@docs
rows
```

```@docs
keys
```

```@docs
values
```

## Column modification

This section describes functions that can modify the set of columns of a table. Note that these functions return new tables and doesn't mutate the existing table. This is done so that type information for a given table is always available and correct.


```@docs
setcol
```

```@docs
pushcol
```

```@docs
popcol
```

```@docs
insertcol
```

```@docs
insertcolafter
```

```@docs
insertcolbefore
```

```@docs
renamecol
```
