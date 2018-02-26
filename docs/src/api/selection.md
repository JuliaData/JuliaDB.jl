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

## Column special selection

This section describes some special types that can be used to simplify column selection. These types can be used in combination with `select`, `rows` or `columns`, as well as any other function that requires a `by` or `select` argument.

```@docs
All
```

```@docs
Not
```

```@docs
Keys
```

```@docs
Between
```

Finally, to select columns whose name respects a given predicate, pass a function to `select` (or `rows`, or `columns`):

```jldoctest specialselector
julia> t = table([0.01, 0.05], [2,1], [2, 3], names=[:t, :x, :z])
Table with 2 rows, 3 columns:
t     x  z
──────────
0.01  2  2
0.05  1  3

julia> select(t, i -> i != :z)
Table with 2 rows, 2 columns:
t     x
───────
0.01  2
0.05  1
```
