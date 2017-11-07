```@meta
CurrentModule = IndexedTables
DocTestSetup = quote
    using JuliaDB
end
```
# Data structures

## Table

A Table is an iterator of tuples or named tuples. These tuples are "rows" of the table. The value of the same field in all rows form a "column".
A Table can be constructed by passing the columns to the `table` function. The `names` argument sets the names of the columns:

```jldoctest
julia> t = table([1,2,3], [4,5,6], names=[:x, :y])
Table with 3 rows, 2 columns:
x  y
────
1  4
2  5
3  6
```

Since a table iterates over rows, indexing with an iteger will return the row at that position:

```jldoctest
julia> row = t[2]
(x = 2, y = 5)

julia> row.x
2

julia> row.y
5
```

The returned value is a named tuple in this case.

Further, indexing a table with a range of indices or generally any array of integer indices will return a new table with those subset of rows.

```jldoctest
julia> t[2:3]
Table with 2 rows, 2 columns:
x  y
────
2  5
3  6

julia> t[[1,1,3]]
Table with 3 rows, 2 columns:
x  y
────
1  4
1  4
3  6

```
 Optionally, a subset of fields can be chosen as "primary key". The rows are kept sorted in lexicographic order of the primary key fields. The benefits are:

1. It makes lookup, grouping, join and sort operations fast when the primary key fields are involved.
2. It provides a natural default for operations such as [`groupby`](@ref) and [`join`](@ref)

Passing the `pkey` option to `table` constructor will select the primary keys.

```jldoctest
julia> b = table([2,1,2,1],[2,3,1,3],[4,5,6,7], names=[:x,:y,:z], pkey=(:x,:y))
Table with 4 rows, 3 columns:
x  y  z
───────
1  3  5
1  3  7
2  1  6
2  2  4
```

Note that the output table is sorted by the primary key fields.

Below is the full documentation of the `table` constructor:

```@docs
table
```

## NDSparse

An NDSparse array datastructure

An NDSparse is constructed using the `ndsparse` function.

```@docs
ndsparse
```

## Indexing

```@docs
reindex
```

```@docs
rechunk
```
