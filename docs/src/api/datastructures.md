```@meta
CurrentModule = IndexedTables
DocTestSetup = quote
    using JuliaDB
end
```
# Data structures

## Table

A Table is a collection of tuples or named tuples. These tuples are "rows" of the table. The values of the same field in all rows form a "column".
A Table can be constructed by passing the columns to the `table` function. The `names` argument sets the names of the columns:

```jldoctest tbl
julia> t = table([1,2,3], [4,5,6], names=[:x, :y])
Table with 3 rows, 2 columns:
x  y
────
1  4
2  5
3  6
```

Since a table iterates over rows, indexing with an iteger will return the row at that position:

```jldoctest tbl
julia> row = t[2]
(x = 2, y = 5)

julia> row.x
2

julia> row.y
5
```

The returned value is a named tuple in this case.

Further, indexing a table with a range of indices or generally any array of integer indices will return a new table with those subset of rows.

```jldoctest tbl
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

```jldoctest tbl
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

An `NDSparse` object is a collection of values sparsely distributed over domains which may be discrete or continuous. For example, stock prices are sparsely distributed over the domains of stock ticker symbols, and timestamps.

```jldoctest nds
julia> prices = ndsparse((ticker=["GOOG", "GOOG", "KO", "KO"],
                         date=Date.(["2017-11-10", "2017-11-11",
                                     "2017-11-10", "2017-11-11"])),
                         [1029.74, 1028.23, 46.23, 46.53])
2-d NDSparse with 4 values (Float64):
ticker  date       │
───────────────────┼────────
"GOOG"  2017-11-10 │ 1029.74
"GOOG"  2017-11-11 │ 1028.23
"KO"    2017-11-10 │ 46.23
"KO"    2017-11-11 │ 46.53
```

`NDSparse` maps tuples of indices of arbitrary types to values, just like an Array maps tuples of integer indices to values. Here, the indices are shown to the left of the vertical line, while the values they map to are to the right.

The indexing syntax can be used for lookup:

```jldoctest nds
julia> prices["KO", Date("2017-11-10")]
46.23

julia> prices["KO", :]
2-d NDSparse with 2 values (Float64):
ticker  date       │
───────────────────┼──────
"KO"    2017-11-10 │ 46.23
"KO"    2017-11-11 │ 46.53

julia> prices[:, Date("2017-11-10")]
2-d NDSparse with 2 values (Float64):
ticker  date       │
───────────────────┼────────
"GOOG"  2017-11-10 │ 1029.74
"KO"    2017-11-10 │ 46.23
```


Similarly, other array operations like [`broadcast`](@ref), [`reducedim`](@ref), and [`mapslices`](@ref) are defined for `NDSparse` as for `Array`s.

An NDSparse is constructed using the `ndsparse` function.

```@docs
ndsparse
```

## Indexing

This section describes the `reindex` and `rechunk` functions which let you change the indexed columns in a table or NDSparse, and sort the contents of a distributed table or NDSparse respectively.

```@docs
reindex
```

```@docs
rechunk
```
