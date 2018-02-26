```@meta
CurrentModule = JuliaDB
DocTestSetup = quote
    using JuliaDB
end
```
# JuliaDB API Reference

## [Data structures](@ref)

Core data structures and indexing.

- [Table](@ref) - JuliaDB's table datastructure
- [NDSparse](@ref) - N-dimensional sparse array datastructure
- [`reindex`](@ref) - set a different index for a dataset
- [`rechunk`](@ref) - re-distribute a distributed dataset

## [Selection](@ref)

Select subsets of columns, map, and filter.

- [`select`](@ref) - select and transform a column or a subset of columns
- [`map`](@ref) - apply a function row-wise
- [`filter`](@ref) - filter rows
- [`dropna`](@ref) - drop rows with NA values
- [`columns`](@ref) - extract struct of column vectors
- [`rows`](@ref) - extract vector of structs
- [`keys`](@ref) - vector of keys of an NDSparse
- [`values`](@ref) - vector of values of an NDSparse

Derivatives of [`select`](@ref) that are convenient for modifying a table's columns.

- [`setcol`](@ref) - replace a column
- [`pushcol`](@ref) - add a column at the end
- [`popcol`](@ref) - remove a column
- [`insertcol`](@ref) - insert a column
- [`insertcolafter`](@ref) - insert a column after another
- [`insertcolbefore`](@ref) - insert a column before another
- [`renamecol`](@ref) - rename a column

Special selector to select a subset of columns.

- [`All`](@ref) - select all columns
- [`Not`](@ref) - select complementary
- [`Keys`](@ref) - select primary columns
- [`Between`](@ref) - select columns between two extremes

## [Aggregation](@ref)

Grouping and reduction with functions or Online statistics.

- [`reduce`](@ref) - aggregate a dataset using functions or OnlineStats
- [`groupreduce`](@ref) - aggregate groups of rows using functions or OnlineStats
- [`groupby`](@ref) - collect groups of rows together
- [`summarize`](@ref) - apply summary functions to selected columns
- [`reducedim`](@ref) - drop a dimension in NDSparse and aggregate

## [Joins](@ref)

Combine two or more tables in various join and merge operations.

- [`join`](@ref) - join two datasets
- [`groupjoin`](@ref) - join two datasets by grouping (no nullables!)
- [`merge`](@ref) - merge two datasets
- [`asofjoin`](@ref) - time series asof-join

## [Loading and saving](@ref)

- [`loadtable`](@ref) - load a Table from CSV or binary data
- [`loadndsparse`](@ref) - load an NDSparse from CSV or binary data
- [`save`](@ref) - save a Table or NDSparse to in an efficient format
