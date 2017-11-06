# JuliaDB API Reference

## [Data structures](@ref)

Core data structures and indexing.

- [Table](@ref NextTable) - JuliaDB's table datastructure
- [NDSparse](@ref) - N-dimensional sparse array datastructure
- [reindex](@ref) - set a different index for a dataset
- [rechunk](@ref) - re-distribute a distributed dataset

## [Selection](@ref)

Select subsets of columns, map, reduce and filter.

- [Conventions](@ref)
- [map](@ref) - apply a function row-wise
- [filter](@ref) - filter rows
- [dropna](@ref) - drop rows with NA values
- [columns](@ref) - extract struct of column vectors
- [rows](@ref) - extract vector of structs
- [keys](@ref) - iterate NDSparse by keys
- [values](@ref) - iterate NDSparse by values

## [Column manipulation](@ref)

Treat a table as a mutable dictionary of columns.

- [ColDict](@ref) - a dictionary of columns
- [@cols](@ref) - to modify a table with imperative syntax
- [push!](@ref) - add a column at the end
- [setindex!](@ref) - replace a column
- [insert!](@ref) - insert a column
- [insertafter!](@ref) - insert a column after another
- [insertbefore!](@ref) - insert a column before another
- [`dict[]`](@ref getindex(::ColDict, x...)) - get the table from dict.

## [Aggregation](@ref)

Calculate statistics using OnlineStats, grouped aggregation.

- [reduce](@ref) - aggregate a dataset using functions or OnlineStats
- [groupreduce](@ref) - aggregate groups of rows
- [groupby](@ref) - collect groups of rows together
- [reducedim](@ref) - drop a dimension in NDSparse and aggregate

## [Joins](@ref)

- [join](@ref) - join two datasets
- [groupjoin](@ref) - join two datasets by grouping (no nullables!)
- [merge](@ref) - merge two datasets
- [asofjoin](@ref) - time series asof-join

## [Loading and saving](@ref)

- [loadtable](@ref) - load a Table from CSV or binary data
- [loadndsparse](@ref) - load an NDSparse from CSV or binary data
- [save](@ref) - save a Table or NDSparse to in an efficient format
