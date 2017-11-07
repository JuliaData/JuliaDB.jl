# JuliaDB API Reference

## [Data structures](@ref)

Core data structures and indexing.

- [Table](@ref) - JuliaDB's table datastructure
- [NDSparse](@ref) - N-dimensional sparse array datastructure
- [reindex](@ref) - set a different index for a dataset
- [rechunk](@ref) - re-distribute a distributed dataset

## [Selection](@ref)

Select subsets of columns, map, and filter.

- [select](@ref) - select and transform a column or a subset of columns
- [map](@ref) - apply a function row-wise
- [filter](@ref) - filter rows
- [dropna](@ref) - drop rows with NA values
- [columns](@ref) - extract struct of column vectors
- [rows](@ref) - extract vector of structs
- [keys](@ref) - vector of keys of an NDSparse
- [values](@ref) - vector of values of an NDSparse

Derivatives of [`select`](@ref) that are convenient for modifying a table's columns.

- [pushcol](@ref) - add a column at the end
- [setcol](@ref) - replace a column
- [insertcol](@ref) - insert a column
- [insertcolafter](@ref) - insert a column after another
- [insertcolbefore](@ref) - insert a column before another

## [Aggregation](@ref)

Calculate statistics using OnlineStats, grouped aggregation.

- [reduce](@ref) - aggregate a dataset using functions or OnlineStats
- [groupreduce](@ref) - aggregate groups of rows using functions or OnlineStats
- [groupby](@ref) - collect groups of rows together
- [reducedim](@ref) - drop a dimension in NDSparse and aggregate

## [Joins](@ref)

Combine two or more tables in various join and merge operations.

- [join](@ref) - join two datasets
- [groupjoin](@ref) - join two datasets by grouping (no nullables!)
- [merge](@ref) - merge two datasets
- [asofjoin](@ref) - time series asof-join

## [Loading and saving](@ref)

- [loadtable](@ref) - load a Table from CSV or binary data
- [loadndsparse](@ref) - load an NDSparse from CSV or binary data
- [save](@ref) - save a Table or NDSparse to in an efficient format
