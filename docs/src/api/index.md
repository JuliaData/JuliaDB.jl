```@meta
CurrentModule = JuliaDB
```

# JuliaDB API Reference

## [Data structures](@ref)

- [`Table`](@ref) - JuliaDB's table datastructure
- [`NDSparse`](@ref) - N-dimensional sparse array datastructure
- [`ColDict`](@ref) - treat a table as a mutable dictionary of columns

## Indexing

- `reindex` - set a different index for a dataset
- `rechunk` - re-distribute a distributed dataset

## [Selection](@ref)

- Conventions in selecting
- `columns` - extract structs of column vectors
- `rows` - extract vector of structs
- `keys` - iterate NDSparse by keys
- `values` - iterate NDSparse by values
- `map` - apply a function row-wise
- `filter` - filter rows
- `dropna` - drop rows with NA values

## [Column manipulation](@ref)

- `setcol` - replace a column
- `movecol` - rename or move column
- `addcol` - insert a new column
- `dropcol` - remove a column

## Aggregation

- `reduce` - aggregate a dataset using functions or OnlineStats
- `groupreduce` - aggregate groups of rows
- `groupby` - collect groups of rows together

## Join

- `join` - join two datasets
- `merge` - merge two datasets

## Updates

- `append` - append new data into a table

## Loading and saving

- `loadtable` - load a Table from CSV or saved datasets
- `loadndsparse` - load an NDSparse from CSV or saved datasets
- `save` - save a Table or NDSparse
