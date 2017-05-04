```@meta
CurrentModule = JuliaDB
```

# JuliaDB.jl

## Overview

JuliaDB is a package for working with large persistent data sets. Given a set of CSV files, it builds and saves an index that allows the data to be accessed efficiently in the future. It also supports an "ingest" mode that converts data to a more efficient binary format.

JuliaDB is based on [Dagger](https://github.com/JuliaParallel/Dagger.jl) and [IndexedTables](https://github.com/JuliaComputing/IndexedTables.jl), providing a distributed-array-like data model. Over time, we hope to expand this to include dense arrays and other Julia array types.

## Installation

```julia
Pkg.clone("https://github.com/JuliaComputing/JuliaDB.jl.git")
```

```@meta
CurrentModule = JuliaDB
```

## Loading data

To use JuliaDB you may start Julia with a few worker processes, for example, `julia -p 4`. Let's load some sample CSV files that are in JuliaDB's test folder:

```@repl fxdata
using JuliaDB

# first use `glob` to list all the files
files = glob("*.csv", Pkg.dir("JuliaDB", "test", "fxsample"))

# loadfiles can load the files in parallel
fxdata = loadfiles(files, header_exists=false,
                   colnames=["pair", "timestamp", "bid", "call"],
                   indexcols=[1, 2])
```

Here we specified that the files don't have a header line (`header_exists`), specified the column names (`colnames`) manually, and also specified that `loadfiles` should use columns 1 and 2 as the index for the data (`indexcols`). The index columns will be used to sort the data for efficient queries. See [the API reference for `loadfiles`](apireference.html#JuliaDB.loadfiles) for all available options.

Notice that the output says `150 rows in 10 chunks`. `loadfiles` creates a distributed table (`DTable`) with as many chunks as the input files. The loaded chunks are distributed across available worker processes. `loadfiles` will also save metadata about the contents of the files in a directory named `.juliadb_cache` under the current working directory. This means, the next time the files are loaded, it will not need to actually parse them to know what's in them. However the files will be parsed once an operation requires the data in it.

Another way to load data into JuliaDB is using [`ingest`](@ref ingest). `ingest` reads and saves the data in an efficient memory-mappable storage format for faster re-reading. You can also add new files to an existing dataset using [`ingest!`](@ref ingest!).

## Indexing

Most lookup and filtering operations on `DTable` are done via indexing. Our `fxdata` object behaves like a 2-d array, accepting two indices:

You can get a specific value by indexing it by the exact index:

```@repl fxdata
fxdata["AUD/NZD", DateTime("2010-03-03T20:27:10.966")]
```

You can also get a subset of the `DTable` by indexing with a range of index values:

```@repl fxdata
fxdata[["EUR/GBP", "USD/JPY"], :]
```

## Permuting dimensions

As with other multi-dimensional arrays, dimensions can be permuted to change the sort order.

With `DTable` the interpretation of this operation is especially natural: simply imagine passing the index columns to the constructor in a different order, and repeating the sorting process:

```@repl fxdata
permutedims(fxdata, [2, 1])
```

Now the data is sorted first by date. In some cases such dimension permutations are needed for performance. The leftmost column is esssentially the primary key --- indexing is fastest in this dimension.

## Select and aggregate

In some cases one wants to consider a subset of dimensions, for example when producing a simplified summary of data.
This can be done by passing dimension (column) numbers (or names, as symbols) to `select`:


```@repl fxdata
select(fxdata, 2)
```

In this case, the result might have multiple values for some indices, and so does not fully behave like a normal array anymore.
Operations that might leave the array in such a state accept the keyword argument `agg`, a function to use to combine all values associated with the same indices.

`select` also supports filtering columns with arbitrary predicates, by passing `column=>predicate` pairs:


```@repl fxdata
select(fxdata, 2=>Dates.isfriday)
```

## Converting dimensions

A location in the coordinate space of an array often has multiple possible descriptions.
This is especially common when describing data at different levels of detail.
For example, a point in time can be expressed at the level of seconds, minutes, or hours.
In our test dataset, we might want to look at monthly values.

This can be accomplished using the `convertdim` function.
It accepts a DTable, a dimension number to convert, a function or dictionary to apply to indices in that dimension, and an aggregation function (the aggregation function is needed in case the mapping is many-to-one).
The following call therefore gives the first entry of each month:


```@repl fxdata
convertdim(fxdata, 2, Dates.month, agg=(x,y)->x)
```

Read about more oprerations in the [API reference](apireference.html) section.
