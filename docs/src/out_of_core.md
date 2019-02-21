# Out-of-core processing

JuliaDB can load data that is too big to fit in memory (RAM) as well as run a subset of operations on big tables.  In particular, [OnlineStats Integration](@ref) works with [`reduce`](@ref) and [`groupreduce`](@ref) for running statistical analyses that traditionally would not be possible!

## Processing Scheme

- Data is loaded into a distributed dataset containing "chunks" that safely fit in memory. 
- Data is processed `Distributed.nworkers()` chunks at a time (each worker processes a chunk and then moves onto the next chunk).
  - Note: This means `Distributed.nworkers() * avg_size_of_chunk` will be in RAM simultaneously.
- Output data is accumulated in-memory.

The limitations of this processing scheme is that only certain operations work out-of-core:

- [`loadtable`](@ref)
- [`loadndsparse`](@ref)
- [`load`](@ref)
- [`reduce`](@ref)
- [`groupreduce`](@ref)
- [`join`](@ref) (see [Join to Big Table](@ref))

## Loading Data

The [`loadtable`](@ref) and [`loadndsparse`](@ref) functions accept the keyword arguments `output` and `chunks` that specify the directory to save the data into and the number of chunks to be generated from the input files, respectively.

Here's an example:

```
loadtable(glob("*.csv"), output="bin", chunks=100; kwargs...)
```

Suppose there are 800 `.csv` files in the current directory.  They will be read into 100 chunks (8 files per chunk).  Each worker process will load 8 files into memory, save the chunk into a single binary file in the `bin` directory, and move onto the next 8 files.

!!! note
    `Distributed.nworkers() * (number_of_csvs / chunks)` needs to fit in memory simultaneously.


Once data has been loaded in this way, you can reload the dataset (extremely fast) via

```
tbl = load("bin")
```

## [`reduce`](@ref) and [`groupreduce`](@ref) Operations

`reduce` is the simplest out-of-core operation since it works pair-wise.  You can also perform group-by operations with a reducer via `groupreduce`.

```@example outofcore
using JuliaDB, OnlineStats

x = rand(Bool, 100)
y = x + randn(100)

t = table((x=x, y=y))

groupreduce(+, t, :x; select = :y)
```

You can also replace the reducer with any `OnlineStat` object (see [OnlineStats Integration](@ref) for more details):

```@example outofcore
groupreduce(Sum(), t, :x; select = :y)
```

## Join to Big Table

[`join`](@ref) operations have limited out-of-core support. Specifically,

```
join(bigtable, smalltable; broadcast=:right, how=:inner|:left|:anti)
```

Here `bigtable` can be larger than memory, while `Distributed.nworkers()` copies of `smalltable` must fit in memory. Note that only `:inner`, `:left`, and `:anti` joins are supported (no `:outer` joins). In this operation, `smalltable` is first broadcast to all processors and `bigtable` is joined `Distributed.nworkers()` chunks at a time.
