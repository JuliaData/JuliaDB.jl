# Out-of-Core Processing

JuliaDB has several features for loading and processing data tables that are too large to fit in memory (RAM).

## How does this work?

1. Data is distributed into "chunks" that each comfortably fit into memory.
2. Data is processed `Distributed.nprocs()` chunks at a time.  
   - Note that this means multiple chunks need to fit into memory simultaneously.
3. The output of table operations must be small enough to fit into memory.

The out-of-core processing scheme in JuliaDB allows the use of the following functions:

- [`loadtable`](@ref)
- [`loadndsparse`](@ref)
- [`reduce`](@ref)
- [`groupreduce`](@ref)
- [`join`](@ref) (limited to `join(distributed_tbl, tbl)` and no `:outer` joins)

## Loading Data Out-of-Core

!!! note
    Currently, one chunk must contain at least one CSV file.  A single file cannot be
    split into multiple chunks. 

The [`loadtable`](@ref) and [`loadndsparse`](@ref) functions have keyword arguments that tell JuliaDB when to load data out-of-core:

- `output` -- directory where the loaded data should be written to.
- `chunks` -- number of chunks to be generated from the input files.

Here's an example:

```
loadtable(glob("*.csv"), output="bin", chunks=100; kwargs...)
```

Say there are 800 CSV files in the directory, they will be read into 100 chunks of 8 files 
each.  Each worker process will save a chunk to disk once it has ingested 8 files and move
onto the next batch of files.  

!!! warn
    This means (number of workers) × (number of CSVs / chunks) should fit in memory.

Once [`loadtable`](@ref)/[`loadndsparse`](@ref) is finished, you can [`load`](@ref) the 
ingested data into a distributed table with chunks that live on disk.

```julia
load("bin")
```

## `reduce` operations

[`reduce`](@ref) is the most trivial out-of-core operation since it works pair-wise requiring a small, fixed amount of memory. For example, you can sum up the `foo` column using `reduce(+, tbl, select=:foo)`.

The [OnlineStats](https://github.com/joshday/OnlineStats.jl) package allows aggregating and merging statistics on data using a small fixed amount of memory. For example, you can find the mean of the `foo` column via:

```
using OnlineStats
reduce(Mean(), tbl, select=:foo)
```


## `groupreduce` operations

[`groupreduce`](@ref) performs _grouped_ reduction. As long as the number of unique groups in the selected grouping key are small enough, `groupreduce` works out-of-core. As with `reduce`, `groupreduce` can be performed with pair-wise functions or OnlineStats. For example, to find the mean of `foo` field for every unique `bar` and `baz` pairs, you can do:

```
using OnlineStats
groupreduce(Mean(), tbl, (:bar, :baz), select=:foo)
```

## broadcast `join` operations

`join` operations have limited out-of-core support. Specifically,

```
join(bigtable, smalltable, broadcast=:right, how=:inner|:left|:anti)
```

Here `bigtable` can be larger than memory, while `p` copies of `smalltable` must fit in memory (where `p` is number of workers). Note that only `:inner`, `:left`, and `:anti` joins are supported. Notably missing is `:outer` join. In this operation the small table is first broadcast to all processors, and the big table is joined `p` chunks at a time. Hence the name "broadcast join".
