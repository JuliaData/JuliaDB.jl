# Out-of-core processing

JuliaDB can be used to load and work with data that are too big to fit in memory (RAM). Several queries are designed to work on such datasets.

## Processing scheme

The basic scheme of out-of-core processing is this:

1. Data is loaded into a distributed dataset containing "chunks" that are of small enough to fit in memory
2. Data is processed `p` chunks at a time -- where `p` is the number of worker processes. This means `p * size of chunks` should fit in memory!
3. Output data is accumulated in-memory and must be small enough to fit in the available memory.

Further, data is memory-mapped from disk so as to minimize IO overhead.

Note that this processing scheme means that not all operations in JuliaDB work out-of-core. There are several operations that do work right now as described in the rest of the document. We are working to make the coverage of out-of-core operations more comprehensive.

## Loading data out-of-core

[`loadtable`](@ref) and [`loadndsparse`](@ref) functions take an `output` keyword argument which can be set to a directory where the loaded data is written to in an efficient binary format. It's also necessary to specify the `chunks` option to these functions which specify how many output chunks are to be generated from the input files.

An example invocation may look like:

```
loadtable(glob("*.csv"), output="bin", chunks=100; kwargs...)
```

If there are, say, 1000 `.csv` files in the current directory, they will be read into 100 chunks (10 CSV files will be read to create a single chunk). Once a batch of 10 CSV files is read, the data is written to a single binary file in the `bin` directory. Now let's say you have 10 worker processes. Each process will load chunks of 10 files each, meaning the data in up to 100 files may be loaded to memory before being written to disk.

Once `loadtable` has completed, you can load the ingested data using `load`:

```
tbl = load("bin")
```

`tbl` is now a distributed table made of chunks which are on disk.

## `reduce` operations

[`reduce`](@ref) is the most trivial out-of-core operation since it works pair-wise requiring a small, fixed amount of memory. For example, you can sum up the `foo` column using `reduce(+, tbl, select=:foo)`.

The OnlineStats.jl package (which is shipped with JuliaDB) allows aggregating and merging statistics on data using a small fixed amount of memory as well. For example, you can find the mean of the `foo` column with this code:

```
using OnlineStats
reduce(Mean(), tbl, select=:foo)
```

Check out other [handy `OnlineStat`s](http://joshday.github.io/OnlineStats.jl/stable/stats_and_models.html). OnlineStats.jl also allows you to extract [histograms](http://joshday.github.io/OnlineStats.jl/stable/datasurrogates.html#IHistogram-1) or [partitioned stats](http://joshday.github.io/OnlineStats.jl/latest/visualizations.html) (i.e. stats on a fixed window of data, hence reducing the output size)

## `groupreduce` operations

[`groupreduce`](@ref) performs _grouped_ reduction. As long as the number of unique groups in the selected grouping key are small enough, `groupreduce` works out-of-core. `groupreduce` can be performed with pair-wise functions or OnlineStats, as with `reduce`. For example, to find the mean of `foo` field for every unique `bar` and `baz` pairs, you can do:


```
using OnlineStats
groupreduce(Mean(), tbl, (:bar, :baz), select=:foo)
```

Note that [`groupby`](@ref) operations may involve an expensive data shuffling step as it requires data belonging to the same group to be on the same processor, and hence isn't generally out-of-core.

## broadcast `join` operations

`join` operations have limited out-of-core support. Specifically,

```
join(bigtable, smalltable, broadcast=:right, how=:inner|:left|:anti)
```

Here `bigtable` can be larger than memory, while `p` copies of `smalltable` must fit in memory (where `p` is number of workers). Note that only `:inner`, `:left`, and `:anti` joins are supported. Notably missing is `:outer` join. In this operation the small table is first broadcast to all processors, and the big table is joined `p` chunks at a time. Hence the name "broadcast join".
