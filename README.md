# JuliaDB

[![JuliaDB](https://juliarun-ci.s3.amazonaws.com/push/JuliaComputing/JuliaDB/julia_0_5.svg)](https://juliarun-ci.s3.amazonaws.com/push/JuliaComputing/JuliaDB/julia_0_5.log) [![JuliaDB](https://juliarun-ci.s3.amazonaws.com/push/JuliaComputing/JuliaDB/julia_0_6.svg)](https://juliarun-ci.s3.amazonaws.com/push/JuliaComputing/JuliaDB/julia_0_6.log)

**Setup:**

```julia
Pkg.clone("https://github.com/JuliaParallel/Dagger.jl.git")
Pkg.clone("https://github.com/JuliaComputing/IndexedTables.jl.git")
Pkg.clone("https://github.com/shashi/TextParse.jl.git")
Pkg.clone("git@github.com:JuliaComputing/JuliaDB.jl.git")
```

**Loading some CSV files:**

Start Julia with many processes `julia -p N` and load JuliaDB with `using JuliaDB`.

From a directory with a bunch of CSV files, try:

```julia
load(glob("*.csv"))
```

Options for `load`:

```julia
load(files::AbstractVector, delim=',';
      indexcols=Int[],
      datacols=Int[],
      agg=nothing,
      presorted=false,
      copy=false,
      csvopts...)
```


`indexcols` is a vector of column indices to be used as the index, and `datacols` is a vector of column indices to be used as the data for the resulting table. `agg`, `presorted` and `copy` are the corresponding keyword arguments passed to `NDSparse` constructor.

You can also pass in any keyword arguments accepted by [`TextParse.csvread`](https://github.com/shashi/TextParse.jl/blob/master/src/csv.jl#L13-L32) to configure the CSV reading process. Once loaded JuliaDB will save some metadata about each file loaded in `./.juliadb_cache`, so that the next time you load the same files it just reads the metadata from the cache instead of recomputing it. Note that the metadata is specific to the set of keyword arguments provided to `load`. Loading the dataset with a changed set of options will re-read the CSV files, and add metadata about files loaded with the new configuration to the cache file.

`load` returns a `DTable` object.

To turn the `DTable` object into an `NDSparse` object with all the data merged together, call `gather(::DTable)`.

**Currently functional API**

- `getindex`: Scalar getindex returns the value, getindex with a range returns a `DTable` object.

- `select(arr::DTable, conditions::Pair...)`

Filter based on index columns. Conditions are accepted as column-function pairs.

Example: `select(arr, 1 => x->x>10, 3 => x->x!=10 ...)`, returns a `DTable`

- `convertdim(x::DTable, d::DimName, xlate; agg::Function, name)`

Apply function or dictionary `xlate` to each index in the specified dimension.
If the mapping is many-to-one, `agg` is used to aggregate the results.
`name` optionally specifies a name for the new dimension. `xlate` must be
a monotonically increasing function.
