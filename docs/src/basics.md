```@setup basics
using JuliaDB
```

# Basics

JuliaDB offers two main data structures as well as distributed counterparts.  This allows
you to easily scale up an analysis, as operations that work on non-distributed tables 
either work out of the box or are easy to transition for distributed tables.

Here is a high level overview of tables in JuliaDB:

- Tables store data in **columns**.
- Tables are typed.
  - Changing a table in some way therefore requires returning a **new** table (underlying data is not copied).
  - JuliaDB has few mutating operations because a new table is necessary in most cases.

## [`IndexedTable`](@ref)

An [`IndexedTable`](@ref) is wrapper around a (named) tuple of Vectors, but it behaves like
a Vector of (named) tuples.  You can choose to sort the table by any number of primary 
keys (in this case columns `:x` and `:y`).

An `IndexedTable` is created with data in Julia via the [`table`](@ref) function or with 
data on disk via the [`loadtable`](@ref) function.

```@example basics
x = 1:10
y = 'a':'j'
z = randn(10)
t = table((x=x, y=y, z=z); pkey = [:x, :y])
t[1]
t[end]
```

## [`NDSparse`](@ref)

An [`NDSparse`](@ref) has a similar underlying structure to [`IndexedTable`](@ref), but it
behaves like a sparse array with arbitrary indices.  The keys of an `NDSparse` are sorted,
much like the primary keys of an `IndexedTable`.

An `NDSparse` is created with data in Julia via the [`ndsparse`](@ref) function or with 
data on disk via the [`loadndsparse`](@ref) function.

```@example basics
nd = ndsparse((x=x, y=y), (z=z,))
nd[1, 'a']
nd[10, 'j'].z
nd[end]
```

## Selectors

JuliaDB has a variety of ways to select columns.  These selection methods get used across
many JuliaDB's functions: [`select`](@ref), [`reduce`](@ref), [`groupreduce`](@ref), 
[`groupby`](@ref), [`join`](@ref), [`pushcol`](@ref), [`reindex`](@ref), and more.

To demonstrate selection, we'll use the [`select`](@ref) function.  A selection can be any
of the following types:

1. `Integer` -- returns the column at this position.
2. `Symbol` -- returns the column with this name.
3. `Pair{Selection => Function}` -- selects and maps a function over the selection, returns the result.
4. `AbstractArray` -- returns the array itself. This must be the same length as the table.
5. `Tuple` of `Selection` -- returns a table containing a column for every selector in the tuple.
6. `Regex` -- returns the columns with names that match the regular expression.
7. `Type` -- returns columns with elements of the given type.
8. `Not(Selection)` -- returns columns that are not included in the selection.

```@repl basics
t = table(1:10, randn(10), rand(Bool, 10); names = [:x, :y, :z])

# select the :x vector
select(t, 1)
select(t, :x)

# map a function to the :y vector
select(t, 2 => abs)
select(t, :y => x -> x > 0 ? x : -x)

# select the table of :x and :z
select(t, (:x, :z))
select(t, r"(x|z)")

# map a function to the table of :x and :y
select(t, (:x, :y) => row -> row[1] + row[2])
select(t, (1, :y) => row -> row.x + row.y)

# select columns that are subtypes of Integer
select(t, Integer)

# select columns that are not subtypes of Integer
select(t, Not(Integer))
```

## Loading and Saving

```@setup loadsave
using Pkg
Pkg.add("RDatasets")
```

### Loading Data From CSV

Loading a CSV file (or multiple files) into one of JuliaDB's [Data Structures](@ref) is accomplished via the [`loadtable`](@ref) and [`loadndsparse`](@ref) functions.  

```@example loadsave
using JuliaDB, DelimitedFiles

x = rand(10, 2)
writedlm("temp.csv", x, ',')

t = loadtable("temp.csv")
```

!!! note 
    `loadtable` and `loadndsparse` use `Missing` to represent missing values.  To load a CSV that instead uses `DataValue`, see [CSVFiles.jl](https://github.com/queryverse/CSVFiles.jl).  For more information on missing value representations, see [Missing Values](@ref).

### Converting From Other Data Structures

```@example loadsave
using JuliaDB, RDatasets

df = dataset("datasets", "iris")  # load data as DataFrame

table(df)  # Convert DataFrame to IndexedTable
```

### Save Table into Binary Format

A table can be saved to disk (for fast, efficient reloading) via the [`save`](@ref) function.

### Load Table from Binary Format

Tables that have been `save`-ed can be loaded efficiently via [`load`](@ref).