```@meta
CurrentModule = JuliaDB
```

# Using JuliaDB

## Construction of an IndexedTable

The `IndexedTable` constructor accepts a series of vectors.
The last vector contains the data values, and the first N vectors contain the indices for each of the N dimensions.
As an example, let's construct an array of the high temperatures for three days in two cities:

```@repl temperatures
using IndexedTables, JuliaDB
hitemps = IndexedTable([fill("New York",3); fill("Boston",3)],
                       repmat(Date(2016,7,6):Date(2016,7,8), 2),
                       [91,89,91,95,83,76])
```

Notice that the data was sorted first by city name, then date, giving a different order than we initially provided.
On construction, `IndexedTable` takes ownership of the columns and sorts them in place (the original vectors are modified).

## Conversion of a Local IndexedTable to a distributed JuliaDB Table

One can convert an existing `IndexedTable` to a JuliaDB `DTable` through the use of the `distribute` function.

```@repl temperatures
dhitemps = distribute(hitemps, 2)
```

The first argument provided to `distribute` is an existing `IndexedTable` and the second argument describes how the indexed table should be distributed amongst worker processes.  If the second argument is a scalar of value `n`, then the `IndexedTable` will be split into `n` approximately equal chunks across the worker processes.  If the second argument is a vector of n integers, then the distributed table with n separate chunks with each chunk having the number of rows present in each element of that vector.

## Importing data

### Reading from CSV files

Importing data from column-based sources is straightforward.  JuliaDB currently provides two distinct methods for importing data: `loadfiles` and `ingest`.  Both functions load the contents of one or more CSV files in a given directory and return a `DTable` of the loaded data.  The `ingest` function has the additional property of transforming the data into an efficient internal storage format, and saving both the original data and associated JuliaDB metadata to disk in a provided output directory.

The argument signature and help for `ingest` is the following:

```@docs
ingest
```

The argument signature and help for `loadfiles` is the following:

```@docs
loadfiles
```

As stated above in the help text, each function has a set of optional input arguments that are specific to that particular function, as well as the ability to pass a set of trailing input arguments that are subsequently passed on to [`TextParse.csvread`](https://juliacomputing.com/TextParse.jl/stable/index.html#TextParse.csvread).

An in-place variant of the `ingest!` function will append data from new files on to an existing `DTable` stored in a defined `outputdir`.  The help string for the in-place version of `ingest!` is the following:

```@docs
ingest!
```

### Saving and Loading existing JuliaDB DTables

Saving an existing `DTable` can be accomplished through the use of the `save` function.  The `save` function has the following help string:

```@docs
save
```

Loading a previously saved `DTable` from disk can be accomplished through use of the `load` function.  The `load` function has the following help string:

```@docs
load
```

## Indexing

Most lookup and filtering operations on `DTable` are done via indexing.
Our `dhitemps` array behaves like a 2-d array of integers, accepting two indices:

```@repl temperatures
dhitemps["Boston", Date(2016,7,8)]
```

If the given indices exactly match the element types of the index columns, then the result is a scalar.
In other cases, a new `DTable` is returned, giving data for all matching locations:

```@repl temperatures
dhitemps["Boston", :]
```

## Permuting dimensions

As with other multi-dimensional arrays, dimensions can be permuted to change the sort order.
With `DTable` the interpretation of this operation is especially natural: simply imagine passing the index columns to the constructor in a different order, and repeating the sorting process:


```@repl temperatures
permutedims(dhitemps, [2, 1])
```

Now the data is sorted first by date.
In some cases such dimension permutations are needed for performance.
The leftmost column is esssentially the primary key --- indexing is fastest in this dimension.

## Select and aggregate

In some cases one wants to consider a subset of dimensions, for example when producing a simplified summary of data.
This can be done by passing dimension (column) numbers (or names, as symbols) to `select`:


```@repl temperatures
select(dhitemps, 2)
```

In this case, the result has multiple values for some indices, and so does not fully behave like a normal array anymore.
Operations that might leave the array in such a state accept the keyword argument `agg`, a function to use to combine all values associated with the same indices:


```@repl temperatures
select(dhitemps, 2, agg=max)
```

The aggregation operation can also be done by itself, in-place, using the function `aggregate!`.

`select` also supports filtering columns with arbitrary predicates, by passing `column=>predicate` pairs:


```@repl temperatures
select(dhitemps, 2=>Dates.isfriday)
```

## Converting dimensions

A location in the coordinate space of an array often has multiple possible descriptions.
This is especially common when describing data at different levels of detail.
For example, a point in time can be expressed at the level of seconds, minutes, or hours.
In our toy temperature dataset, we might want to look at monthly instead of daily highs.

This can be accomplished using the `convertdim` function.
It accepts a DTable, a dimension number to convert, a function or dictionary to apply to indices in that dimension, and an aggregation function (the aggregation function is needed in case the mapping is many-to-one).
The following call therefore gives monthly high temperatures:


```@repl temperatures
convertdim(dhitemps, 2, Dates.month, agg=max)
```

## Named columns

`DTable` and `IndexedTable` are built on a simpler data structure called `Columns` that groups a set of vectors together.
This structure is used to store the index part of an `IndexedTable`, and a `IndexedTable` can be constructed by passing one of these objects directly.
`Columns` allows names to be associated with its constituent vectors.
Together, these features allow `IndexedTable` and `DTable` arrays with named dimensions:


```@repl temperatures
hitemps = IndexedTable(Columns(city = [fill("New York",3); fill("Boston",3)],
                               date = repmat(Date(2016,7,6):Date(2016,7,8), 2)),
                               [91,89,91,95,83,76])
dhitemps = distribute(hitemps,2)
```

Now dimensions (e.g. in `select` operations) can be identified by symbol
(e.g. `:city`) as well as integer index.

A `Columns` object itself behaves like a vector, and so can be used to represent the data part of a `DTable`.
This provides one possible way to store multiple columns of data:


```@repl temperatures
t = IndexedTable(Columns(x = rand(4), y = rand(4)),
                 Columns(observation = rand(1:2,4), confidence = rand(4)))
dt = distribute(t, 2)
```

In this case the data elements are structs with fields `observation` and `confidence`, and can be used as follows:


```@repl temperatures
filter(d->d.confidence > 0.5, dt)
```
