# OnlineStats Integration

[OnlineStats.jl](https://github.com/joshday/OnlineStats.jl) is a package for calculating 
statistics and models with online (one observation at a time) parallelizable algorithms.  
This integrates tightly with JuliaDB's distributed data structures to calculate statistics
on large datasets.

For the full OnlineStats documentation, see [http://joshday.github.io/OnlineStats.jl/stable/](http://joshday.github.io/OnlineStats.jl/stable/).

## Basics

Each statistic/model is a subtype of `OnlineStat`.  `OnlineStat`s are grouped together in 
a `Series`.  In JuliaDB, the functions [`reduce`](@ref) and [`groupreduce`](@ref) can accept:

1. An `OnlineStat`
1. A tuple of `OnlineStat`s
1. A `Series`


### Example Table

```@repl ex1
using JuliaDB, OnlineStats

t = table(@NT(x = randn(100), y = randn(100), z = rand(1:5, 100)))
```

## Usage on a single column

### `reduce` via `OnlineStat`

```@repl ex1
reduce(Mean(), t; select = :x)
```

### `reduce` via Tuple of `OnlineStat`s

```@repl ex1
reduce((Mean(), Variance()), t; select = :x)
```

### `reduce` via `Series`
```@repl ex1 
s = Series(Mean(), Variance(), Sum());
reduce(s, t; select = :x)
```

## Usage on multiple columns

### Same `OnlineStat` on each column

If we want the same statistic calculated for each column in the selection, we need to specify
the number of columns. 

```@repl ex1
reduce(2Mean(), t; select = (:x, :y))
```

### Different `OnlineStat`s on columns

To calculate different statistics on different columns, we need to make a `Group`, which can
be created via `hcat`.

```@repl ex1 
s = reduce([Mean() CountMap(Int)], t; select = (:x, :z))

value(stats(s)[1])
```