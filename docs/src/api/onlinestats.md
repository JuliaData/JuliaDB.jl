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

## Usage on a single column

```@repl ex1
using JuliaDB, OnlineStats

t = table(@NT(x = randn(100), y = randn(100)));
```

### `reduce` via `OnlineStat`

```@repl ex1
reduce(Mean(), t; select = :x)
```

### `reduce` via `Series`
```@repl ex1 
s = Series(Mean(), Variance(), Sum());
reduce(s, t; select = :x)
```

## Usage on multiple columns

If we want the same statistic calculated for each column in the selection, we need to specify
the number of columns. 

```@repl ex1
reduce(2Mean(), t; select = (:x, :y))
```