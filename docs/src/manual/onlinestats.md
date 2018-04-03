# OnlineStats Integration

[**OnlineStats**](https://github.com/joshday/OnlineStats.jl) is a package for calculating 
statistics and models with online (one observation at a time) parallelizable algorithms.  
This integrates tightly with JuliaDB's distributed data structures to calculate statistics
on large datasets.

For the full OnlineStats documentation, see [http://joshday.github.io/OnlineStats.jl/stable/](http://joshday.github.io/OnlineStats.jl/stable/).

---

## Basics

Each `OnlineStat` can be updated with more data and merged together with another of the 
same type.  JuliaDB integrates with OnlineStats via the [`reduce`](@ref) and 
[`groupreduce`](@ref) functions by accepting an `OnlineStat` or tuple of `OnlineStats`.


### Example Table

```@repl ex1
using JuliaDB, OnlineStats

t = table(@NT(x = randn(100), y = randn(100), z = rand(1:5, 100)))
```

---

## Usage on a single column

### `reduce` via `OnlineStat`

```@repl ex1
reduce(Mean(), t; select = :x)
```

Several `OnlineStat`s can be calculated on the same column by joining them via `Series`.

```@repl ex1 
reduce(Series(Mean(), Variance()), t; select = :x)
```

### `reduce` via Tuple of `OnlineStat`s

```@repl ex1
reduce((Mean(), Variance()), t; select = :x)
```

---

## Usage on multiple columns

To calculate different statistics on each column, OnlineStats offers the `Group` type.  
There are several methods for creating a `Group`.  

```
2Mean() == Group(Mean(), Mean())
[Mean() CountMap(Int)] == Group(Mean(), CountMap(Int))
```


```@repl ex1
reduce(2Mean(), t; select = (:x, :y))
```

### Different `OnlineStat`s on columns

To calculate different statistics on different columns, we need to make a `Group`, which can
be created via `hcat`.

```@repl ex1 
g = reduce([Mean() CountMap(Int)], t; select = (:x, :z))
```