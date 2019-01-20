```@setup onlinestats
using OnlineStats
```

# OnlineStats Integration

[OnlineStats](https://github.com/joshday/OnlineStats.jl) is a package for calculating statistics and models with online (one observation at a time) parallelizable algorithms. This integrates tightly with JuliaDB's distributed data structures to calculate statistics on large datasets.  The full documentation for OnlineStats [is available here](https://joshday.github.io/OnlineStats.jl/latest/).

## Basics

OnlineStats' objects can be updated with more data and also merged together.  The image below demonstrates what goes on under the hood in JuliaDB to compute a statistic `s` in parallel.


```@raw html
<img src="https://user-images.githubusercontent.com/8075494/32748459-519986e8-c88a-11e7-89b3-80dedf7f261b.png" width=400>
```


OnlineStats integration is available via the [`reduce`](@ref) and [`groupreduce`](@ref) functions.  An OnlineStat acts differently from a normal reducer:

- Normal reducer `f`:  `val = f(val, row)`
- OnlineStat reducer `o`: `fit!(o, row)`

```@repl onlinestats
using JuliaDB, OnlineStats
t = table(1:100, rand(Bool, 100), randn(100));
reduce(Mean(), t; select = 3)
grp = groupreduce(Mean(), t, 2; select=3)
select(grp, (1, 2 => value))
```

!!! note
    The `OnlineStats.value` function extracts the value of the statistic.  E.g. `value(Mean())`.

### Calculating Statistics on Multiple Columns.

The `OnlineStats.Group` type is used for calculating statistics on multiple data streams.  A `Group` that computes the same `OnlineStat` can be created through integer multiplication:

```@repl onlinestats
reduce(3Mean(), t)
```

Alternatively, a `Group` can be created by providing a collection of `OnlineStat`s.

```@repl onlinestats
reduce(Group(Extrema(Int), CountMap(Bool), Mean()), t)
```