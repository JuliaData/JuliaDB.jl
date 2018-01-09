# Plotting

JuliaDB has all access to all the power and flexibility of [Plots](https://github.com/JuliaPlots/Plots.jl)
via [StatPlots](https://github.com/JuliaPlots/StatPlots.jl) and the `@df` macro.

```
using JuliaDB, StatPlots

t = table(@NT(x = randn(100), y = randn(100)))

@df t scatter(:x, :y)
```

For handling huge data, JuliaDB also adds `partitionplot` for plotting summaries of data 
columns, optionally grouped by another column.

```
using JuliaDB, Plots, OnlineStats

t = table(@NT(x = randn(10^6), y = rand(Bool, 10^6)))

partitionplot(t, :x)

partitionplot(t, :x, by = :y, nparts = 200, stat = Extrema())
```