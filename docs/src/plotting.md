# Plotting

```@setup plot
using Pkg, Random
Pkg.add("StatPlots")
Pkg.add("GR")
using StatPlots
ENV["GKSwstype"] = "100"
gr()
Random.seed!(1234)  # set random seed to get consistent plots
```

## StatPlots

JuliaDB has all access to all the power and flexibility of [Plots](https://github.com/JuliaPlots/Plots.jl)
via [StatPlots](https://github.com/JuliaPlots/StatPlots.jl) and the `@df` macro.

```@example plot
using JuliaDB, StatPlots

t = table((x = randn(100), y = randn(100)))

@df t scatter(:x, :y)
savefig("statplot.png"); nothing # hide
```
![](statplot.png)

## Plotting Big Data

For large datasets, it isn't feasible to render every data point.  The OnlineStats package provides a number of [data structures for big data visualization](http://joshday.github.io/OnlineStats.jl/latest/visualizations.html) that can be created via the [`reduce`](@ref) and [`groupreduce`](@ref) functions.

