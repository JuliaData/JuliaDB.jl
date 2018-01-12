# Plotting

JuliaDB has all access to all the power and flexibility of [Plots](https://github.com/JuliaPlots/Plots.jl)
via [StatPlots](https://github.com/JuliaPlots/StatPlots.jl) and the `@df` macro.

```
using JuliaDB, StatPlots

t = table(@NT(x = randn(100), y = randn(100)))

@df t scatter(:x, :y)
```

## `partitionplot`

```@docs
partitionplot
```