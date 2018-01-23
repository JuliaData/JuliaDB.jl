# Plotting

```@setup plot
Pkg.add("StatPlots")
using StatPlots
```

JuliaDB has all access to all the power and flexibility of [Plots](https://github.com/JuliaPlots/Plots.jl)
via [StatPlots](https://github.com/JuliaPlots/StatPlots.jl) and the `@df` macro.

```@example plot
using JuliaDB, StatPlots

t = table(@NT(x = randn(100), y = randn(100)))

@df t scatter(:x, :y)
savefig("statplot.png"); nothing # hide
```
![](statplot.png)

## `partitionplot`

```@docs
partitionplot
```