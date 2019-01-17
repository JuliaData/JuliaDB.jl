# Plotting

```@setup plot
Pkg.add("StatPlots")
Pkg.add("GR")
using StatPlots
ENV["GKSwstype"] = "100"
gr()
srand(1234)  # set random seed to get consistent plots
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

- Example data:
  
```@example plot
using JuliaDB, Plots, OnlineStats

x = randn(10^6)
y = x + randn(10^6)
z = x .> 1
z2 = (x .+ y) .> 0
t = table((x=x, y=y, z=z, z2=z2))
```

### Mosaic Plots

A [mosaic plot](https://en.wikipedia.org/wiki/Mosaic_plot) visualizes the bivariate distribution of two categorical variables.  

```@example plot
o = reduce(Mosaic(Bool, Bool), t; select = (3, 4))
plot(o)
png("mosaic.png"); nothing  # hide
```
![](mosaic.png)

### Histograms

```@example plot
grp = groupreduce(Hist(-5.1:5), t, :z, select = :x)
plot(plot.(select(grp, 2))...)
```

### Convenience function for Partition and IndexedPartition

You can also use the [`partitionplot`](@ref) function, a slightly less verbose way of plotting `Partition` and `IndexedPartition` objects.

```@example plot
# x by itself
partitionplot(t, :x, stat = Extrema())
savefig("partitionplot1.png"); nothing # hide
```
![](partitionplot1.png)


```@example plot
# y by x, grouped by z
partitionplot(t, :x, :y, stat = Extrema(), by = :z)
savefig("partitionplot2.png"); nothing # hide
```
![](partitionplot2.png)