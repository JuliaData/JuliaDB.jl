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

## `partitionplot`

```@docs
partitionplot
```

### Examples 

```@example plot
using JuliaDB, Plots, OnlineStats

x = randn(10^6)
y = x + randn(10^6)
z = x .> 0
t = table((x=x, y=y, z=z))

# x by itself
partitionplot(t, :x, stat = Extrema())
savefig("plot1.png"); nothing # hide
```
![](plot1.png)


```@example plot
# y by x
partitionplot(t, :x, :y, stat = Hist(25))
savefig("plot2.png"); nothing # hide
```
![](plot2.png)

```@example plot
# y by x, grouped by z
partitionplot(t, :x, :y, stat = Extrema(), by = z)
savefig("plot3.png"); nothing # hide
```
![](plot3.png)