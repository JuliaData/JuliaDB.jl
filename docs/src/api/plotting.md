# Plotting

JuliaDB implements recipes for plotting data columns with [Plots.jl](https://github.com/JuliaPlots/Plots.jl).

```
using JuliaDB

t = table(@NT(x = randn(100)))

t2 = table(@NT(x = randn(1_000_000)))
```

- If the data is "small" (< 10,000 rows), plotting a table will plot each observation.  
- If the data is "large", plotting a table will plot the data summarized by an `OnlineStats.Partition`.  

```
using Plots, OnlineStats

# Small table: All observations plotted
plot(t, :x) 

# Big table: Plot sections of data summarized by max/min
plot(t2, :x; reducer = Extrema(), nparts = 75)
```