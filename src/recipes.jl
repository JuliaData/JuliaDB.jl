using RecipesBase

export partitionplot

#-----------------------------------------------------------------# partitionplot
@userplot struct PartitionPlot
    args
end

@recipe function f(o::PartitionPlot; nparts = 100, stat = nothing, by = nothing)
    t = o.args[1]
    sel_x = o.args[2] 
    stat = (stat == nothing) ? Extrema() : stat
    if length(o.args) == 3
        sel_y = o.args[3]
        T = fieldtype(eltype(t), IndexedTables.colindex(t, sel_x))
        if by == nothing 
            label --> OnlineStats.name(stat,false,false) * " of $sel_y"
            reduce(IndexedPartition(T, stat, nparts), t; select = (sel_x, sel_y))
        else 
            out = collect(groupreduce(IndexedPartition(T, stat, nparts), t, by; select = (sel_x, sel_y)))
            for i in 1:length(out)
                @series begin 
                    label --> OnlineStats.name(stat,false,false) * " of $(out[i][1])"
                    out[i][2]
                end
            end
        end
    elseif length(o.args) == 2 
        if by == nothing
            label --> OnlineStats.name(stat,false,false) * " of $sel_x"
            reduce(Partition(stat, nparts), t; select = sel_x)
        else 
            out = groupreduce(Partition(stat, nparts), t; select = sel_x)
            for i in 1:length(out)
                @series begin 
                    label --> OnlineStats.name(stat,false,false) * " of $(out[i][1])"
                    out[i][2]
                end
            end
        end
    end
end

"""
    partitionplot(table, y;    stat=Extrema(), nparts=100, by=nothing)
    partitionplot(table, x, y; stat=Extrema(), nparts=100, by=nothing)

Plot a summary of variable `y` against `x` (`1:length(y)` if not specified).  Using `nparts`
approximately-equal sections along the x-axis, the data in `y` over each section is 
summarized by `stat`. 

# Examples 

```@example plot
using JuliaDB, Plots, OnlineStats

x = randn(10^6)
y = x + randn(10^6)
z = x .> 0
t = table(@NT(x=x, y=y, z=z))

# x by itself
partitionplot(t, :x, stat = Extrema())

# y by x
partitionplot(t, :x, :y, stat = Hist(25))

# y by x, grouped by z
partitionplot(t, :x, :y, stat = Extrema(), by = z)
```
"""
partitionplot

