using RecipesBase

export partitionplot

#-----------------------------------------------------------------# partitionplot
@userplot struct PartitionPlot
    args
end

getvalue(x) = x 
getvalue(x::DataValues.DataValue) = get(x)

@recipe function f(o::PartitionPlot; nparts = 100, stat = nothing, by = nothing, dropmissing = false)
    t = o.args[1]
    sel_x = o.args[2] 
    stat = (stat == nothing) ? Extrema() : stat
    if length(o.args) == 3
        sel_y = o.args[3]
        T = fieldtype(eltype(t), IndexedTables.colindex(t, sel_x))
        s = dropmissing ? 
            series(IndexedPartition(T, stat, nparts)) :
            series(IndexedPartition(T, stat, nparts); filter = !isnull, transform = getvalue)
        if by == nothing 
            label --> OnlineStats.name(stat,false,false) * " of $sel_y"
            reduce(s, t; select = (sel_x, sel_y))
        else 
            out = collect(groupreduce(s, t, by; select = (sel_x, sel_y)))
            for i in 1:length(out)
                @series begin 
                    label --> OnlineStats.name(stat,false,false) * " of $(out[i][1])"
                    out[i][2]
                end
            end
        end
    elseif length(o.args) == 2 
        s = dropmissing ? 
            series(Partition(stat, nparts)) : 
            series(Partition(stat, nparts); filter = !isnull, transform = getvalue)
        if by == nothing
            label --> OnlineStats.name(stat,false,false) * " of $sel_x"
            reduce(s, t; select = sel_x)
        else 
            out = groupreduce(s, t, by; select = sel_x)
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
"""
partitionplot

