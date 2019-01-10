#-----------------------------------------------------------------# partitionplot
@userplot struct PartitionPlot
    args
end


@recipe function f(o::PartitionPlot; nparts=100, stat=Extrema(), by=nothing, dropmissing=false)
    t = o.args[1]
    sel = map(x -> lowerselection(t, x), o.args[2:end])
    o = if length(sel) == 1 
        Partition(stat, nparts) 
    else
        T = typeof(collect(rows(t)[1])[sel[1]])
        IndexedPartition(T, stat, nparts)
    end
    s = FTSeries(o; filter = dropmissing ? !_ismissing : x -> true)
    if by === nothing 
        reduce(s, t; select=sel)
    else
        grp = groupreduce(s, t, by; select=sel)
        grp = isa(t, DDataset) ? collect(grp) : grp 
        for row in rows(grp)
            @series begin row[2] end
        end
    end
end

"""
    partitionplot(table, y;    stat=Extrema(), nparts=100, by=nothing, dropmissing=false)
    partitionplot(table, x, y; stat=Extrema(), nparts=100, by=nothing, dropmissing=false)

Plot a summary of variable `y` against `x` (`1:length(y)` if not specified).  Using `nparts`
approximately-equal sections along the x-axis, the data in `y` over each section is 
summarized by `stat`. 
"""
partitionplot

