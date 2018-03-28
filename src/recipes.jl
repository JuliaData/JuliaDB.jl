using RecipesBase

export partitionplot

#-----------------------------------------------------------------# partitionplot
@userplot struct PartitionPlot
    args
end

getvalue(x) = x 
getvalue(x::DataValues.DataValue) = get(x)

indextype(x::Type) = x 
indextype(x::Type{DataValues.DataValue{T}}) where {T} = T

@recipe function f(o::PartitionPlot; nparts = 100, stat = nothing, by = nothing, dropmissing = false)
    t = o.args[1]
    sel_x = o.args[2] 
    stat = (stat == nothing) ? Extrema() : stat
    if length(o.args) == 3
        sel_y = o.args[3]
        T = indextype.(fieldtype(eltype(t), IndexedTables.colindex(t, sel_x)))
        s = dropmissing ? 
            FTSeries(IndexedPartition(T, stat, nparts); filter = x->all(!isnull, x), transform = x->getvalue.(x)) :
            FTSeries(IndexedPartition(T, stat, nparts))
        if by == nothing 
            reduce(s, t; select = (sel_x, sel_y))
        else 
            out = collect(groupreduce(s, t, by; select = (sel_x, sel_y)))
            for i in 1:length(out)
                @series begin 
                    label --> OnlineStats.name(stat,false,false) * " of $sel_x ($(out[i][1]))"
                    out[i][2]
                end
            end
        end
    elseif length(o.args) == 2 
        s = dropmissing ? 
            FTSeries(Partition(stat, nparts); filter = !isnull, transform = getvalue) :
            FTSeries(Partition(stat, nparts))
        if by == nothing
            reduce(s, t; select = sel_x)
        else 
            out = groupreduce(s, t, by; select = sel_x)
            for i in 1:length(out)
                @series begin 
                    label --> OnlineStats.name(stat,false,false) * " of $sel_x ($(out[i][1]))"
                    out[i][2]
                end
            end
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

