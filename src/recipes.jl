using RecipesBase

export partitionplot

#-----------------------------------------------------------------------# PartitionPlot
@userplot struct PartitionPlot args end

selection_type(t, sel) = error("Not sure how to infer type of selection")
selection_type(t, sel::Void) = Int

# NextTable/NDSparse
selection_type(t::Dataset, sel::Pair) = typeof(sel[2].(rows(t, sel[1])[1]))
selection_type(t::Dataset, sel::Union{Symbol,Int}) = typeof(rows(t, sel)[1])

# DNextTable/DNDSparse
function selection_type(t::DDataset, sel::Pair)
    typeof(sel[2].(collect(rows(t, sel[1])[1])))
end
function selection_type(t::DDataset, sel::Union{Symbol, Int})
    typeof(collect(rows(t, sel[1])[1]))
end

 
function get_args(o)
    t = o.args[1]
    selx = length(o.args) == 3 ? o.args[2] : nothing
    sely = length(o.args) == 3 ? o.args[3] : o.args[2]
    TX = selection_type(t, selx)
    TY = selection_type(t, sely)
    t, selx, sely, TX, TY
end

@recipe function f(o::PartitionPlot; nparts=100, stat=nothing, by=nothing, filter=x->true)
    t, x, y, TX, TY = get_args(o)
    st = stat == nothing ? Extrema(TY) : stat
    if x == nothing 
        ft = FTSeries(Partition(st, nparts), filter=filter)
        if by == nothing 
            reduce(ft, t, select=y)
        else 
            out = collect(rows(groupreduce(ft, t, by, select=y)))
            for i in 1:length(out)
                @series begin 
                    label --> "Group: $(out[i][1])"
                    out[i][2]
                end
            end
        end
    else 
        ft = FTSeries(IndexedPartition(TX, st, nparts), filter=filter)
        if by == nothing 
            reduce(ft, t, select = (x,y))
        else
            out = collect(rows(groupreduce(ft, t, by, select=(x,y))))
            for i in 1:length(out)
                @series begin 
                    label --> "Group: $(out[i][1])"
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
