using RecipesBase

export partitionplot

#-----------------------------------------------------------------------# PartitionPlot
@userplot struct PartitionPlot args end
 

function get_args(o)
    t = o.args[1]
    selx = length(o.args) == 3 ? o.args[2] : nothing
    sely = length(o.args) == 3 ? o.args[3] : o.args[2]
    # if sel is a pair, apply the function sel[2] to sel[1]
    example_x = selx isa Pair ? 
        selx[2].(collect(rows(t, selx[1]))) :
        (selx == nothing ? [1] : collect(rows(t, selx)[1]))
    example_y = sely isa Pair ? 
        sely[2].(collect(rows(t, sely[1]))) :
        collect(rows(t, sely)[1])
    # use eltype to support both Dataset and DDataset (since collect is used above)
    TX = eltype(example_x)  # type of X that OnlineStats will see
    TY = eltype(example_y)  # type of Y that OnlineStats will see
    # default stat is Extrema
    stat = Extrema(TY)
    t, selx, sely, stat, TX, TY
end

@recipe function f(o::PartitionPlot; nparts=100, stat=nothing, by=nothing, filter=x->true)
    t, x, y, st, TX, TY = get_args(o)
    if stat != nothing 
        st = stat 
    end
    if x == nothing 
        ft = FTSeries(Partition(st, nparts), filter=filter)
        if by == nothing 
            reduce(ft, t, select=y)
        else 
            out = collect(table(groupreduce(ft, t, by, select=y)))
            for i in 1:length(out)
                @series begin 
                    label --> "Group: $(out[i][1])"
                    out[i][2]
                end
            end
        end
    else 
        ft = FTSeries(IndexedPartition(TX, st, nparts))
        if by == nothing 
            reduce(ft, t, select = (x,y))
        else
            out = collect(table(groupreduce(ft, t, by, select=(x,y))))
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



# export partitionplot, partplot 

# #-----------------------------------------------------------------# partitionplot
# @userplot struct PartitionPlot
#     args
# end

# getvalue(x) = x 
# getvalue(x::DataValues.DataValue) = get(x)

# indextype(x::Type) = x 
# indextype(x::Type{DataValues.DataValue{T}}) where {T} = T

# @recipe function f(o::PartitionPlot; nparts = 100, stat = Extrema(), by = nothing, dropmissing = false)
#     t = o.args[1]
#     sel_x = o.args[2] 
#     if length(o.args) == 3
#         sel_y = o.args[3]
#         T = eltype.(columns(t))[sel_x]
#         s = dropmissing ? 
#             FTSeries(IndexedPartition(T, stat, nparts); filter = x->all(!isnull, x), transform = x->getvalue.(x)) :
#             IndexedPartition(T, stat, nparts)
#         if by == nothing 
#             reduce(s, t; select = (sel_x, sel_y))
#         else 
#             out = collect(groupreduce(s, t, by; select = (sel_x, sel_y)))
#             for i in 1:length(out)
#                 @series begin 
#                     label --> OnlineStats.name(stat,false,false) * " of $sel_x ($(out[i][1]))"
#                     out[i][2]
#                 end
#             end
#         end
#     elseif length(o.args) == 2 
#         s = dropmissing ? 
#             FTSeries(Partition(stat, nparts); filter = !isnull, transform = getvalue) :
#             Partition(stat, nparts)
#         if by == nothing
#             reduce(s, t; select = sel_x)
#         else 
#             out = groupreduce(s, t, by; select = sel_x)
#             for i in 1:length(out)
#                 @series begin 
#                     label --> OnlineStats.name(stat,false,false) * " of $sel_x ($(out[i][1]))"
#                     out[i][2]
#                 end
#             end
#         end
#     end
# end

# """
#     partitionplot(table, y;    stat=Extrema(), nparts=100, by=nothing, dropmissing=false)
#     partitionplot(table, x, y; stat=Extrema(), nparts=100, by=nothing, dropmissing=false)

# Plot a summary of variable `y` against `x` (`1:length(y)` if not specified).  Using `nparts`
# approximately-equal sections along the x-axis, the data in `y` over each section is 
# summarized by `stat`. 
# """
# partitionplot

