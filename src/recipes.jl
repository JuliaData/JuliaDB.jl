using RecipesBase

#-----------------------------------------------------------------# partitionplot
@userplot struct PartitionPlot
    args
end

@recipe function f(tp::PartitionPlot; nparts = 50, stat = nothing, by = nothing)
    t = tp.args[1]
    selection = tp.args[2]
    o = (stat == nothing) ? 
            make_stat(fieldtype(eltype(t), IndexedTables.colindex(t, selection))) : 
            stat 
    if by == nothing 
        @series begin 
            title --> selection
            reduce(Partition(o, nparts), t; select = selection)
        end
    else
        out = collect(groupreduce(Partition(o, nparts), t, by; select = selection))
        layout := length(out)
        for i in 1:length(out)
            @series begin 
                title --> string(selection) * " ($(out[i][1]))"
                subplot --> i
                out[i][2]
            end
        end
    end
end

# Default OnlineStat based on data type
make_stat(::Type{<:Number}) = Mean()
make_stat(::Type{T})  where {T<:Union{AbstractString, Symbol, Bool}} = CountMap(T)

make_stat(T) = error("No predefined stat for $T")