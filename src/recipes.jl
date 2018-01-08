using RecipesBase

# threshold to plot partitions rather than data
const PTHRESH = 10_000

#-----------------------------------------------------------------# table and selection
@recipe function f(t::AbstractIndexedTable, selection::Union{Symbol, Number, Pair}; 
                   partition = length(t) > PTHRESH, nparts = 50, reducer = nothing)
    if partition
        reducer = (reducer == nothing) ? make_reducer(t, selection) : reducer
        @series begin 
            title --> selection 
            reduce(Partition(reducer, nparts), t; select = selection)
        end
    else
        @series begin 
            title --> selection
            collect(select(t, selection))
        end
    end
end

function make_reducer(t::Union{NextTable, DNextTable}, selection)
    T = typeof(t[1][selection])
    make_reducer(T)
end

function make_reducer(t::Union{NDSparse, DNDSparse}, selection)
    error("Josh needs to figure out what to do here")
end

# Default OnlineStat based on data type
make_reducer(::Type{<:Number}) = Mean()
make_reducer(::Type{T})  where {T<:Union{AbstractString, Symbol}} = CountMap(T)