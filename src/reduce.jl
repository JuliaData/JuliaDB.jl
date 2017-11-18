import IndexedTables: aggregate, aggregate_vec, reducedim_vec, pick
import Base: reducedim

function reducedim(f, x::DNDSparse, dims)
    keep = setdiff([1:ndims(x);], dims) # TODO: Allow symbols
    if isempty(keep)
        throw(ArgumentError("to remove all dimensions, use `reduce(f, A)`"))
    end
    @show x, keep, f
    cache_thunks(select(x, keep..., agg=f))
end

reducedim(f, x::DNDSparse, dims::Symbol) = reducedim(f, x, [dims])

"""
    reducedim_vec(f::Function, t::DNDSparse, dims)

Like `reducedim`, except uses a function mapping a vector of values to a scalar instead of a 2-argument scalar function.

See also [`reducedim`](@ref) and [`aggregate_vec`](@ref).
"""
function reducedim_vec(f, x::DNDSparse, dims)
    keep = setdiff([1:ndims(x);], dims)
    if isempty(keep)
        throw(ArgumentError("to remove all dimensions, use `reduce(f, A)`"))
    end

    t = select(x, keep...; agg=nothing)
    cache_thunks(aggregate_vec(f, t))
end

reducedim_vec(f, x::DNDSparse, dims::Symbol) = reducedim_vec(f, x, [dims])
