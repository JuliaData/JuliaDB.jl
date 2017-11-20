import IndexedTables: aggregate, aggregate_vec, reducedim_vec, pick
import IndexedTables: groupreduce, groupby
import Base: reducedim

export reducedim_vec, aggregate, aggregate_vec

function reduce(f, t::DDataset; select=valuenames(t))
    xs = delayedmap(t.chunks) do x
        f = isa(f, OnlineStat) ? copy(f) : f # required for > 1 chunks on the same proc
        reduce(f, x; select=select)
    end
    if f isa Tup
        g, _ = IndexedTables.init_funcs(f, false)
    elseif f isa OnlineStat
        g = Series(f)
    else
        g = f
    end
    h = (a,b)->_apply_merge(g,a,b)
    collect(treereduce(delayed(h), xs))
end

function reduce(f, t::DDataset, v0; select=nothing)
    xs = delayedmap(t.chunks) do x
        f = isa(f, OnlineStat) ? copy(f) : f
        reduce(f, x; select=select === nothing ? rows(x) : select)
    end
    g = isa(f, OnlineStat) ? merge : f
    merge(collect(treereduce(delayed(g), xs)), v0)
end

function groupreduce(f, t::DDataset, by=pkeynames(t); kwargs...)
    function groupchunk(x)
        groupreduce(f, x, by; kwargs...)
    end
    if f isa Tup
        g, _ = IndexedTables.init_funcs(f, false)
    elseif f isa OnlineStat
        g = Series(f)
    else
        g = f
    end
    h = (a,b)->_apply_merge(g,a,b)
    mergechunk(x, y) = groupreduce(h, _merge(x, y))

    t1 = fromchunks(delayedmap(groupchunk, t.chunks))
    with_overlaps(t1, true) do cs
        treereduce(delayed(mergechunk), cs)
    end
end

function groupby(f, t::DDataset, by=pkeynames(t); select=excludecols(t, by), kwargs...)
    if by != pkeynames(t) || has_overlaps(t.domains, closed=true)
        t = rechunk(t, by, select)
    end

    function groupchunk(x)
        groupby(f, x, by; select=select, kwargs...)
    end

    fromchunks(delayedmap(groupchunk, t.chunks))
end

function reducedim(f, x::DNDSparse, dims)
    keep = setdiff([1:ndims(x);], dims) # TODO: Allow symbols
    if isempty(keep)
        throw(ArgumentError("to remove all dimensions, use `reduce(f, A)`"))
    end
    cache_thunks(selectkeys(x, (keep...), agg=f))
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

    t = selectkeys(x, (keep...); agg=nothing)
    cache_thunks(groupby(f, t))
end

reducedim_vec(f, x::DNDSparse, dims::Symbol) = reducedim_vec(f, x, [dims])
Base.@deprecate aggregate_stats(s, t; by=pkeynames(t), with=valuenames(t)) groupreduce(s, t, by; select=with)

OnlineStats.Series(x::DDataset, stat; select=valuenames(x)) =
    reduce(stat, x, select=select)
