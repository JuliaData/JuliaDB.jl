export select, convertdim, aggregate, reducedim_vec, aggregate_vec

import IndexedTables: convertdim, aggregate, aggregate_vec, reducedim_vec, pick
import Base: reducedim, mapslices

# re-export
export pick

"""
    aggregate(f, t::DNDSparse)

Combines adjacent rows with equal indices using the given
2-argument reduction function `f`.
"""
function aggregate(f, t::DNDSparse; kwargs...)
    t1 = mapchunks(c->aggregate(f, c; kwargs...), t, keeplengths=false)
    if has_overlaps(t1.domains, true)
        overlap_merge = (x, y) -> merge(x, y, agg=f)
        t2 = rechunk(t1, merge=(ts...) -> _merge(overlap_merge, ts...), closed=true)
        cache_thunks(t2)
    else
        cache_thunks(t1)
    end
end

"""
    aggregate_vec(f::Function, x::DNDSparse)

Combine adjacent rows with equal indices using a function from vector to scalar,
e.g. `mean`.
"""
function aggregate_vec(f, t::DNDSparse)
    if has_overlaps(t.domains, true)
        t = rechunk(t, closed=true) # Should not have chunks that are continuations
    end
    mapchunks(c->aggregate_vec(f, c), t, keeplengths=false) |> cache_thunks
end

# Filter on data field
"""
    filter(f, t::DNDSparse)

Filters `t` removing rows for which `f` is false. `f` is passed only the data
and not the index.
"""
function Base.filter(f, t::DNDSparse)
    cache_thunks(mapchunks(x -> filter(f, x), t, keeplengths=false))
end

"""
    convertdim(x::DNDSparse, d::DimName, xlate; agg::Function, name)

Apply function or dictionary `xlate` to each index in the specified dimension.
If the mapping is many-to-one, `agg` is used to aggregate the results.
`name` optionally specifies a name for the new dimension. `xlate` must be a
monotonically increasing function.

See also [`reducedim`](@ref) and [`aggregate`](@ref)
"""
function convertdim(t::DNDSparse{K,V}, d::DimName, xlat;
                    agg=nothing, vecagg=nothing, name=nothing) where {K,V}

    if isa(d, Symbol)
        dn = findfirst(dimlabels(t), d)
        if dn == 0
            throw(ArgumentError("table has no dimension \"$d\""))
        end
        d = dn
    end

    chunkf(c) = convertdim(c, d, xlat; agg=agg, vecagg=nothing, name=name)
    chunks = map(delayed(chunkf), t.chunks)

    xlatdim(intv, d) = Interval(tuplesetindex(first(intv), xlat(first(intv)[d]), d),
                                tuplesetindex(last(intv),  xlat(last(intv)[d]), d))

    # TODO: handle name kwarg
    # apply xlat to bounding rectangles
    domains = map(t.domains) do space
        nrows = agg === nothing ? space.nrows : Nullable{Int}()
        IndexSpace(xlatdim(space.interval, d), xlatdim(space.boundingrect, d), nrows)
    end

    t1 = DNDSparse{eltype(domains[1]),V}(domains, chunks)

    if agg !== nothing && has_overlaps(domains, true)
        overlap_merge(x, y) = merge(x, y, agg=agg)
        chunk_merge(ts...)  = _merge(overlap_merge, ts...)
        cache_thunks(rechunk(t1, merge=chunk_merge, closed=true))
    elseif vecagg != nothing
        aggregate_vec(vecagg, t1) # already cached
    else
        cache_thunks(t1)
    end
end

"""
    reducedim(f, t::DNDSparse, dims)

Remove `dims` dimensions from `t`, aggregate any rows with equal indices
using 2-argument function `f`.

See also [`reducedim_vec`](@ref), [`select`](@ref) and [`aggregate`](@ref).
"""
function reducedim(f, x::DNDSparse, dims)
    keep = setdiff([1:ndims(x);], dims) # TODO: Allow symbols
    if isempty(keep)
        throw(ArgumentError("to remove all dimensions, use `reduce(f, A)`"))
    end
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

keyindex(t::DNDSparse, i::Int) = i
keyindex(t::DNDSparse{K}, i::Symbol) where {K} = findfirst(x->x===i, fieldnames(K))

function mapslices(f, x::DNDSparse, dims; name=nothing)
    iterdims = setdiff([1:ndims(x);], map(d->keyindex(x, d), dims))
    if iterdims != [1:length(iterdims);]
        throw(ArgumentError("$dims must be the trailing dimensions of the table. You can use `permutedims` first to permute the dimensions."))
    end

    t = has_overlaps(x.domains, iterdims) ?
        rechunk(x, closed=true, by=(iterdims...)) : x

    cache_thunks(mapchunks(y -> mapslices(f, y, dims, name=name),
                           t, keeplengths=false))
end

mapslices(f, x::DNDSparse, dims::Symbol; name=nothing) =
    mapslices(f, x, (dims,); name=name)
