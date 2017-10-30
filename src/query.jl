export select, convertdim, aggregate, reducedim_vec, aggregate_vec

import IndexedTables: convertdim, aggregate, aggregate_vec, reducedim_vec, pick
import Base: reducedim, mapslices

# re-export
export pick

"""
    select(t::DTable, conditions::Pair...)

Filter based on index columns. Conditions are accepted as column-function pairs.

Example: `select(t, 1 => x->x>10, 3 => x->x!=10 ...)`
"""
function Base.select(t::DTable, conditions::Pair...)
    cache_thunks(mapchunks(x -> select(x, conditions...), t, keeplengths=false))
end

"""
    select(t::DTable, which...; agg)

Returns a new DTable where only a subset of the index columns (specified by `which`) are kept.

The `agg` keyword argument is a function which specifies how entries with
equal indices should be aggregated. If `agg` is unspecified, then the repeating
indices are kept in the output, you can then aggregate using [`aggregate`](@ref)
"""
function Base.select(t::DTable{K,V}, which::DimName...; agg=nothing) where {K,V}

    # remove dimensions from bounding boxes
    sub_dims = [which...]
    subinterval(intv, idx) = Interval(first(intv)[idx], last(intv)[idx])
    subdomains = map(t.subdomains) do idxspace
        # We replace the interval with the sub-bounding box as a conservative
        # estimate of the interval...
        # `subinterval(idxspace.interval, sub_dims)` would be wrong
        subbox = subinterval(idxspace.boundingrect, sub_dims)
        IndexSpace(subbox, subbox, Nullable{Int}())
    end

    chunks = map(delayed(x -> select(x, which...; agg=agg)), t.chunks)

    t1 = DTable{eltype(subdomains[1]), V}(subdomains, chunks)

    if agg !== nothing && has_overlaps(subdomains, true)
        with_overlaps(t1) do cs
            treereduce(delayed((x,y) -> merge(x, y, agg=agg)), cs)
        end
    else
        t1
    end |> cache_thunks
end

"""
    aggregate(f, t::DTable)

Combines adjacent rows with equal indices using the given
2-argument reduction function `f`.
"""
function aggregate(f, t::DTable; kwargs...)
    t1 = mapchunks(c->aggregate(f, c; kwargs...), t, keeplengths=false)
    if has_overlaps(t1.subdomains, true)
        overlap_merge = (x, y) -> merge(x, y, agg=f)
        t2 = rechunk(t1, merge=(ts...) -> _merge(overlap_merge, ts...), closed=true)
        cache_thunks(t2)
    else
        cache_thunks(t1)
    end
end

"""
    aggregate_vec(f::Function, x::DTable)

Combine adjacent rows with equal indices using a function from vector to scalar,
e.g. `mean`.
"""
function aggregate_vec(f, t::DTable)
    if has_overlaps(t.subdomains, true)
        t = rechunk(t, closed=true) # Should not have chunks that are continuations
    end
    mapchunks(c->aggregate_vec(f, c), t, keeplengths=false) |> cache_thunks
end

# Filter on data field
"""
    filter(f, t::DTable)

Filters `t` removing rows for which `f` is false. `f` is passed only the data
and not the index.
"""
function Base.filter(f, t::DTable)
    cache_thunks(mapchunks(x -> filter(f, x), t, keeplengths=false))
end

"""
    convertdim(x::DTable, d::DimName, xlate; agg::Function, name)

Apply function or dictionary `xlate` to each index in the specified dimension.
If the mapping is many-to-one, `agg` is used to aggregate the results.
`name` optionally specifies a name for the new dimension. `xlate` must be a
monotonically increasing function.

See also [`reducedim`](@ref) and [`aggregate`](@ref)
"""
function convertdim(t::DTable{K,V}, d::DimName, xlat;
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
    subdomains = map(t.subdomains) do space
        nrows = agg === nothing ? space.nrows : Nullable{Int}()
        IndexSpace(xlatdim(space.interval, d), xlatdim(space.boundingrect, d), nrows)
    end

    t1 = DTable{eltype(subdomains[1]),V}(subdomains, chunks)

    if agg !== nothing && has_overlaps(subdomains, true)
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
    reducedim(f, t::DTable, dims)

Remove `dims` dimensions from `t`, aggregate any rows with equal indices
using 2-argument function `f`.

See also [`reducedim_vec`](@ref), [`select`](@ref) and [`aggregate`](@ref).
"""
function reducedim(f, x::DTable, dims)
    keep = setdiff([1:ndims(x);], dims) # TODO: Allow symbols
    if isempty(keep)
        throw(ArgumentError("to remove all dimensions, use `reduce(f, A)`"))
    end
    cache_thunks(select(x, keep..., agg=f))
end

reducedim(f, x::DTable, dims::Symbol) = reducedim(f, x, [dims])

"""
    reducedim_vec(f::Function, t::DTable, dims)

Like `reducedim`, except uses a function mapping a vector of values to a scalar instead of a 2-argument scalar function.

See also [`reducedim`](@ref) and [`aggregate_vec`](@ref).
"""
function reducedim_vec(f, x::DTable, dims)
    keep = setdiff([1:ndims(x);], dims)
    if isempty(keep)
        throw(ArgumentError("to remove all dimensions, use `reduce(f, A)`"))
    end

    t = select(x, keep...; agg=nothing)
    cache_thunks(aggregate_vec(f, t))
end

reducedim_vec(f, x::DTable, dims::Symbol) = reducedim_vec(f, x, [dims])

keyindex(t::DTable, i::Int) = i
keyindex(t::DTable{K}, i::Symbol) where {K} = findfirst(x->x===i, fieldnames(K))

function mapslices(f, x::DTable, dims; name=nothing)
    iterdims = setdiff([1:ndims(x);], map(d->keyindex(x, d), dims))
    if iterdims != [1:length(iterdims);]
        throw(ArgumentError("$dims must be the trailing dimensions of the table. You can use `permutedims` first to permute the dimensions."))
    end

    t = has_overlaps(x.subdomains, iterdims) ?
        rechunk(x, closed=true, by=iterdims) : x

    cache_thunks(mapchunks(y -> mapslices(f, y, dims, name=name),
                           t, keeplengths=false))
end

mapslices(f, x::DTable, dims::Symbol; name=nothing) =
    mapslices(f, x, (dims,); name=name)
