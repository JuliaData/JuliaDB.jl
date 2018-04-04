import IndexedTables: selectkeys, selectvalues, select, lowerselection
import IndexedTables: convertdim
import Base: mapslices

export convertdim, selectkeys, selectvalues

function Base.map(f, t::DDataset; select=nothing)
    # TODO: fix when select has a user-supplied vector
    delayedmap(t.chunks) do x
        map(f, x; select=select)
    end |> fromchunks
end

function DataValues.dropna(t::DDataset, select=(colnames(t)...))
    delayedmap(t.chunks) do x
        dropna(x, select)
    end |> fromchunks
end

function Base.filter(f, t::DDataset; select=isa(f, Union{Tuple, Pair}) ? nothing : valuenames(t))
    mapchunks(t, keeplengths=false) do x
        filter(f, x; select=select)
    end |> cache_thunks
end

function selectkeys(x::DNDSparse, which; kwargs...)
    ndsparse(rows(keys(x), which), values(x); kwargs...)
end

function selectvalues(x::DNDSparse, which; kwargs...)
    ndsparse(keys(x), rows(values(x), which); kwargs...)
end

Base.@deprecate select(x::DNDSparse, conditions::Pair...) filter(conditions, x)
Base.@deprecate select(x::DNDSparse, which::DimName...; kwargs...) selectkeys(x, which; kwargs...)

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

    if agg !== nothing && has_overlaps(domains)
        overlap_merge(x, y) = merge(x, y, agg=agg)
        chunk_merge(ts...)  = _merge(overlap_merge, ts...)
        cache_thunks(rechunk(t1, merge=chunk_merge, closed=true))
    elseif vecagg != nothing
        groupby(vecagg, t1) # already cached
    else
        cache_thunks(t1)
    end
end

keyindex(t::DNDSparse, i::Int) = i
keyindex(t::DNDSparse{K}, i::Symbol) where {K} = findfirst(x->x===i, fieldnames(K))
