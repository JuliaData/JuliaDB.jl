
"""
    t[idx...]

Returns a `DNDSparse` containing only the elements of `t` where the given indices (`idx`)
match. If `idx` has the same type as the index tuple of the `t`, then this is
considered a scalar indexing (indexing of a single value). In this case the value
itself is looked up and returned.

"""
function Base.getindex(t::DNDSparse{K}, idxs...) where K
    if typeof(idxs) <: astuple(K)
        _getindex_scalar(t, idxs)
    else
        _getindex(t, idxs)
    end
end

function _getindex_scalar(t::DNDSparse{K,V}, idxs) where {K,V}
    # scalar getindex
    rng = map(x->[x], idxs)
    t = collect(fromchunks(delayedmap(x->x[rng...], t.chunks)))
    if isempty(t)
        throw(KeyError(idxs))
    else
        first(t)
    end
end

function _getindex(t::DNDSparse{K,V}, idxs) where {K,V}
    if length(idxs) != ndims(t)
        error("wrong number of indices")
    end
    for idx in idxs
        isa(idx, AbstractVector) && (issorted(idx) || error("indices must be sorted for ranged/vector indexing"))
    end

    # Subset the chunks
    # this is currently a linear search

    mapchunks(t, keeplengths=false) do chunk
        getindex(chunk, idxs...)
    end |> cache_thunks
end

# update a given domain to include a new key
function update_domain(d::IndexSpace{<:NamedTuple}, key::Tuple)
    knt = namedtuple(keys(first(d))...)(key)
    IndexSpace(Interval(min(first(d.interval), knt),
                        max(last(d.interval), knt)),
               Interval(map(min, first(d.interval), knt),
                        map(max, last(d.interval), knt)),
               Nullable{Int}())
end

function update_domain(d::IndexSpace{<:Tuple}, key::Tuple)
    IndexSpace(Interval(min(first(d.interval), key),
                        max(last(d.interval), key)),
               Interval(map(min, first(d.interval), key),
                        map(max, last(d.interval), key)),
               Nullable{Int}())
end

function insert_row!(x::DNDSparse{K,T}, idxs::Tuple, val) where {K,T}
    perm = sortperm(x.domains, by=last)
    cs = convert(Array{Any}, x.chunks[perm])
    ds = x.domains[perm]
    i = searchsortedfirst(astuple.(last.(x.domains)), idxs)
    if i >= length(cs)
        i = length(cs)
    end

    ds[i] = update_domain(ds[i], idxs)
    tmp = cs[i]
    cs[i] = compute(get_context(), delayed(x->(x[idxs...] = val; x))(cs[i]))
    x.domains = ds
    x.chunks = cs
end

function insert_row!(x::DNDSparse{K,T}, idxs::NamedTuple, val) where {K,T}
    insert_row!(s, astuple(idxs), val)
end

function Base.setindex!(x::DNDSparse, val, idxs...)
    insert_row!(x, idxs, val)
end
