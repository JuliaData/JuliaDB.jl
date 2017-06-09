
"""
    t[idx...]

Returns a `DTable` containing only the elements of `t` where the given indices (`idx`)
match. If `idx` has the same type as the index tuple of the `t`, then this is
considered a scalar indexing (indexing of a single value). In this case the value
itself is looked up and returned.

"""
Base.getindex(t::DTable, idxs...) = _getindex(t, idxs)

function _getindex{K,V}(t::DTable{K,V}, idxs::K)
    # scalar getindex
    brects = boundingrect.(t.subdomains)
    subchunk_idxs = find(c->all(map(in, idxs, map(Interval, c.first, c.last))), brects)
    t1 = DTable{K,V}(t.subdomains[subchunk_idxs], t.chunks[subchunk_idxs])
    collect(t1)[idxs...]
end

function _getindex{K,V}(t::DTable{K,V}, idxs)
    if length(idxs) != ndims(t)
        error("wrong number of indices")
    end
    for idx in idxs
        isa(idx, AbstractVector) && (issorted(idx) || error("indices must be sorted for ranged/vector indexing"))
    end

    # Subset the chunks
    # this is currently a linear search

    brects = boundingrect.(t.subdomains)
    subchunk_idxs = find(c->all(map(in, idxs, map(Interval, c.first, c.last))), brects)
    t = DTable{K,V}(t.subdomains[subchunk_idxs], t.chunks[subchunk_idxs])

    mapchunks(t, keeplengths=false) do chunk
        getindex(chunk, idxs...)
    end |> cache_thunks
end

