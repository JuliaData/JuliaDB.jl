
"""
    t[idx...]

Returns a `DTable` containing only the elements of `t` where the given indices (`idx`)
match. If `idx` has the same type as the index tuple of the `t`, then this is
considered a scalar indexing (indexing of a single value). In this case the value
itself is looked up and returned.

"""
function Base.getindex{K}(t::DTable{K}, idxs...)
    if typeof(idxs) <: astuple(K)
        _getindex_scalar(t, idxs)
    else
        _getindex(t, idxs)
    end
end

function _getindex_scalar{K,V}(t::DTable{K,V}, idxs)
    # scalar getindex
    brects = boundingrect.(t.subdomains)
    function shouldlook(rect)
        for i in 1:nfields(idxs)
            if !(idxs[i] in Interval(rect.first[i], rect.last[i]))
                return false
            end
        end
        return true
    end
    subchunk_idxs = find(shouldlook, brects)
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

