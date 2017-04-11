import IndexedTables: astuple

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
    cs = chunks(t)
    subchunk_idxs = find(c->all(map(in, idxs, c)), cs.data.columns.boundingrect)
    cs1 = Table(cs.index[subchunk_idxs], cs.data[subchunk_idxs])
    t1 = DTable{K,V}(cs1)
    gather(t1)[idxs...]
end

function _getindex{K,V}(t::DTable{K,V}, idxs)
    I = chunks(t).index
    cs = astuple(I.columns)
    if length(idxs) != length(I.columns)
        error("wrong number of indices")
    end
    for idx in idxs
        isa(idx, AbstractVector) && (issorted(idx) || error("indices must be sorted for ranged/vector indexing"))
    end

    # Subset the chunks
    # this is currently a linear search

    cs = chunks(t)
    subchunk_idxs = find(c->all(map(in, idxs, c)), cs.data.columns.boundingrect)
    cs2 = Table(cs.index[subchunk_idxs], cs.data[subchunk_idxs])
    t = DTable{K,V}(cs2)

    mapchunks(t, keeplengths=false) do chunk
        getindex(chunk, idxs...)
    end
end

