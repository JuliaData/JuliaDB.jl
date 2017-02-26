import IndexedTables: astuple

Base.getindex(t::DTable, idxs...) = _getindex(t, idxs)

function _getindex{D,I,A,B}(t::DTable{NDSparse{D,I,A,B}}, idxs::I)
    # scalar getindex
    t1 = withchunksindex(t) do nds
        subchunk_idxs = find(c->any(map(in, idxs, c)), nds.index)
        NDSparse(nds.index[subchunk_idxs], nds.data[subchunk_idxs])
    end
    gather(t1)[idxs...]
end

function _getindex(t::DTable, idxs)
    I = index(t)
    cs = astuple(I.columns)
    if length(idxs) != length(I.columns)
        error("wrong number of indices")
    end
    for idx in idxs
        isa(idx, AbstractVector) && (issorted(idx) || error("indices must be sorted for ranged/vector indexing"))
    end

    # Subset the chunks
    # this is quite dumb at the moment, - only looks at the first dimension
    # really
    t = withchunksindex(t) do nds
        subchunk_idxs = find(c->any(map(in, idxs, c)), nds.index)
        NDSparse(nds.index[subchunk_idxs], nds.data[subchunk_idxs])
    end

    mapchunks(t, keeplengths=false) do chunk
        Thunk(x -> x[idxs...], chunk)
    end
end

function where{N}(d::DTable, idxs::Vararg{Any,N})
end