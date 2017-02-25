import IndexedTables: astuple

Base.getindex(t::DTable, idxs...) = _getindex(t, idxs)

# TODO make it possible to dispatch and detect scalar indexing
# i.e. parameterize DTable with index and data tuple types

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

@testset "Getindex" begin
    nds = NDSparse(Columns(rand(["X","Y"], 100), rand(1:20, 100)), rand(100), agg=+)
    dt = distribute(nds, 10)
    @test gather(dt[["X", "Y"], 2:8]) == nds[["X", "Y"], 2:8]
end

function where{N}(d::DTable, idxs::Vararg{Any,N})
end
