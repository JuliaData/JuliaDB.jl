import IndexedTables: groupreduce, groupby

function groupreduce(f, t::DDataset, by=pkeynames(t); kwargs...)
    function groupchunk(x)
        groupreduce(f, x, by; kwargs...)
    end

    t1 = fromchunks(delayedmap(groupchunk, t.chunks))
    with_overlaps(t1, true) do cs
        treereduce(delayed(groupchunk∘_merge), cs)
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
