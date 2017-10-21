import IndexedTables: NextTable, colnames, reindex, excludecols

struct DNextTable{T}
    primarydomains::Vector{Pair{Int, IndexSpace{K}}}
    chunks::Vector
end

delayedmap(f, xs) = map(delayed(f), xs)

Base.eltype(dt::DNextTable{T}) where {T} = T
colnames{T}(t::DNextTable{T}) = fieldnames(T)

"""
`fromchunks(chunks::AbstractArray, [subdomains::AbstracArray]; allowoverlap=true)`

Convenience function to create a DNextTable from an array of chunks.
The chunks must be non-Thunks. Omits empty chunks in the output.
"""
function fromchunks(chunks::AbstractArray,
                    subdomains::AbstractArray = map(domain, chunks);
                    T = geteltype(chunks),
                    closed = false,
                    allowoverlap = true)

    nzidxs = find(x->!isempty(x), subdomains)
    subdomains = subdomains[nzidxs]

    dt = DNextTable{T}(subdomains, chunks[nzidxs])

    if !allowoverlap && has_overlaps(subdomains, closed)
        return reindex(dt, closed=closed)
    else
        return dt
    end
end

function geteltypes(xs::AbstractArray)
    types = eltype.(chunktype.(xs))
    reduce(_promote_type, types)
end

function reindex(t::DNextTable, by, select=excludecols(t, by);
                 sample=sampleselect,
                 merge=_merge)
    cs = delayedmap(t.chunks) do c
        reindex(t, by, select)
    end
end
