import IndexedTables: pkeynames, excludecols, pkeys, reindex
import Dagger: dsort_chunks

export reindex, rechunk

using StatsBase

function reindex(t::DDataset, by=pkeynames(t), select=excludecols(t, by))
    delayedmap(t.chunks) do c
        reindex(c, by, select)
    end |> fromchunks
end

"""
`rechunk(t::Union{DTable, DNDSparse}[, by[, select]];
         chunks, closed, nsamples, batchsize)`

Re-chunk a distributed Table or NDSparse.
"""
function rechunk(dt::DDataset,
                 by=pkeynames(dt),
                 select=excludecols(dt, by);

                 chunks=nworkers(),
                 closed=true,
                 nsamples=2000,
                 batchsize=nworkers())

    cs = dt.chunks

    function sortandsample(data, nsamples)
        r = sample(1:length(data), min(length(data), nsamples),
                   replace=false, ordered=true)

        sorted = reindex(data, by, select)
        (tochunk(sorted), pkeys(sorted)[r])
    end

    dsort_chunks(cs, chunks, nsamples,
                 batchsize=batchsize,
                 sortandsample=sortandsample,
                 merge=_merge,
                 by=keys,
                 sub=subtable) |> fromchunks

end

### Permutedims

function Base.permutedims(t::DNDSparse{K,V}, p::AbstractVector) where {K,V}
    if !(length(p) == ndims(t) && isperm(p))
        throw(ArgumentError("argument to permutedims must be a valid permutation"))
    end

    permuteintv(intv,d) = Interval(first(intv)[d],
                                   last(intv)[d])
    idxs = map(t.domains) do dmn
        IndexSpace(permuteintv(dmn.interval, p),
                   permuteintv(dmn.boundingrect, p),
                   dmn.nrows,
                  )
    end

    chunks = map(delayed(c -> permutedims(c, p)), t.chunks)
    t1 = DNDSparse{eltype(idxs[1]), V}(idxs, chunks)

    cache_thunks(rechunk(t1))
end
