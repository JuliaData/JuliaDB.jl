import IndexedTables: pkeynames, excludecols, primarykeys, reindex
import Dagger: dsort_chunks

function reindex(dt::DNDSparse, by=pkeynames(dt), select=excludecols(dt, by);
                 nchunks=nworkers(),
                 closed=true,
                 nsamples=2000,
                 batchsize=nworkers())

    cs = dt.chunks

    function sortandsample_table(data, nsamples)
        r = sample(1:length(data), min(length(data), nsamples), replace=false, ordered=true)
        # TODO: check to see if reindex is redundant
        sorted = reindex(data, by, select)
        (tochunk(sorted), primarykeys(sorted)[r])
    end
    cs1 = dsort_chunks(cs, nchunks, nsamples, batchsize=batchsize, sortandsample=sortandsample_ndsparse, merge=_merge, by=keys, sub=subtable)
    cs2 = compute(delayed((xs...)->[xs...]; meta=true)(cs1...))
    fromchunks(cs2)
end

function reindex(dt::DNextTable, by=pkeynames(dt), select=excludecols(dt, by);
                 nchunks=nworkers(),
                 closed=true,
                 batchsize=nworkers(),
                 nsamples=2000)

    cs = dt.chunks

    function sortandsample_table(data, nsamples)
        r = sample(1:length(data), min(length(data), nsamples), replace=false, ordered=true)
        # TODO: check to see if reindex is redundant
        sorted = reindex(data, by, select)
        (tochunk(sorted), primarykeys(sorted)[r])
    end

    cs1 = dsort_chunks(cs, nchunks, nsamples, batchsize=batchsize, sortandsample=sortandsample_table, merge=_merge, by=primarykeys)
    cs2 = compute(delayed((xs...)->[xs...]; meta=true)(cs1...))
    tablefromchunks(cs2)
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
