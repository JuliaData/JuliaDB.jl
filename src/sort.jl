function reindex(t::DDataset, by=pkeynames(t), select=excludecols(t, by); kwargs...)
    @noinline function _rechunk(c)
        reindex(c, by, select; kwargs...)
    end
    fromchunks(delayedmap(_rechunk, t.chunks))
end

"""
`rechunk(t::Union{DNDSparse, DNDSparse}[, by[, select]]; <options>)`

Reindex and sort a distributed dataset by keys selected by `by`.

Optionally `select` specifies which non-indexed fields are kept. By default this is all fields not mentioned in `by` for Table and the value columns for NDSparse.

# Options:
- `chunks` -- how to distribute the data. This can be:
    1. An integer -- number of chunks to create
    2. An vector of `k` integers -- number of elements in each of the `k` chunks. `sum(k)` must be same as `length(t)`
    3. The distribution of another array. i.e. `vec.subdomains` where `vec` is a distributed array.
- `merge::Function` -- a function which merges two sub-table or sub-ndsparse into one NDSparse. They may have overlaps in their indices.
- `splitters::AbstractVector` -- specify keys to split by. To create `n` chunks you would need to pass `n-1` splitters and also the `chunks=n` option.
- `chunks_sorted::Bool` -- are the chunks sorted locally? If true, this skips sorting or re-indexing them.
- `affinities::Vector{<:Integer}` -- which processes (Int pid) should each output chunk be created on. If unspecified all workers are used.
- `closed::Bool` -- if true, the same key will not be present in multiple chunks (although sorted). `true` by default.
- `nsamples::Integer` -- number of keys to randomly sample from each chunk to estimate splitters in the sorting process. (See [samplesort](https://en.wikipedia.org/wiki/Samplesort)). Defaults to 2000.
- `batchsize::Integer` -- how many chunks at a time from the input should be loaded into memory at any given time. This will essentially sort in batches of `batchsize` chunks.
"""
function rechunk(dt::DDataset,
                 by=pkeynames(dt),
                 select=dt isa DIndexedTable ? excludecols(dt, by) : valuenames(dt);
                 merge=_merge,
                 splitters=nothing,
                 chunks_presorted=false,
                 affinities=workers(),
                 chunks=nworkers(),
                 closed=true,
                 sortchunks=true,
                 nsamples=2000,
                 batchsize=max(2, nworkers()))

    if sortchunks
        perm = sortperm(dt.domains, by=first)
        cs = dt.chunks[perm]
    else
        cs = dt.chunks
    end

    function sortandsample(data, nsamples, presorted)
        r = sample(1:length(data), min(length(data), nsamples),
                   replace=false, ordered=true)

        sorted = !presorted ? reindex(data, by, select) : data
        chunk = !presorted ? tochunk(sorted) : nothing

        (chunk, pkeys(sorted)[r])
    end

    dsort_chunks(cs, chunks, nsamples,
                 batchsize=batchsize,
                 sortandsample=sortandsample,
                 affinities=affinities,
                 splitters=splitters,
                 chunks_presorted=chunks_presorted,
                 merge=merge,
                 by=pkeys,
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
