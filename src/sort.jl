import Base.Sort: Forward, Ordering, Algorithm

export rechunk

function rechunk{K,V}(t::DTable{K,V}, lengths = nothing;
               closed=false,
               merge=_merge)

    order = Forward
    ctx = Dagger.Context()
    # This might have overlapping chunks
    computed_t = compute(ctx, t, allowoverlap=true)
    # We need to persist all the chunks since computation
    # of any subset of the final chunks in this function
    # might cause required chunks to be gc'd.
    # we need to find ways to fix exactly this though
    foreach(Dagger.persist!, computed_t.chunks)

    chunk_lengths = get.(nrows.(computed_t.subdomains))
    if lengths === nothing
        lengths = chunk_lengths
    end

    # Get ranks for splitting at
    ranks = cumsum(lengths)[1:end-1]

    idx = dindex(computed_t).result
    Dagger.persist!(idx)
    # select elements of required ranks in parallel:
    splitters = Dagger.pselect(ctx, idx, ranks, order)

    thunks, subspaces = shuffle_merge(ctx, computed_t.chunks,
                                      merge, ranks, closed, 
                                      splitters, chunk_lengths, order)

    fromchunks(thunks, subspaces; KV=(K,V))
end

function sampleselect(ctx, idx, cumweights, order; samples=32)

    sample(n) = x -> x[randomsample(n, 1:length(x))]
    sample_chunks = map(delayed(sample(samples)), idx.chunks)
    sampleidx = sort!(gather(delayed(vcat)(sample_chunks...)), order=order)

    samplecuts = round.(Int, (cumweights ./ sum(cumweights)) .* length(sampleidx))
    splitteridxs = min.(samplecuts, length(sampleidx))
    sampleidxs[splitteridxs]
end

function merge_thunk(cs::AbstractArray, merge::Function, starts::AbstractArray, lasts::AbstractArray, empty, ord::Base.Sort.Ordering)
    ranges = map(UnitRange, starts, lasts)
    nonempty = find(map(x->!isempty(x), ranges))
    if isempty(nonempty)
        empty, domain(empty)
    else
        cs1 = Any[]
        ds = Any[]
        for (c, r) in zip(cs[nonempty], ranges[nonempty])
            n = nrows(domain(c))
            if !isnull(n) && get(n) == length(r)
                push!(cs1, c)
                push!(ds, domain(c))
            else
                push!(cs1, delayed(subtable)(c, r))
                push!(ds, delayed(subindexspace)(c, r))
            end
        end
        subspaces = gather(delayed(vcat)(ds...))
        thunk = delayed(merge)(cs1...)
        thunk, reduce(JuliaDB.merge, subspaces)
    end
end

function shuffle_merge(ctx::Dagger.Context, cs::AbstractArray,
                       merge::Function, ranks::AbstractArray,
                       closed::Bool, splitter_indices::AbstractArray,
                       ls::AbstractArray, ord::Base.Sort.Ordering)
    # splitter_indices: array of (splitter => vector of p index ranges) in sorted order
    starts = ones(Int, length(cs))
    lasts = copy(starts)

    empty = compute(delayed(x->subtable(x, 1:0))(cs[1])) # An empty table with the right types

    thunks = Any[]
    subdomains = Any[]
    for (rank, idxs) in zip(ranks, map(last, splitter_indices))
        if closed
            include_rank  = map(last, idxs)     # include elements of rank r
            lessthan_rank = map(first, idxs).-1 # only elements of rank < r
            if abs(sum(include_rank) - rank) <= abs(sum(lessthan_rank) - rank)
                lasts = include_rank
            else
                lasts = lessthan_rank
            end
        else
            lasts = map(first, idxs).-1 # First, we keep all elements less than
                                        # the one with the required rank
            i = 1
            while sum(lasts) < rank
                reqd = rank - sum(lasts)
                if i > length(idxs)
                    error("Median of wrong rank found")
                end
                available = min(reqd, length(idxs[i])) # We add more elements
                                                       # keeping sort stability
                lasts[i] += available
                i += 1
            end
        end

        thnk, subspace = merge_thunk(cs, merge, starts, lasts, empty, ord)
        starts = lasts.+1

        push!(thunks, thnk)
        push!(subdomains, subspace)
    end

    # trailing sub-chunks make up the last chunk:
    t, s = merge_thunk(cs, merge, starts, ls, empty, ord)

    vcat(thunks, t), vcat(subdomains, s)
end


### Permutedims

function Base.permutedims{K,V}(t::DTable{K,V}, p::AbstractVector)
    if !(length(p) == ndims(t) && isperm(p))
        throw(ArgumentError("argument to permutedims must be a valid permutation"))
    end

    permuteintv(intv,d) = Interval(first(intv)[d],
                                   last(intv)[d])
    idxs = map(t.subdomains) do dmn
        IndexSpace(permuteintv(dmn.interval, p),
                   permuteintv(dmn.boundingrect, p),
                   dmn.nrows,
                  )
    end

    chunks = map(delayed(c -> permutedims(c, p)), t.chunks)
    t1 = DTable{eltype(idxs[1]), V}(idxs, chunks)

    cache_thunks(rechunk(t1))
end
