import Base.Sort: Forward, Ordering, Algorithm, defalg, lt

function _sort{K,V}(t::DTable{K,V};
               alg::Algorithm=defalg([1]),
               lt=Base.isless,
               by=identity,
               rev::Bool=false,
               order::Ordering=Forward)

    ctx = Dagger.Context()
    computed_t = compute(ctx, t, true) # This might have overlapping chunks

    lengths = map(get, chunks(computed_t).data.columns.length)
    ranks = cumsum(lengths)[1:end-1]  # Get ranks for splitting at

    idx = dindex(computed_t).result
    # select elements of required ranks in parallel:
    splitters = Dagger.pselect(ctx, idx, ranks, order)

    cs = chunks(computed_t).data.columns.chunk
    thunks, subspaces = shuffle_merge(ctx, cs, ranks, splitters, lengths, order)
    fromchunks(thunks, subspaces; KV=(K,V))
end

Dagger.mid(x::NamedTuple, y::NamedTuple) = map(Dagger.mid, x, y)

function merge_thunk(cs, starts, lasts, empty, ord)
    ranges = map(UnitRange, starts, lasts)
    nonempty = find(map(x->!isempty(x), ranges))
    if isempty(nonempty)
        empty
    else
        subspaces = gather(delayed(vcat)(map(delayed(subindexspace),
                                             cs[nonempty], ranges[nonempty])...))
        thunk = delayed(_merge)(map(delayed(subtable), cs[nonempty], ranges[nonempty])...)
        thunk, reduce(merge, subspaces)
    end
end

function shuffle_merge(ctx, cs, ranks, splitter_indices, lengths, ord)
    # splitter_indices: array of (splitter => vector of p index ranges) in sorted order
    starts = ones(Int, length(cs))

    empty = delayed(x->subtable(x, 1:0))(cs[1]) # An empty table with the right types

    merged_chunks = [begin
        lasts = map(first, idxs).-1 # First, all elements less than that of the required rank
        i = 1
        while sum(lasts) < rank
            reqd = rank - sum(lasts)
            if i > length(idxs)
                error("Median of wrong rank found")
            end
            available = min(reqd, length(idxs[i]))
            lasts[i] += available
            i += 1
        end

        thnk, subspace = merge_thunk(cs, starts, lasts, empty, ord)
        starts = lasts.+1
        thnk, subspace

        end for (rank, idxs) in zip(ranks, map(last, splitter_indices))]

    # trailing sub-chunks make up the last chunk:
    t, s = merge_thunk(cs, starts, lengths, empty, ord)
    thunks = vcat(map(first, merged_chunks), t)
    subspaces = vcat(map(last, merged_chunks), s)
    thunks, subspaces
end


### Permutedims

function Base.permutedims(t::DTable, p::AbstractVector)
    if !(length(p) == ndims(t) && isperm(p))
        throw(ArgumentError("argument to permutedims must be a valid permutation"))
    end

    t1 = mapchunks(t) do c
        delayed(permutedims)(c, p)
    end

    _sort(t1)
end
