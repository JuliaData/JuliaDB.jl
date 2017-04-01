import Base.Sort: Forward, Ordering, Algorithm, defalg, lt

function _sort(t::DTable;
               alg::Algorithm=defalg([1]),
               lt=Base.isless,
               by=identity,
               rev::Bool=false,
               order::Ordering=Forward)
    computed_t = compute(t)

    lengths = map(get, chunks(computed_t).data.columns.length)
    splitter_ranks = cumsum(lengths)[1:end-1]  # Get ranks for splitting at

    ctx = Dagger.Context()
    idx = dindex(computed_t).result
    # note: here we assume that each chunk is already sorted.
    splitters = Dagger.select(ctx, idx, splitter_ranks, order)

    cs = chunks(computed_t).data.columns.chunk
    thunks = shuffle_merge(ctx, cs, splitters, lengths, order)
    #delayed(println)(thunks...)
    compute(ctx, delayed((x...)->fromchunks([x...]); meta=true)(thunks...))
end

function merge_thunk(cs, starts, lasts, empty, ord)
    ranges = map(UnitRange, starts, lasts)
    nonempty = find(map(x->!isempty(x), ranges))
    if isempty(nonempty)
        empty
    else
        delayed(_merge)(map(delayed(subtable), cs[nonempty], ranges[nonempty])...)
    end
end

function shuffle_merge(ctx, cs, splitter_indices, lengths, ord)
    # splitter_indices: array of (splitter => vector of p index ranges) in sorted order
    starts = ones(Int, length(cs))

    empty = delayed(x->subtable(x, 1:0))(cs[1]) # An empty table with the right types

    merged_chunks = [begin
        lasts = map(last, idxs)
        thnk = merge_thunk(cs, starts, lasts, empty, ord)
        #sz = sum(lasts.-starts.+1)
        starts = lasts.+1
        thnk#,sz
    end for (val, idxs) in splitter_indices]

    # trailing sub-chunks make up the last chunk:
    thunks = vcat(merged_chunks, merge_thunk(cs, starts, lengths, empty, ord))
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
