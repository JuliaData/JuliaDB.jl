import Base.Sort: Forward, Ordering, Algorithm, defalg, lt

function _sort{K,V}(t::DTable{K,V};
               alg::Algorithm=defalg([1]),
               lt=Base.isless,
               closed=false,
               merge=_merge,
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
    thunks, subspaces = shuffle_merge(ctx, cs, merge, ranks, closed, 
                                      splitters, lengths, order)
    fromchunks(thunks, subspaces; KV=(K,V))
end

Dagger.mid(x::NamedTuple, y::NamedTuple) = map(Dagger.mid, x, y)

function merge_thunk(cs::AbstractArray, merge::Function, starts::AbstractArray, lasts::AbstractArray, empty, ord::Base.Sort.Ordering)
    ranges = map(UnitRange, starts, lasts)
    nonempty = find(map(x->!isempty(x), ranges))
    if isempty(nonempty)
        empty, domain(empty)
    else
        subspaces = gather(delayed(vcat)(map(delayed(subindexspace),
                                             cs[nonempty], ranges[nonempty])...))
        thunk = delayed(merge)(map(delayed(subtable), cs[nonempty], ranges[nonempty])...)
        thunk, reduce(JuliaDB.merge, subspaces)
    end
end

function shuffle_merge(ctx::Dagger.Context, cs::AbstractArray,
                       merge::Function, ranks::AbstractArray,
                       closed::Bool, splitter_indices::AbstractArray,
                       lengths::AbstractArray, ord::Base.Sort.Ordering)
    # splitter_indices: array of (splitter => vector of p index ranges) in sorted order
    starts = ones(Int, length(cs))

    empty = compute(delayed(x->subtable(x, 1:0))(cs[1])) # An empty table with the right types

    merged_chunks = [begin
        if closed
            lasts = map(last, idxs) # no continuations
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
        thnk, subspace

        end for (rank, idxs) in zip(ranks, map(last, splitter_indices))]

    # trailing sub-chunks make up the last chunk:
    t, s = merge_thunk(cs, merge, starts, lengths, empty, ord)
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
