import Base.Sort: Forward, Ordering, Algorithm

export rechunk

function rechunk{K,V}(t::DTable{K,V}, lengths = nothing;
               closed=false,
               merge=_merge)

    order = Forward
    ctx = Dagger.Context()
    # This might have overlapping chunks
    computed_t = compute(ctx, t, allowoverlap=true)


    if lengths === nothing
        lengths = get.(nrows.(computed_t.subdomains))
    end

    # Get ranks for splitting at
    ranks = cumsum(lengths)[1:end-1]

    idx = dindex(computed_t).result
    # select elements of required ranks in parallel:
    splitters = Dagger.pselect(ctx, idx, ranks, order)

    thunks, subspaces = shuffle_merge(ctx, computed_t.chunks,
                                      merge, ranks, closed, 
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
                       lengths::AbstractArray, ord::Base.Sort.Ordering)
    # splitter_indices: array of (splitter => vector of p index ranges) in sorted order
    starts = ones(Int, length(cs))

    empty = compute(delayed(x->subtable(x, 1:0))(cs[1])) # An empty table with the right types
    merged_chunks = [begin
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
        thnk, subspace

        end for (rank, idxs) in zip(ranks, map(last, splitter_indices))]

    # trailing sub-chunks make up the last chunk:
    t, s = merge_thunk(cs, merge, starts, lengths, empty, ord)
    thunks = vcat(map(first, merged_chunks), t)
    subspaces = vcat(map(last, merged_chunks), s)
    thunks, subspaces
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
