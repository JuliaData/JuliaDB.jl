import Base.Sort: Forward, Ordering, Algorithm
import Dagger: affinity, @dbg, OSProc, timespan_start, timespan_end

export rechunk

function rechunk{K,V}(t::DTable{K,V}, lengths = nothing;
               select=sampleselect,
               closed=false,
               merge=_merge)

    order = Forward
    ctx = get_context()

    # This might have overlapping chunks
    # We cache the chunks and use them in the final result
    computed_t = compute(ctx, cache_thunks(t), allowoverlap=true)

    chunk_lengths = get.(nrows.(computed_t.subdomains))
    if lengths === nothing
        lengths = chunk_lengths
    end

    # Get ranks for splitting at
    ranks = cumsum(lengths)[1:end-1]

    idx = dindex(computed_t)
    map(Dagger.persist!, idx.chunks)
    # select elements of required ranks in parallel:
    splitters = select(ctx, idx, ranks, order)

    Dagger.free!(idx, force=true, cache=false) # no more useful
    thunks, subspaces = shuffle_merge(ctx, t.chunks, computed_t.subdomains,
                                      merge, ranks, closed, 
                                      splitters, chunk_lengths, order)

    fromchunks(thunks, subspaces; KV=(K,V))
end

function sampleselect(ctx, idx, ranks, order; samples=32)

    sample(n) = x -> x[randomsample(n, 1:length(x))]
    sample_chunks = map(delayed(sample(samples)), idx.chunks)
    sampleidx = sort!(collect(ctx, delayed(vcat)(sample_chunks...)), order=order)

    samplecuts = map(x->round(Int, x), (ranks ./ length(domain(idx))) .* length(sampleidx))
    splitteridxs = max.(1, min.(samplecuts, length(sampleidx)))
    splitters = sampleidx[splitteridxs]
    find_ranges(x) = map(splitters) do s
        searchsorted(x, s)
    end
    xs = collect(ctx, delayed(hcat)(map(delayed(find_ranges), idx.chunks)...))
    Pair[splitters[i]=>xs[i, :] for i in 1:size(xs,1)]
end

immutable All
end
subtable(t::IndexedTable, ::All) = t

function merge_thunk(cs::AbstractArray, subdomains::AbstractArray, merge::Function, starts::AbstractArray, lasts::AbstractArray, empty, ord::Base.Sort.Ordering)
    ranges = map(UnitRange, starts, lasts)
    nonempty = find(map(x->!isempty(x), ranges))
    if isempty(nonempty)
        []
    else
        cs1 = Any[]
        ds = Any[]
        for i in nonempty
            c,d,r = cs[i], subdomains[i], ranges[i]
            n = nrows(d)
            if !isnull(n) && get(n) == length(r)
                push!(cs1, (c, All()))
                push!(ds, domain(c))
            else
                push!(cs1, (c, r))
                push!(ds, delayed(subindexspace)(c, r))
            end
        end
        cs1
    end
end

function shuffle_merge(ctx::Dagger.Context, cs::AbstractArray,
                       subdomains::AbstractArray,
                       merge::Function, ranks::AbstractArray,
                       closed::Bool, splitter_indices::AbstractArray,
                       ls::AbstractArray, ord::Base.Sort.Ordering)
    # splitter_indices: array of (splitter => vector of p index ranges) in sorted order
    starts = ones(Int, length(cs))
    lasts = copy(starts)

    empty = compute(ctx, delayed(x->subtable(x, 1:0))(cs[1])) # An empty table with the right types

    subparts = Any[]
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
                    break
                end
                available = min(reqd, length(idxs[i])) # We add more elements
                                                       # keeping sort stability
                lasts[i] += available
                i += 1
            end
        end

        subps = merge_thunk(cs, subdomains, merge, starts, lasts, empty, ord)
        starts = lasts.+1

        push!(subparts, subps)
    end

    # trailing sub-chunks make up the last chunk:
    subps = merge_thunk(cs, subdomains, merge, starts, ls, empty, ord)


    push!(subparts, subps)

    transfers = Dict()
    ws = workers()
    for (to_chunk, ps) in enumerate(subparts)
        to_pid = (to_chunk % length(ws)) + 1
        for (p, range) in ps
            pids = affinity(p)
            if !isempty(pids)
                aff = first(rand(affinity(p))).pid
            else
                aff = rand(ws) # ask a random worker to compute it
            end
            if !haskey(transfers, aff=>to_pid)
                transfers[aff=>to_pid] = Any[]
            end
            push!(transfers[aff=>to_pid], to_chunk=>(p, range))
        end
    end

    dest_chunks = Dict()
    @sync for (from_pid, to_pid) in sort(collect(keys(transfers)))
        subchunks = transfers[from_pid=>to_pid]
        dest_chunk_ids = first.(subchunks)
        actual_data = last.(subchunks)
        @async begin
            part_chunks =
                let id = Dagger.next_id()
                    remotecall_fetch(to_pid) do
                        @dbg timespan_start(ctx, :comm, id, OSProc(to_pid))
                        parts = remotecall_fetch(from_pid) do
                            ps = Any[]
                            for (d, r) in actual_data
                                push!(ps, subtable(collect(ctx, d), r))
                            end
                            ps
                        end
                        cs = map(tochunk, parts)
                        @dbg timespan_end(ctx, :comm, id, OSProc(to_pid))
                        cs
                    end
                end

            for (c_id, p) in zip(dest_chunk_ids, part_chunks)
                if !haskey(dest_chunks, c_id)
                    dest_chunks[c_id] = Any[]
                end
                push!(dest_chunks[c_id], p)
            end
        end
    end

    result = [begin
        delayed(merge)(sort(dest_chunks[k], by=x->first(domain(x)))...) =>
        reduce(JuliaDB.merge, domain.(dest_chunks[k]))
    end for k in sort(collect(keys(dest_chunks)))]

    first.(result), last.(result)
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
