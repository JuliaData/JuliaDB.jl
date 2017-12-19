## Table
export rechunk_together

function rechunk_together(left, right, lkey, rkey,
                          lselect=excludecols(left, lkey), rselect=excludecols(right, rkey); chunks=nworkers(), keepkeys=false)
    # we will assume that right has to be aligned to left
    if keepkeys
        l = reindex(left, lkey, (pkeynames(left), lselect))
    else
        l = reindex(left, lkey, lselect)
    end
    r = reindex(right, rkey, rselect)

    if has_overlaps(l.domains)
        l = rechunk(l, chunks=chunks)
    end

    splitters = map(last, l.domains)

    r = rechunk(r, rkey, rselect,
                splitters=splitters[1:end-1],
                chunks_presorted=true,
                affinities=map(x->first(Dagger.affinity(x))[1].pid, l.chunks),
               )
    l, r
end

function Base.join(f, left::DDataset, right::DDataset;
                   how=:inner,
                   lkey=pkeynames(left), rkey=pkeynames(right),
                   lselect=left isa DNDSparse ? valuenames(left) : excludecols(left, lkey),
                   rselect=right isa DNDSparse ? valuenames(right) : excludecols(right, rkey),
                   broadcast=nothing,
                   keepkeys=false,
                   chunks=nworkers(),
                   kwargs...)
    cl = compute(left)
    cr = compute(right)

    if broadcast === :left
        error("only broadcast = :right is supported at the moment")
    elseif broadcast === :right
        if !(how in [:inner, :left, :anti])
            error("Can only do inner, left, or anti join with broadcast = :right")
        end
        if length(cr.chunks) == 1
            right_ser = cr.chunks[1]
        else
            right_ser = collect(cr)
        end
        ps = map(x->first(Dagger.affinity(x))[1], cl.chunks)
        tasks = [delayed(identity)(right_ser) for p in ps]
        for (t, p) in zip(tasks, ps)
            t.affinity = Nullable([p=>1])
        end
        r = fromchunks(tasks)
        l = cl
    else
        l, r = rechunk_together(left, right, lkey, rkey, lselect, rselect,
                                chunks=chunks, keepkeys=keepkeys)
    end

    i = 1
    j = 1

    cs = []
    rempty = delayed(empty!∘copy)(r.chunks[end])
    lempty = delayed(empty!∘copy)(l.chunks[end])

    function joinchunks(x, y)
        res=if keepkeys && broadcast === nothing
            vs = rows(x, excludecols(x, pkeynames(x)))
            k = columns(vs)[1]
            v = columns(vs)[2]
            join(f, convert(IndexedTables.collectiontype(x), k, v),
                 y, lkey=lkey, keepkeys=true, how=how; kwargs...)
        elseif broadcast !== nothing
            join(f, x, y, lkey=lkey, rkey=rkey,
                 lselect=lselect, rselect=rselect,
                 keepkeys=keepkeys, how=how; kwargs...)
        else
            join(f, x, y, how=how; kwargs...)
        end
    end

    while i <= length(l.chunks) && j <= length(r.chunks)
        c1 = l.chunks[i]
        c2 = r.chunks[j]

        d1 = domain(c1) isa Pair ? domain(c1)[2] : domain(c1)
        d2 = domain(c1) isa Pair ? domain(c2)[2] : domain(c2)
        if hasoverlap(d1.interval, d2.interval)
            i += 1
            j += 1
        elseif last(d1.interval) < last(d2.interval)
            # this means there's no corresponding chunk on the right
            c2 = rempty
            i += 1
        else
            # this means there's no corresponding chunk on the left
            c1 = lempty
            j += 1
        end
        c = delayed(joinchunks)(c1,c2)
        push!(cs, c)
    end

    while i <= length(l.chunks)
        push!(cs, delayed(joinchunks)(l.chunks[i], rempty))
        i+=1
    end

    while j <= length(r.chunks)
        push!(cs, delayed(joinchunks)(lempty, r.chunks[j]))
        j += 1
    end

    fromchunks(cs)
end

function Base.join(left::DDataset, right; how=:inner, kwargs...)
    f = how === :anti ? ((x,y)->x) : IndexedTables.concat_tup
    join(f, left, right; how=how, kwargs...)
end

function groupjoin(f, left::DDataset, right; how=:inner, kwargs...)
    join(f, left, right; how=how, group=true, kwargs...)
end

function groupjoin(left::DDataset, right; how=:inner, kwargs...)
    join(left, right; how=how, group=true, kwargs...)
end

function join(f, left::DDataset, right::IndexedTables.Dataset; how=:inner, kwargs...)
    if how in [:inner, :left, :anti]
        join(f, left, distribute(right, 1), broadcast=:right, how=how; kwargs...)
    else
        join(f, left, distribute(right, 1), how=how; kwargs...)
    end
end

## NDSparse join

import IndexedTables: naturaljoin, leftjoin, asofjoin, merge
import Base: broadcast

export naturaljoin, innerjoin, leftjoin, asofjoin, merge

"""
    naturaljoin(left::DNDSparse, right::DNDSparse, [op])

Returns a new `DNDSparse` containing only rows where the indices are present both in
`left` AND `right` tables. The data columns are concatenated.
"""
function naturaljoin(left::DNDSparse{I,D1}, right::DNDSparse{J,D2}) where {I,J,D1,D2}
    naturaljoin(IndexedTables.concat_tup, left, right)
end

"""
    naturaljoin(op, left::DNDSparse, right::DNDSparse, ascolumns=false)

Returns a new `DNDSparse` containing only rows where the indices are present both in
`left` AND `right` tables. The data columns are concatenated. The data of the matching
rows from `left` and `right` are combined using `op`. If `op` returns a tuple or
NamedTuple, and `ascolumns` is set to true, the output table will contain the tuple
elements as separate data columns instead as a single column of resultant tuples.
"""
function naturaljoin(op, left::DNDSparse{I1,D1},
                     right::DNDSparse{I2,D2}) where {I1, I2, D1, D2}
    out_domains = Any[]
    out_chunks = Any[]

    I = promote_type(I1, I2)        # output index type
    D = IndexedTables._promote_op(op, D1, D2) # output data type

    # if the output data type is a tuple and `columns` arg is true,
    # we want the output to be a Columns rather than an array of tuples
    for i in 1:length(left.chunks)
        lchunk = left.chunks[i]
        subdomain = left.domains[i]
        lbrect = subdomain.boundingrect
        # for each chunk in `left`
        # find all the overlapping chunks from `right`
        overlapping = map(boundingrect.(right.domains)) do rbrect
            boxhasoverlap(lbrect, rbrect)
        end
        overlapping_chunks = right.chunks[overlapping]

        # each overlapping chunk from `right` should be joined
        # with the chunk `lchunk`
        joined_chunks = map(overlapping_chunks) do r
            delayed(naturaljoin)(op, lchunk, r)
        end
        append!(out_chunks, joined_chunks)

        overlapping_domains = map(r->intersect(subdomain, r),
                                     right.domains[overlapping])

        append!(out_domains, overlapping_domains)
    end

    return cache_thunks(DNDSparse{I, D}(out_domains, out_chunks))
end

Base.map(f, x::DNDSparse{I}, y::DNDSparse{I}) where {I} = naturaljoin(x, y, f)


# left join

"""
    leftjoin(left::DNDSparse, right::DNDSparse, [op::Function])

Keeps only rows with indices in `left`. If rows of the same index are
present in `right`, then they are combined using `op`. `op` by default
picks the value from `right`.
"""
function leftjoin(op, left::DNDSparse{K,V}, right::DNDSparse,
                  joinwhen = boxhasoverlap,
                  chunkjoin = leftjoin) where {K,V}

    out_chunks = Any[]

    for i in 1:length(left.chunks)
        lchunk = left.chunks[i]
        subdomain = left.domains[i]
        lbrect = subdomain.boundingrect
        # for each chunk in `left`
        # find all the overlapping chunks from `right`
        overlapping = map(rbrect -> joinwhen(lbrect, rbrect),
                          boundingrect.(right.domains))
        overlapping_chunks = right.chunks[overlapping]
        if !isempty(overlapping_chunks)
            push!(out_chunks, delayed(chunkjoin)(op, lchunk, treereduce(delayed(_merge), overlapping_chunks)))
        else
            emptyop = delayed() do op, t
                empty = NDSparse(similar(keys(t), 0), similar(values(t), 0))
                chunkjoin(op, t, empty)
            end
            push!(out_chunks, emptyop(op, lchunk))
        end
    end

    cache_thunks(DNDSparse{K,V}(left.domains, out_chunks))
end

leftjoin(left::DNDSparse, right::DNDSparse) = leftjoin(IndexedTables.concat_tup, left, right)

function asofpred(lbrect, rbrect)
    allbutlast(x::Interval) = Interval(first(x)[1:end-1],
                                       last(x)[1:end-1])
    all(boxhasoverlap(lbrect, rbrect)) ||
    (all(boxhasoverlap(allbutlast(lbrect),
                       allbutlast(rbrect))) &&
     !isless(last(lbrect), first(rbrect)))
end

function asofjoin(left::DNDSparse, right::DNDSparse)
    leftjoin(IndexedTables.right, left, right, asofpred, (op, x,y)->asofjoin(x,y))
end

function merge(left::DNDSparse{I1,D1}, right::DNDSparse{I2,D2}; agg=IndexedTables.right) where {I1,I2,D1,D2}
    out_domains = Any[]
    out_chunks = Any[]

    I = promote_type(I1, I2)        # output index type
    D = promote_type(D1, D2)        # output data type

    t = DNDSparse{I,D}(vcat(left.domains, right.domains),
                    vcat(left.chunks, right.chunks))

    overlap_merge(x, y) = merge(x, y, agg=agg)
    if has_overlaps(t.domains)
        t = rechunk(t,
                    merge=(x...)->_merge(overlap_merge, x...),
                    closed=agg!==nothing)
    end

    return cache_thunks(t)
end

function merge(left::DNextTable, right::DNextTable; chunks=nworkers())
    l, r = rechunk_together(left, right, pkeynames(left), pkeynames(right); chunks=chunks)
    fromchunks(delayedmap(merge, l.chunks, r.chunks))
end

function subbox(i::Interval, idx)
    Interval(i.first[idx], i.last[idx])
end

function bcast_narrow_space(d, idxs, fst, lst)
    intv = Interval(
        tuplesetindex(d.interval.first, fst, idxs),
        tuplesetindex(d.interval.last, lst, idxs)
    )

    box = Interval(
        tuplesetindex(d.boundingrect.first, fst, idxs),
        tuplesetindex(d.boundingrect.last, lst, idxs)
    )
    IndexSpace(intv, box, Nullable{Int}())
end

function broadcast(f, A::DNDSparse{K1,V1}, B::DNDSparse{K2,V2}; dimmap=nothing) where {K1,K2,V1,V2}
    if ndims(A) < ndims(B)
        broadcast((x,y)->f(y,x), B, A; dimmap=dimmap)
    end

    if dimmap === nothing
        dimmap = match_indices(A, B)
    end

    common_A = Iterators.filter(i->dimmap[i] > 0, 1:ndims(A)) |> collect
    common_B = Iterators.filter(i -> i>0, dimmap) |> collect
    #@assert length(common_B) == length(common_A)

    # for every bounding box in A, take compare common_A
    # bounding boxes vs. every common_B bounding box

    out_chunks = []
    innerbcast(a, b) = broadcast(f, a, b; dimmap=dimmap)
    out_domains = []
    for (dA, cA) in zip(A.domains, A.chunks)
        for (dB, cB) in zip(B.domains, B.chunks)
            boxA = subbox(dA.boundingrect, common_A)
            boxB = subbox(dB.boundingrect, common_B)

            fst = map(max, boxA.first, boxB.first)
            lst = map(min, boxA.last, boxB.last)
            dmn = bcast_narrow_space(dA, common_A, fst, lst)
            if boxhasoverlap(boxA, boxB)
                push!(out_chunks, delayed(innerbcast)(cA, cB))
                push!(out_domains, dmn)
            end
        end
    end
    V = IndexedTables._promote_op(f, V1, V2)
    t1 = DNDSparse{K1, V}(out_domains, out_chunks)
    with_overlaps(t1) do chunks
        treereduce(delayed(_merge), chunks)
    end
end

function match_indices(A::DNDSparse{K1},B::DNDSparse{K2}) where {K1,K2}
    if K1 <: NamedTuple && K2 <: NamedTuple
        Ap = dimlabels(A)
        Bp = dimlabels(B)
    else
        Ap = K1.parameters
        Bp = K2.parameters
    end
    IndexedTables.find_corresponding(Ap, Bp)
end

## Deprecation

Base.@deprecate naturaljoin(left::DNDSparse, right::DNDSparse, op::Function) naturaljoin(op, left::DNDSparse, right::DNDSparse)

Base.@deprecate leftjoin(left::DNDSparse, right::DNDSparse, op::Function) leftjoin(op, left, right)



