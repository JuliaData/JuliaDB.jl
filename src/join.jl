import IndexedTables: naturaljoin, _naturaljoin, leftjoin, similarz, asofjoin, merge
import Base: broadcast

export naturaljoin, innerjoin, leftjoin, asofjoin, merge

"""
    naturaljoin(left::DTable, right::DTable, [op])

Returns a new `DTable` containing only rows where the indices are present both in
`left` AND `right` tables. The data columns are concatenated.
"""
function naturaljoin(left::DTable{I,D1}, right::DTable{J,D2}) where {I,J,D1,D2}
    op = combine_op_t(D1, D2)
    naturaljoin(left, right, op, true)
end

"""
    naturaljoin(left::DTable, right::DTable, op, ascolumns=false)

Returns a new `DTable` containing only rows where the indices are present both in
`left` AND `right` tables. The data columns are concatenated. The data of the matching
rows from `left` and `right` are combined using `op`. If `op` returns a tuple or
NamedTuple, and `ascolumns` is set to true, the output table will contain the tuple
elements as separate data columns instead as a single column of resultant tuples.
"""
function naturaljoin(left::DTable{I1,D1},
                     right::DTable{I2,D2},
                     op, ascolumns=false) where {I1, I2, D1, D2}
    out_subdomains = Any[]
    out_chunks = Any[]

    I = promote_type(I1, I2)        # output index type
    D = Base.promote_op(op, D1, D2) # output data type

    cols(v) = (v,)
    cols(v::Columns) = v.columns

    # if the output data type is a tuple and `columns` arg is true,
    # we want the output to be a Columns rather than an array of tuples
    default_data = if ascolumns && issubtype(D, Tuple)
        (l,r) -> Columns(map(similarz,cols(l.data))...,map(similarz,cols(r.data))...)
        # TODO: do this sort of thing for NamedTuples
        # needs IndexedTables to comply as well.
    else
        (l,r) -> similar(l.data, D, 0)
    end

    for i in 1:length(left.chunks)
        lchunk = left.chunks[i]
        subdomain = left.subdomains[i]
        lbrect = subdomain.boundingrect
        # for each chunk in `left`
        # find all the overlapping chunks from `right`
        overlapping = map(boundingrect.(right.subdomains)) do rbrect
            boxhasoverlap(lbrect, rbrect)
        end
        overlapping_chunks = right.chunks[overlapping]

        # each overlapping chunk from `right` should be joined
        # with the chunk `lchunk`
        joined_chunks = map(overlapping_chunks) do r
            delayed((x,y) -> IndexedTables._naturaljoin(x,y,op, default_data(x,y)))(lchunk, r)
        end
        append!(out_chunks, joined_chunks)

        overlapping_subdomains = map(r->intersect(subdomain, r),
                                     right.subdomains[overlapping])

        append!(out_subdomains, overlapping_subdomains)
    end

    return cache_thunks(DTable{I, D}(out_subdomains, out_chunks))
end

combine_op_t(a, b) = tuple
combine_op_t(a::Type{T}, b::Type{U}) where {T<:Tuple, U<:Tuple} = (l, r)->(l..., r...)
combine_op_t(a, b::Type{T}) where {T<:Tuple} = (l, r)->(l, r...)
combine_op_t(a::Type{T}, b) where {T<:Tuple} = (l, r)->(l..., r)

Base.map(f, x::DTable{I}, y::DTable{I}) where {I} = naturaljoin(x, y, f)


# left join

"""
    leftjoin(left::DTable, right::DTable, [op::Function])

Keeps only rows with indices in `left`. If rows of the same index are
present in `right`, then they are combined using `op`. `op` by default
picks the value from `right`.
"""
function leftjoin(left::DTable{K,V}, right::DTable,
             op = IndexedTables.right,
             joinwhen = boxhasoverlap,
             chunkjoin = leftjoin) where {K,V}

    out_chunks = Any[]

    for i in 1:length(left.chunks)
        lchunk = left.chunks[i]
        subdomain = left.subdomains[i]
        lbrect = subdomain.boundingrect
        # for each chunk in `left`
        # find all the overlapping chunks from `right`
        overlapping = map(rbrect -> joinwhen(lbrect, rbrect),
                          boundingrect.(right.subdomains))
        overlapping_chunks = right.chunks[overlapping]
        if !isempty(overlapping_chunks)
            push!(out_chunks, reduce(delayed((x,y)->chunkjoin(x,y, op)), lchunk,
                                         overlapping_chunks))
        else
            push!(out_chunks, lchunk)
        end
    end

    cache_thunks(DTable{K,V}(left.subdomains, out_chunks))
end

function asofpred(lbrect, rbrect)
    allbutlast(x::Interval) = Interval(first(x)[1:end-1],
                                       last(x)[1:end-1])
    all(boxhasoverlap(lbrect, rbrect)) ||
    (all(boxhasoverlap(allbutlast(lbrect),
                       allbutlast(rbrect))) &&
     !isless(last(lbrect), first(rbrect)))
end

"""
    asofjoin(left::DTable, right::DTable)

Keeps the indices of `left` but uses the value from `right` corresponding to highest
index less than or equal to that of left.
"""
function asofjoin(left::DTable, right::DTable)
    leftjoin(left, right, IndexedTables.right, asofpred, (x,y,op)->asofjoin(x,y))
end

"""
    merge(left::DTable, right::DTable; agg)

Merges `left` and `right` combining rows with matching indices using `agg`.
By default `agg` picks the value from `right`.
"""
function merge(left::DTable{I1,D1}, right::DTable{I2,D2}; agg=IndexedTables.right) where {I1,I2,D1,D2}
    out_subdomains = Any[]
    out_chunks = Any[]
    usedup_right = Array{Bool}(length(right.subdomains))

    I = promote_type(I1, I2)        # output index type
    D = promote_type(D1, D2)        # output data type

    t = DTable{I,D}(vcat(left.subdomains, right.subdomains),
                    vcat(left.chunks, right.chunks))

    overlap_merge(x, y) = merge(x, y, agg=agg)
    if has_overlaps(t.subdomains)
        t = rechunk(t,
                    merge = (x...)->_merge(overlap_merge, x...),
                    closed=agg!==nothing)
    end

    return cache_thunks(t)
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

function broadcast(f, A::DTable{K1,V1}, B::DTable{K2,V2}; dimmap=nothing) where {K1,K2,V1,V2}
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
    out_subdomains = []
    for (dA, cA) in zip(A.subdomains, A.chunks)
        for (dB, cB) in zip(B.subdomains, B.chunks)
            boxA = subbox(dA.boundingrect, common_A)
            boxB = subbox(dB.boundingrect, common_B)

            fst = map(max, boxA.first, boxB.first)
            lst = map(min, boxA.last, boxB.last)
            dmn = bcast_narrow_space(dA, common_A, fst, lst)
            if boxhasoverlap(boxA, boxB)
                push!(out_chunks, delayed(innerbcast)(cA, cB))
                push!(out_subdomains, dmn)
            end
        end
    end
    V = IndexedTables._promote_op(f, V1, V2)
    t1 = DTable{K1, V}(out_subdomains, out_chunks)
    with_overlaps(t1) do chunks
        treereduce(delayed(_merge), chunks)
    end
end

function match_indices(A::DTable{K1},B::DTable{K2}) where {K1,K2}
    if K1 <: NamedTuple && K2 <: NamedTuple
        Ap = dimlabels(A)
        Bp = dimlabels(B)
    else
        Ap = K1.parameters
        Bp = K2.parameters
    end
    IndexedTables.find_corresponding(Ap, Bp)
end
