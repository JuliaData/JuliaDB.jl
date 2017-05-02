import IndexedTables: naturaljoin, _naturaljoin, leftjoin, similarz, asofjoin, merge

export naturaljoin, innerjoin, leftjoin, asofjoin, merge

"""
    naturaljoin(left::DTable, right::DTable, [op])

Returns a new `DTable` containing only rows where the indices are present both in
`left` AND `right` tables. The data columns are concatenated.
"""
function naturaljoin{I,J,D1,D2}(left::DTable{I,D1}, right::DTable{J,D2})
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
function naturaljoin{I1, I2, D1, D2}(left::DTable{I1,D1},
                                     right::DTable{I2,D2},
                                     op, ascolumns=false)
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
combine_op_t{T<:Tuple, U<:Tuple}(a::Type{T}, b::Type{U}) = (l, r)->(l..., r...)
combine_op_t{T<:Tuple}(a, b::Type{T}) = (l, r)->(l, r...)
combine_op_t{T<:Tuple}(a::Type{T}, b) = (l, r)->(l..., r)

Base.map{I}(f, x::DTable{I}, y::DTable{I}) = naturaljoin(x, y, f)


# left join

"""
    leftjoin(left::DTable, right::DTable, [op::Function])

Keeps only rows with indices in `left`. If rows of the same index are
present in `right`, then they are combined using `op`. `op` by default
picks the value from `right`.
"""
function leftjoin{K,V}(left::DTable{K,V}, right::DTable,
                  op = IndexedTables.right,
                  joinwhen = boxhasoverlap,
                  chunkjoin = leftjoin)

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
function merge{I1,I2,D1,D2}(left::DTable{I1,D1}, right::DTable{I2,D2}; agg=IndexedTables.right)
    out_subdomains = Any[]
    out_chunks = Any[]
    usedup_right = Array{Bool}(length(right.subdomains))

    I = promote_type(I1, I2)        # output index type
    D = promote_type(D1, D2)        # output data type

    cols(v) = (v,)
    cols(v::Columns) = v.columns

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

        broadcast!(&, usedup_right, usedup_right, overlapping)

        # all overlapping chunk from `right` should be merged
        # with the chunk `lchunk`
        out_chunk = treereduce(delayed((x,y)->merge(x, y; agg=agg)),
                               [lchunk, overlapping_chunks...], lchunk)
        push!(out_chunks, out_chunk)

        merged_subdomain = reduce(merge, subdomain, right.subdomains[overlapping])
        push!(out_subdomains, merged_subdomain)
    end
    leftout_right = broadcast(!, usedup_right)
    out_subdomains = vcat(out_subdomains, right.subdomains[leftout_right])

    out_chunks = vcat(out_chunks, right.chunks[leftout_right])

    t = DTable{I, D}(out_subdomains, out_chunks)

    if agg !== nothing && has_overlaps(out_subdomains, true)
        overlap_merge = (x, y) -> merge(x, y, agg=agg)
        t = rechunk(t, merge=(ts...) -> _merge(overlap_merge, ts...), closed=true)
    end

    return cache_thunks(t)
end
