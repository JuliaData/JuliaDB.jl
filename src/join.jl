import IndexedTables: naturaljoin, _naturaljoin, leftjoin, similarz, asofjoin, merge

export naturaljoin, innerjoin, leftjoin, asofjoin, merge

function naturaljoin{I1, I2, D1, D2}(left::DTable{I1,D1},
                                     right::DTable{I2,D2},
                                     op, columns=false)
    lcs = chunks(left)
    rcs = chunks(right)
    lidx_spaces = index_spaces(lcs) # an iterable of subdomains in the chunks table
    ridx_spaces = index_spaces(rcs)
    out_subdomains = Any[]
    out_chunks = Any[]

    I = promote_type(I1, I2)        # output index type
    D = Base.promote_op(op, D1, D2) # output data type

    cols(v) = (v,)
    cols(v::Columns) = v.columns

    # if the output data type is a tuple and `columns` arg is true,
    # we want the output to be a Columns rather than an array of tuples
    default_data = if columns && issubtype(D, Tuple)
        (l,r) -> Columns(map(similarz,cols(l.data))...,map(similarz,cols(r.data))...)
        # TODO: do this sort of thing for NamedTuples
        # needs IndexedTables to comply as well.
    else
        (l,r) -> similar(l.data, D, 0)
    end

    for i in 1:length(lcs)
        lchunk = lcs.data.columns.chunk[i]
        lbrect = lcs.data.columns.boundingrect[i]
        subdomain = lidx_spaces[i]
        # for each chunk in `left`
        # find all the overlapping chunks from `right`
        overlapping = map(rcs.data.columns.boundingrect) do rbrect
            all(map(hasoverlap, lbrect, rbrect))
        end
        overlapping_chunks = rcs.data.columns.chunk[overlapping]

        # each overlapping chunk from `right` should be joined
        # with the chunk `lchunk`
        joined_chunks = map(overlapping_chunks) do r
            delayed((x,y) -> IndexedTables._naturaljoin(x,y,op, default_data(x,y)))(lchunk, r)
        end
        append!(out_chunks, joined_chunks)

        overlapping_subdomains = map(r->intersect(subdomain, r),
                                 ridx_spaces[overlapping])

        append!(out_subdomains, overlapping_subdomains)
    end

    idxspace = intersect(left.index_space, right.index_space)
    return DTable(I, D, idxspace, chunks_index(out_subdomains, out_chunks))
end

combine_op_t(a, b) = tuple
combine_op_t{T<:Tuple, U<:Tuple}(a::Type{T}, b::Type{U}) = (l, r)->(l..., r...)
combine_op_t{T<:Tuple}(a, b::Type{T}) = (l, r)->(l, r...)
combine_op_t{T<:Tuple}(a::Type{T}, b) = (l, r)->(l..., r)

function naturaljoin{I,J,D1,D2}(left::DTable{I,D1}, right::DTable{J,D2})
    op = combine_op_t(D1, D2)
    naturaljoin(left, right, op, true)
end

Base.map{I}(f, x::DTable{I}, y::DTable{I}) = naturaljoin(x, y, f)


# left join

function leftjoin(left::DTable, right::DTable,
                  op = IndexedTables.right,
                  joinwhen = (lrect, rrect) -> any(map(hasoverlap, lrect, rrect)),
                  chunkjoin = leftjoin)

    lcs = chunks(left)
    rcs = chunks(right)
    lidx_spaces = index_spaces(lcs) # an iterable of subdomains in the chunks table
    ridx_spaces = index_spaces(rcs)
    out_chunks = Any[]

    for i in 1:length(lcs)
        lchunk = lcs.data.columns.chunk[i]
        lbrect = lcs.data.columns.boundingrect[i]
        subdomain = lidx_spaces[i]
        # for each chunk in `left`
        # find all the overlapping chunks from `right`
        overlapping = map(rbrect -> joinwhen(lbrect, rbrect),
                          rcs.data.columns.boundingrect)
        overlapping_chunks = rcs.data.columns.chunk[overlapping]
        if !isempty(overlapping_chunks)
            push!(out_chunks, reduce(delayed((x,y)->chunkjoin(x,y, op)), lchunk,
                                         overlapping_chunks))
        else
            push!(out_chunks, lchunk)
        end
    end

    newdata = tuplesetindex(lcs.data.columns, out_chunks, :chunk)
    withchunksindex(left) do cs
        Table(cs.index, Columns(newdata))
    end
end

function asofpred(lbrect, rbrect)
    all(map(hasoverlap, lbrect, rbrect)) ||
    (all(map(hasoverlap, lbrect[1:end-1], rbrect[1:end-1])) &&
     !isless(lbrect[end], rbrect[end]))
end

function asofjoin(left::DTable, right::DTable)
    leftjoin(left, right, IndexedTables.right, asofpred, (x,y,op)->asofjoin(x,y))
end

function merge{I1,I2,D1,D2}(left::DTable{I1,D1}, right::DTable{I2,D2})
    lcs = chunks(left)
    rcs = chunks(right)
    lidx_spaces = index_spaces(lcs) # an iterable of subdomains in the chunks table
    ridx_spaces = index_spaces(rcs)
    out_subdomains = Any[]
    out_chunks = Any[]
    usedup_right = Array{Bool}(length(ridx_spaces))

    I = promote_type(I1, I2)        # output index type
    D = promote_type(D1, D2)        # output data type

    cols(v) = (v,)
    cols(v::Columns) = v.columns

    for i in 1:length(lcs)
        lchunk = lcs.data.columns.chunk[i]
        lbrect = lcs.data.columns.boundingrect[i]
        subdomain = lidx_spaces[i]
        # for each chunk in `left`
        # find all the overlapping chunks from `right`
        overlapping = map(rcs.data.columns.boundingrect) do rbrect
            all(map(hasoverlap, lbrect, rbrect))
        end
        overlapping_chunks = rcs.data.columns.chunk[overlapping]

        usedup_right &= overlapping

        # all overlapping chunk from `right` should be merged
        # with the chunk `lchunk`
        out_chunk = treereduce(delayed(merge),
                               [lchunk, overlapping_chunks...], lchunk)
        push!(out_chunks, out_chunk)

        merged_subdomain = reduce(merge, subdomain, ridx_spaces[overlapping])
        push!(out_subdomains, merged_subdomain)
    end
    leftout_right = !usedup_right
    out_subdomains = vcat(out_subdomains, ridx_spaces[leftout_right])
    out_chunks = vcat(out_chunks, rcs.data.columns.chunk[leftout_right])

    idxspace = merge(left.index_space, right.index_space)
    return DTable(I, D, idxspace, chunks_index(out_subdomains, out_chunks))
end
