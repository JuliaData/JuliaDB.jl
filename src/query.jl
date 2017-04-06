export select, convertdim, aggregate

import IndexedTables: convertdim, aggregate, aggregate_to, aggregate_vec

"""
`select(arr::DTable, conditions::Pair...)`

Filter based on index columns. Conditions are accepted as column-function pairs.

Example: `select(arr, 1 => x->x>10, 3 => x->x!=10 ...)`
"""
function Base.select(t::DTable, conditions::Pair...)
    mapchunks(delayed(x -> select(x, conditions...)), t, keeplengths=false)
end

function aggregate(f, t::DTable)
    if has_overlaps(index_spaces(chunks(t)), true)
        overlap_merge = (x, y) -> merge(x, y, agg=f)
        t = _sort(t, merge=(ts...) -> _merge(overlap_merge, ts...), closed=true)
    end
    mapchunks(delayed(c->aggregate(f, c)), t, keeplengths=false)
end

function aggregate_vec(f, t::DTable)
    if has_overlaps(index_spaces(chunks(t)), true)
        t = _sort(t, closed=true) # Do not have chunks that are continuations
    end
    mapchunks(delayed(c->aggregate_vec(f, c)), t, keeplengths=false)
end

# Filter on data field
function Base.filter(fn::Function, t::DTable)
    mapchunks(delayed(x -> filter(fn, x)), t, keeplengths=false)
end

"""
`convertdim(x::DTable, d::DimName, xlate; agg::Function, name)`

Apply function or dictionary `xlate` to each index in the specified dimension.
If the mapping is many-to-one, `agg` is used to aggregate the results.
`name` optionally specifies a name for the new dimension. `xlate` must be a
monotonically increasing function.
"""
function convertdim(t::DTable, d::DimName, xlat; agg=nothing, vecagg=nothing, name=nothing)
    if isa(d, Symbol)
        dn = findfirst(dimlabels(t), d)
        if dn == 0
            throw(ArgumentError("table has no dimension \"$d\""))
        end
        d = dn
    end

    chunkf(c) = convertdim(c, d, xlat; agg=agg, vecagg=nothing, name=name)
    t1 = mapchunks(delayed(chunkf), t)

    # apply the same convertdim on the index
    t2 = withchunksindex(t1) do cs
        cs1 = convertdim(cs, d, x->_map(xlat,x), name=name)

        # apply xlat to bounding rectangles
        newrects = map(cs.data.columns.boundingrect) do box
            # box is an Interval of tuples
            # xlat the (d)th element of tuple
            tuplesetindex(box, _map(xlat, box[d]), d)
        end

        newcols = tuplesetindex(cs.data.columns, newrects, :boundingrect)
        newcols = tuplesetindex(cs.data.columns, fill(Nullable{Int}(), length(newrects)), :length)
        Table(cs1.index, Columns(newcols..., names=fieldnames(newcols)))
    end

    if agg !== nothing
        overlap_merge = (x, y) -> merge(x, y, agg=agg)
        t3 = _sort(t2, merge=(ts...) -> _merge(overlap_merge, ts...), closed=true)
    elseif vecagg != nothing
        t3 = aggregate_vec(vecagg, t2)
    else
        t3 = t2
    end

    return t3
end
