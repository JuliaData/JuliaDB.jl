export select, convertdim, aggregate

import IndexedTables: convertdim, aggregate, aggregate_to

"""
`select(arr::DTable, conditions::Pair...)`

Filter based on index columns. Conditions are accepted as column-function pairs.

Example: `select(arr, 1 => x->x>10, 3 => x->x!=10 ...)`
"""
function Base.select(t::DTable, conditions::Pair...)
    mapchunks(delayed(x -> select(x, conditions...)), t, keeplengths=false)
end


# Merge two data-rows in the metadata table
function merge_metadata(m1, m2, chunk_merge=merge, boundingrect_merge=merge)
    # Order of args is significant!!
    @NT(boundingrect=boundingrect_merge(m1.boundingrect, m2.boundingrect),
        chunk=delayed(chunk_merge)(m1.chunk, m2.chunk),
        length=Nullable{Int}())
end

function _aggregate_chunks(cs::NDSparse, f, boundingf=merge)
    merged_data = aggregate_to(cs.index, cs.data) do x, y
        # TODO: aggregate_vec then treereduce
        merge_metadata(x, y, f, boundingf)
    end |> last
    # merge index-rows
    merged_index = aggregate_to(cs.index, cs.index) do x, y
        map(merge, x, y)
    end |> last
    NDSparse(merged_index, merged_data)
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
    cs = chunks(t)

    chunkf(c) = convertdim(c, d, xlat; agg=agg, vecagg=vecagg, name=nothing)
    t1 = mapchunks(delayed(chunkf), t)

    # First, apply the same convertdim on the index
    t2 = withchunksindex(t1) do cs
        convertdim(cs, d, x->_map(xlat,x), name=name)
        # apply xlat to bounding rectangles
        newrects = map(cs.data.columns.boundingrect) do box
            # box is an Interval of tuples
            # xlat the (d)th element of tuple
            tuplesetindex(box, _map(xlat, box[d]), d)
        end
        newcols = tuplesetindex(cs.data.columns, newrects, :boundingrect)
        newcols = tuplesetindex(cs.data.columns, fill(Nullable{Int}(), length(newrects)), :length)
        NDSparse(cs.index, Columns(newcols..., names=fieldnames(newcols)))
    end

    function merge_boundingrect(a, b)
        a_convdim = _map(xlat, a[d])
        b_convdim = _map(xlat, b[d])
        map(merge, tuplesetindex(a, a_convdim, d), tuplesetindex(b, a_convdim, d))
    end
    # Collapse overlapping chunks:
    withchunksindex(t2) do cs
        _aggregate_chunks(cs, (x,y)->convertdim(merge(x,y), d, xlat;
                                                agg=agg, vecagg=vecagg, name=nothing),
                         merge_boundingrect)
    end
end
