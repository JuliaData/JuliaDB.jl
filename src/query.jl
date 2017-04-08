export select, convertdim, aggregate, reducedim_vec

import IndexedTables: convertdim, aggregate, aggregate_vec, reducedim_vec
import Base: reducedim

"""
`select(arr::DTable, conditions::Pair...)`

Filter based on index columns. Conditions are accepted as column-function pairs.

Example: `select(arr, 1 => x->x>10, 3 => x->x!=10 ...)`
"""
function Base.select(t::DTable, conditions::Pair...)
    mapchunks(delayed(x -> select(x, conditions...)), t, keeplengths=false)
end

function Base.select(t::DTable, which::DimName...; agg=nothing)
    t1 = mapchunks(delayed(x -> select(x, which...; agg=agg)), t, keeplengths=true)
    t2 = withchunksindex(t1) do cs
        cs1 = select(cs, which...)
        # remove dimensions from bounding boxes
        sub_dims = [which...]
        newbrects = map(b->b[sub_dims], cs1.data.columns.boundingrect)
        newdata = tuplesetindex(cs1.data.columns, newbrects, :boundingrect)
        Table(cs1.index, Columns(newdata), presorted=true)
    end
    if has_overlaps(index_spaces(chunks(t2)), true)
        overlap_merge = (x, y) -> merge(x, y, agg=agg)
        rechunk(t2, merge=(ts...) -> _merge(overlap_merge, ts...), closed=true)
    else
        t2
    end
end

function aggregate(f, t::DTable)
    if has_overlaps(index_spaces(chunks(t)), true)
        overlap_merge = (x, y) -> merge(x, y, agg=f)
        t = rechunk(t, merge=(ts...) -> _merge(overlap_merge, ts...), closed=true)
    end
    mapchunks(delayed(c->aggregate(f, c)), t, keeplengths=false)
end

function aggregate_vec(f, t::DTable)
    if has_overlaps(index_spaces(chunks(t)), true)
        t = rechunk(t, closed=true) # Do not have chunks that are continuations
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
function convertdim(t::DTable, d::DimName, xlat;
                    agg=nothing, vecagg=nothing, name=nothing)

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

        lengths = agg !== nothing ?
            fill(Nullable{Int}(), length(newrects)) :
            cs.data.columns.length

        newcols = tuplesetindex(newcols, lengths, :length)

        Table(cs1.index, Columns(newcols..., names=fieldnames(newcols)))
    end

    if agg !== nothing && has_overlaps(index_spaces(chunks(t2)), true)
         overlap_merge = (x, y) -> merge(x, y, agg=agg)
         rechunk(t2, merge=(ts...) -> _merge(overlap_merge, ts...), closed=true)
    elseif vecagg != nothing
        aggregate_vec(vecagg, t2)
    else
        t2
    end
end

function reducedim(f, x::DTable, dims)
    keep = setdiff([1:ndims(x);], dims) # TODO: Allow symbols
    if isempty(keep)
        throw(ArgumentError("to remove all dimensions, use `reduce(f, A)`"))
    end
    select(x, keep..., agg=f)
end

reducedim(f, x::DTable, dims::Symbol) = reducedim(f, x, [dims])

"""
`reducedim_vec(f::Function, t::DTable, dims)`

Like `reducedim`, except uses a function mapping a vector of values to a scalar instead
of a 2-argument scalar function.
"""
function reducedim_vec(f, x::DTable, dims)
    keep = setdiff([1:ndims(x);], dims)
    if isempty(keep)
        throw(ArgumentError("to remove all dimensions, use `reduce(f, A)`"))
    end

    t = select(x, keep...; agg=nothing)
    aggregate_vec(f, t)
end

reducedim_vec(f, x::DTable, dims::Symbol) = reducedim_vec(f, x, [dims])
