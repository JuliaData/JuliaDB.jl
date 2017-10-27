function take_n(t::DNDSparse, n)
    required = n
    getter(required, c) = collect(delayed(x->subtable(x, 1:min(required, length(x))))(c))

    i = 1
    top = getter(required, t.chunks[i])
    required = n - length(top)
    while required > 0 && 1 <= i < length(t.chunks)
        i += 1
        required = n - length(top)
        top = _merge(top, getter(required, t.chunks[i]))
    end
    return top
end

import IndexedTables: showtable

function Base.show(io::IO, big::DNDSparse)
    h, w = displaysize(io)
    showrows = h - 5 # This will trigger an ellipsis when there's
                     # more to see than the screen fits
    t = first(Iterators.partition(big, showrows))
    if !(values(t) isa Columns)
        cnames = colnames(keys(t))
        eltypeheader = "$(eltype(t))"
    else
        cnames = colnames(t)
        nf = nfields(eltype(t))
        if eltype(t) <: NamedTuple
            eltypeheader = "$(nf) field named tuples"
        else
            eltypeheader = "$(nf)-tuples"
        end
    end
    len = trylength(big)
    vals = isnull(len) ? "of" : "with $(get(len)) values"
    header = "$(ndims(t))-d Distributed NDSparse $vals ($eltypeheader) in $(length(big.chunks)) chunks:"
    showtable(io, t; header=header, divider=ndims(t), ellipsis=:end)
end
