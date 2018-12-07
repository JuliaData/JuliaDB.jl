function stack(t::DDataset, by = pkeynames(t); select = isa(t, DNDSparse) ? valuenames(t) : excludecols(t, by),
    variable = :variable, value = :value)

    function stackchunk(x)
        stack(x, by; select=select, variable=variable, value=value)
    end

    return fromchunks(delayedmap(stackchunk, t.chunks))
end

function unstack(::Type{D}, ::Type{T}, key, val, cols::AbstractVector{S}) where {D <:DDataset, T, S}
    D1 = D isa DNDSparse ? NDSparse : IndexedTable
    function unstackchunk(x, y)
        unstack(D1, T, x, y, cols)
    end
    fromchunks(delayedmap(unstackchunk, key.chunks, val.chunks))
end

function unstack(t::D, by = pkeynames(t); variable = :variable, value = :value) where {D<:DDataset}
    tgrp = groupby((value => identity,), t, by, select = (variable, value))
    S = eltype(colnames(t))
    col = column(t, variable)
    cols = S.(collect(Dagger.treereduce(delayed(union), delayedmap(unique, col.chunks))))
    T = eltype(columns(t, value))
    unstack(D, Missing <: T ? nonmissing(T) : T, pkeys(tgrp), columns(tgrp, value), cols)
end
