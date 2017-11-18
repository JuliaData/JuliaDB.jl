import IndexedTables: selectkeys, selectvalues, select
import IndexedTables: convertdim
import Base: mapslices

export convertdim, selectkeys, selectvalues

function Base.map(f, t::DDataset; select=nothing)
    # TODO: fix when select has a user-supplied vector
    delayedmap(t.chunks) do x
        map(f, x; select=select)
    end |> fromchunks
end

function DataValues.dropna(t::DDataset, select=(colnames(t)...))
    delayedmap(t.chunks) do x
        dropna(x, select)
    end |> fromchunks
end

function Base.filter(f, t::DDataset; select=nothing)
    delayedmap(t.chunks) do x
        filter(f, x; select=select)
    end |> fromchunks
end

function selectkeys(x::DNDSparse, which; kwargs...)
    ndsparse(rows(keys(x), which), values(x); kwargs...)
end

function selectvalues(x::DNDSparse, which; kwargs...)
    ndsparse(keys(x), rows(values(x), which); kwargs...)
end

Base.@deprecate select(x::DNDSparse, conditions::Pair...) filter(conditions, x)
Base.@deprecate select(x::DNDSparse, which::DimName...; kwargs...) selectkeys(x, which; kwargs...)
