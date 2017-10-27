import IndexedTables: NextTable, colnames, reindex, excludecols

export DNextTable

struct DNextTable{T,K}
    pkey::Vector{Int}
    primarydomains::Vector{IndexSpace{K}}
    chunks::Vector
end

function Dagger.domain(t::NextTable)
    ks = primarykeys(t)
    T = eltype(ks)

    if isempty(t)
        return EmptySpace{T}()
    end

    wrap = T<:NamedTuple ? T : tuple

    interval = Interval(wrap(first(ks)...), wrap(last(ks)...))
    cs = astuple(columns(ks))
    extr = map(extrema, cs[2:end]) # we use first and last value of first column
    boundingrect = Interval(wrap(first(cs[1]), map(first, extr)...),
                            wrap(last(cs[1]), map(last, extr)...))
    return t.primarykey => IndexSpace{T}(interval, boundingrect, Nullable{Int}(length(t)))
end

delayedmap(f, xs) = map(delayed(f), xs)

Base.eltype(dt::DNextTable{T}) where {T} = T
colnames{T}(t::DNextTable{T}) = fieldnames(T)

"""
`tablefromchunks(chunks::AbstractArray, [subdomains::AbstracArray]; allowoverlap=true)`

Convenience function to create a DNextTable from an array of chunks.
The chunks must be non-Thunks. Omits empty chunks in the output.
"""
function tablefromchunks(chunks::AbstractArray, pkey,
                    subdomains::AbstractArray = map(domain, chunks);
                    K = promote_eltypes(eltype.(subdomains)),
                    T = promote_eltypes(eltype.(chunktype.(chunks))))

    nzidxs = find(x->!isempty(x), subdomains)
    subdomains = subdomains[nzidxs]

    DNextTable{T, K}(pkey, subdomains, chunks[nzidxs])
end

function promote_eltypes(ts::AbstractArray)
    reduce(_promote_type, ts)
end

"""
    distribute(itable::NextTable, rowgroups::AbstractArray)

Distributes an NextTable object into a DNextTable by splitting it up into chunks
of `rowgroups` elements. `rowgroups` is a vector specifying the number of
rows in the chunks.

Returns a `DNextTable`.
"""
function distribute(t::NextTable{V}, rowgroups::AbstractArray; closed = false) where V
    splits = cumsum([0, rowgroups;])

    if splits[end] != length(t)
        throw(ArgumentError("the row groups don't add up to total number of rows"))
    end

    ranges = map(UnitRange, splits[1:end-1].+1, splits[2:end])

    # this works around locality optimizations in Dagger to make
    # sure that the parts get distributed instead of being left on
    # the master process - which would lead to all operations being serial.
    chunks = map(r->delayed(identity)(t[r]), ranges)
    subdomains = map(r->subindexspace(t, r), ranges)
    cache_thunks(tablefromchunks(chunks, t.primarykey, subdomains, K=eltype(primarykeys(t)), T=V))
end

function distribute(t::NextTable, n::Integer=nworkers())
    N = length(t)
    q, r = divrem(N, n)
    nrows = vcat(collect(_repeated(q, n)))
    nrows[end] += r
    distribute(t, nrows)
end

compute(t::DNextTable; kwargs...) = compute(get_context(), t; kwargs...)

function compute(ctx, t::DNextTable)
    if any(Dagger.istask, t.chunks)
        # we need to splat `thunks` so that Dagger knows the inputs
        # are thunks and they need to be staged for scheduling
        vec_thunk = delayed((refs...) -> [refs...]; meta=true)(t.chunks...)
        cs = compute(ctx, vec_thunk) # returns a vector of Chunk objects
        ds = domain.(cs)
        pkeys = first.(ds)
        t1 = tablefromchunks(cs, first(pkeys), last.(ds))
        compute(t1)
    else
        map(Dagger.unrelease, t.chunks) # don't let this be freed
        foreach(Dagger.persist!, t.chunks)
        t
    end
end
