const IndexTuple = Union{Tuple, NamedTuple}

"""
    IndexSpace(interval, boundingrect, nrows)

Metadata about an chunk.

- `interval`: An `Interval` object with the first and the last index tuples.
- `boundingrect`: An `Interval` object with the lowest and the highest indices as tuples.
- `nrows`: A `Nullable{Int}` of number of rows in the NDSparse, if knowable.
"""
struct IndexSpace{T<:IndexTuple}
    interval::Interval{T}
    boundingrect::Interval{T}
    nrows::Nullable{Int}
end

"""
A distributed table
"""
mutable struct DIndexedTable{T,K} <: AbstractIndexedTable
    # primary key columns
    pkey::Vector{Int}
    # extent of values in the pkeys
    domains::Vector{IndexSpace}
    chunks::Vector
end


noweakref(w::WeakRefString) = string(w)
noweakref(x) = x
function Dagger.domain(t::IndexedTable)
    ks = pkeys(t)
    T = eltype(ks)

    if isempty(t)
        return t.pkey => EmptySpace{T}()
    end

    wrap = T<:NamedTuple ? IndexedTables.namedtuple(fieldnames(T)...)∘tuple : tuple

    interval = Interval(map(noweakref, first(ks)), map(noweakref, last(ks)))
    cs = astuple(columns(ks))
    extr = map(extrema, cs[2:end]) # we use first and last value of first column
    boundingrect = Interval(wrap(noweakref(first(cs[1])), noweakref.(map(first, extr))...),
                            wrap(noweakref(last(cs[1])), noweakref.(map(last, extr))...))
    return t.pkey => IndexSpace(interval, boundingrect, Nullable{Int}(length(t)))
end

# if one of the input vectors is a Dagger operation / array
# chose the distributed implementation.
IndexedTables._impl(::Val, ::Dagger.ArrayOp, x...) = Val(:distributed)

function table(::Val{:distributed}, tup::Tup; chunks=nothing, kwargs...)
    if chunks === nothing
        # this means the vectors are distributed.
        # pick the first distributed vector and distribute
        # all others similarly
        idx = findfirst(x->isa(x, ArrayOp), tup)
        if idx == 0
            error("Don't know how to distribute. specify `chunks`")
        end
        darr = compute(tup[idx])
        chunks = domainchunks(darr)
    end

    darrays = map(x->distribute(x, chunks), tup)

    if isempty(darrays)
        error("Table must be constructed with at least one column")
    end

    nchunks = length(darrays[1].chunks)
    cs = Array{Any}(undef, nchunks)
    names = isa(tup, NamedTuple) ? [keys(tup)...] : nothing
    f = delayed((cs...) -> table(cs...; names=names, kwargs...))
    for i = 1:nchunks
        cs[i] = f(map(x->x.chunks[i], darrays)...)
    end
    fromchunks(cs)
end

# Copying constructor
function table(t::Union{IndexedTable, DIndexedTable};
               columns=IndexedTables.columns(t),
               pkey=t.pkey,
               presorted=false,
               copy=true, kwargs...)

    table(columns;
          pkey=pkey,
          presorted=presorted,
          copy=copy, kwargs...)
end

Base.eltype(dt::DIndexedTable{T}) where {T} = T
function colnames(t::DIndexedTable{T}) where T
    fieldnames(T)
end

function trylength(t)::Nullable{Int}
    len = 0
    for l in map(x->x.nrows, t.domains)
        if !isnull(l)
            len = len + get(l)
        else
            return nothing
        end
    end
    return len
end

function Base.length(t::DIndexedTable)
    l = trylength(t)
    if isnull(l)
        error("The length of the DNDSparse is not yet known since some of its parts are not yet computed. Call `compute` to compute them, and then call `length` on the result of `compute`.")
    else
        get(l)
    end
end

"""
    fromchunks(cs)

Construct a distributed object from chunks. Calls `fromchunks(T, cs)`
where T is the type of the data in the first chunk. Computes any thunks.
"""
function fromchunks(cs::AbstractArray, args...; output=nothing, fnoffset=0, kwargs...)
    if output !== nothing
        if !isdir(output)
            mkdir(output)
        end
        cs = Any[(
            fn = lpad(idx+fnoffset, 5, "0");
            delayed(Dagger.savechunk, get_result=true)(
                c, output, fn);
        ) for (idx, c) in enumerate(cs)]

        vec_thunk = treereduce(delayed(vcat, meta=true), cs)
        if length(cs) == 1
            cs = [compute(get_context(), vec_thunk)]
        else
            cs = compute(get_context(), vec_thunk)
        end
        return fromchunks(cs)
    elseif any(x->isa(x, Thunk), cs)
        vec_thunk = delayed((refs...) -> [refs...]; meta=true)(cs...)
        cs = compute(get_context(), vec_thunk)
    end
    T = chunktype(first(cs))
    fromchunks(T, cs, args...; kwargs...)
end

function fromchunks(::Type{<:AbstractVector},
                    cs::AbstractArray, args...; kwargs...)
    lengths = length.(domain.(cs))
    dmnchunks = DomainBlocks((1,), (cumsum(lengths),))
    T = promote_eltype_chunktypes(cs)
    DArray(T, ArrayDomain(1:sum(lengths)),
           dmnchunks, cs, dvcat)
end

function fromchunks(::Type{<:IndexedTable}, chunks::AbstractArray;
                    domains::AbstractArray = last.(domain.(chunks)),
                    pkey=first(domain(first(chunks))),
                    K = promote_eltypes(domains),
                    T = promote_eltype_chunktypes(chunks))

    nzidxs = findall(!isempty, domains)
    domains = domains[nzidxs]

    DIndexedTable{T, K}(pkey, domains, chunks[nzidxs])
end

function promote_eltypes(ts::AbstractArray)
    t = eltype(ts[1])
    for i = 2:length(ts)
        t = _promote_type(t, eltype(ts[i]))
    end
    return t
end

function promote_eltype_chunktypes(ts::AbstractArray)
    t = eltype(chunktype(ts[1]))
    for i = 2:length(ts)
        t = _promote_type(t, eltype(chunktype(ts[i])))
    end
    return t
end

"""
    distribute(t::Table, chunks)

Distribute a table in `chunks` pieces. Equivalent to `table(t, chunks=chunks)`.
"""
distribute(t::IndexedTable, chunks) = table(t, chunks=chunks, copy=false)

compute(t::DIndexedTable; kwargs...) = compute(get_context(), t; kwargs...)

function compute(ctx, t::DIndexedTable; output=nothing)
    if any(Dagger.istask, t.chunks)
        fromchunks(IndexedTable, cs, output=output)
    else
        map(Dagger.unrelease, t.chunks) # don't let this be freed
        foreach(Dagger.persist!, t.chunks)
        t
    end
end

distribute(t::DIndexedTable, cs) = table(t, chunks=cs)

collect(t::DIndexedTable) = collect(get_context(), t)

function collect(ctx::Context, dt::DIndexedTable{T}) where T
    cs = dt.chunks
    if length(cs) > 0
        collect(ctx, treereduce(delayed(_merge), cs))
    else
        table(similar(IndexedTables.arrayof(T), 0), pkey=dt.pkey)
    end
end

# merging two tables

function _merge(f, a::IndexedTable, b::IndexedTable)
    if isempty(a.pkey) && isempty(b.pkey)
        return table(vcat(rows(a), rows(b)))
    end

    @assert a.pkey == b.pkey
    ia = pkeys(a)
    ib = pkeys(b)

    if isempty(a)
        b
    elseif isempty(b)
        a
    elseif last(ia) < first(ib)
        # can vcat
        table(map(vcat, columns(a), columns(b)), pkey=a.pkey, presorted=true, copy=false)
    elseif last(ib) < first(ia)
        _merge(f, b, a)
    else
        f(a,b)
    end
end

_merge(f, x::IndexedTable) = x
function _merge(f, x::IndexedTable, y::IndexedTable, ys::IndexedTable...)
    treereduce((a,b)->_merge(f, a, b), [x,y,ys...])
end

_merge(x::IndexedTable, y::IndexedTable...) = _merge((a,b) -> merge(a, b), x, y...)

function Base.show(io::IO, big::DIndexedTable)
    h, w = displaysize(io)
    showrows = h - 5 # This will trigger an ellipsis when there's
                     # more to see than the screen fits
    t = first(Iterators.partition(big, showrows))
    len = trylength(big)
    vals = isnull(len) ? "" : " with $(get(len)) rows"
    header = "Distributed Table$vals in $(length(big.chunks)) chunks:"
    cstyle = Dict(i=>:bold for i in t.pkey)
    showtable(io, t; header=header, ellipsis=:end, cstyle=cstyle)
end
