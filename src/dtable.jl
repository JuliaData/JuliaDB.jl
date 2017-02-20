using IndexedTables, NamedTuples, Dagger

import Dagger: Domain, AbstractChunk, Thunk, chunktype,
               domain, domainchunks, tochunk, chunks, Cat

# reexport the essentials
export NDSparse, compute, gather

immutable DTable
    dag::AbstractChunk
end

Dagger.compute(ctx, dt::DTable) = compute(ctx, dt.dag)
Dagger.compute(dt::DTable) = compute(Context(), dt)
Dagger.chunks(dt::DTable) = chunks(dt.dag)

typealias IndexTuple Union{Tuple, NamedTuple}

include("interval.jl")

"""
`TableDomain(interval, nrows)`

metadata about an NDSparse chunk

- `interval`: An `Interval` object with the lowest and the highest index tuples.
- `nrows`: A `Nullable{Int}` of number of rows in the NDSparse, if knowable
           (See design doc section on "Knowability of chunk size")
"""
immutable TableDomain{T<:IndexTuple} <: Domain
    interval::Interval{T}
    nrows::Nullable{Int}
end

function TableDomain(x::IndexTuple, y::IndexTuple, nrows=Nullable{Int}())
    TableDomain(Interval(x,y), nrows)
end

function Dagger.domain(nd::NDSparse)
    TableDomain(first(nd.index), last(nd.index), Nullable{Int}(length(nd)))
end

# all methods of NDSparse that only need the info in TableDomain
Base.eltype{T}(::TableDomain{T}) = T
nrows(td::TableDomain) = td.nrows
Base.length(td::TableDomain) = get(td.nrows) # well when it works
Base.first(td::TableDomain) = first(td.interval)
Base.last(td::TableDomain) = last(td.interval)
Base.ndims(td::TableDomain)  = length(first(td))
function Base.merge(d1::TableDomain, d2::TableDomain, collisions=false)
    n = collisions || isnull(d1.nrows) || isnull(d2.nrows) ?
        Nullable{Int}() :
        Nullable(get(d1.nrows)+get(d2.nrows))
    TableDomain(min(first(d1), first(d2)), max(last(d1), last(d2)), n)
end


"""
Create an `NDSparse` lookup table from a bunch of `TableDomain`s
"""
function lookup_table(subdomains, chunks, lengths)
    index = Columns(map(x->Array{Interval{typeof(x)}}(0), first(subdomains[1].interval))...)
    for subd in subdomains
        int=subd.interval
        push!(index, map(Interval, first(int), last(int)))
    end
    NDSparse(index, Columns(chunks, lengths, names=[:chunk, :length]))
end

function _DTable(chunks::AbstractArray)
    subdomains = map(domain, chunks)
    DTable(Cat(promote_type(map(chunktype, chunks)...),
        reduce(merge, subdomains),
        nothing,
        lookup_table(subdomains, chunks, map(nrows, subdomains)),
       )
     )
end

export distribute

function subdomain(nds, r)
    TableDomain(nds.index[first(r)],
                nds.index[last(r)],
                Nullable(length(nds.index[r])))
end

function subtable(nds, r)
    NDSparse(nds.index[r], nds.data[r])
end

"""
`distribute(nds::NDSparse, rowgroups::AbstractArray)`

Distribute an NDSparse object into chunks of number of
rows specified by `rowgroups`. `rowgroups` is a vector specifying the number of
rows in the respective chunk.

Returns a `DTable`.
"""
function distribute(nds::NDSparse, rowgroups::AbstractArray)
    splits = cumsum([0, rowgroups;])

    if splits[end] != length(nds)
        throw(ArgumentError("the row groups don't add up to total number of rows"))
    end

    ranges = map(UnitRange, splits[1:end-1].+1, splits[2:end])
    subdomains = map(r -> subdomain(nds, r), ranges)

    chunks = map(r->tochunk(subtable(nds, r)), ranges)
    chunkmap = lookup_table(subdomains, chunks, nrows.(domain.(chunks)))

    DTable(Cat(typeof(nds), domain(nds), nothing, chunkmap))
end

"""
`distribute(nds::NDSparse, nchunks::Int=nworkers())`

Distribute an NDSpase object into `nchunks` chunks of equal size.

Returns a `DTable`.
"""
function distribute(nds::NDSparse, nchunks=nworkers())
    N = length(nds)
    q, r = divrem(N, nchunks)
    nrows = vcat(collect(repeated(q, nchunks)))
    nrows[end] += r
    distribute(nds, nrows)
end
