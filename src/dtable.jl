using IndexedTables, NamedTuples, Dagger

import Dagger: Domain, AbstractChunk, Thunk,
               domain, domainchunks, tochunk, chunks, Cat

# reexport the essentials
export NDSparse, compute, gather

immutable DTable
    dag::AbstractChunk
end

Dagger.compute(ctx, dt::DTable) = compute(ctx, dt.dag)
Dagger.compute(dt::DTable) = compute(Context(), dt)

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
Base.length(td::TableDomain) = get(td.nrows) # well when it works
Base.ndims(td::TableDomain)  = length(first(td.interval))


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
`distribute(nds::NDSparse, nrows::AbstractArray)`

Distribute an NDSparse object into chunks of number of
rows specified by `nrows`. `nrows` is a vector specifying the number of
rows in the respective chunk.

Returns a `DTable`.
"""
function distribute(nds::NDSparse, nrows::AbstractArray)
    splits = cumsum([0, nrows;])

    if splits[end] != length(nds)
        throw(ArgumentError("the row groups don't add up to total number of rows"))
    end

    ranges = map(UnitRange, splits[1:end-1].+1, splits[2:end])
    subdomains = map(r -> subdomain(nds, r), ranges)

    chunks = map(r->tochunk(subtable(nds, r)), ranges)
    @show chunks
    chunkmap = lookup_table(subdomains, chunks, length.(domain.(chunks)))

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
    nrows = vcat(collect(repeated(q, nchunks-1)), r == 0 ? q : r)
    distribute(nds, nrows)
end
