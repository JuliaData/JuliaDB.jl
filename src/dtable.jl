import Dagger: Domain, AbstractChunk, Thunk, chunktype,
               domain, domainchunks, tochunk, chunks, Cat


# re-export the essentials
export distribute, chunks, compute, gather

immutable DTable{T}
    dag::Cat{T}
end

Dagger.compute(ctx, dt::DTable) = compute(ctx, dt.dag)
Dagger.compute(dt::DTable) = compute(Context(), dt)
Dagger.chunks(dt::DTable) = chunks(dt.dag)
index(dt::DTable) = chunks(dt.dag).index


typealias IndexTuple Union{Tuple, NamedTuple}

"""
`TableDomain(interval, nrows)`

metadata about an NDSparse chunk

- `interval`: An `Interval` object with the lowest and the highest index tuples.
- `nrows`: A `Nullable{Int}` of number of rows in the NDSparse, if knowable
           (See design doc section on "Knowability of chunk size")
"""
immutable TableDomain{T<:IndexTuple} <: Domain
    interval::Interval{T}
    boundingrect::Interval{T}
    nrows::Nullable{Int}
end

immutable EmptyDomain{T} <: Domain
end

function TableDomain(x::IndexTuple, y::IndexTuple, nrows=Nullable{Int}())
    TableDomain(Interval(x,y), nrows)
end

function Dagger.domain(nd::NDSparse)
    if !isempty(nd)
        interval = Interval(first(nd.index), last(nd.index))
        cs = astuple(nd.index.columns)
        extr = map(extrema, cs)
        boundingrect = Interval(map(first, extr), map(last, extr))
        TableDomain(interval, boundingrect, Nullable{Int}(length(nd)))
    else
        EmptyDomain{eltype(nd.index)}()
    end
end

# many methods of NDSparse that only need the info in a TableDomain
Base.eltype{T}(::TableDomain{T}) = T
Base.eltype{T}(::EmptyDomain{T}) = T

Base.isempty(::EmptyDomain) = true
Base.isempty(::TableDomain) = false

nrows(td::TableDomain) = td.nrows
nrows(td::EmptyDomain) = Nullable(0)

Base.length(td::TableDomain) = get(td.nrows) # well when it works
Base.length(td::EmptyDomain) = 0

Base.ndims{T}(::TableDomain{T})  = nfields(T)
Base.ndims{T}(::EmptyDomain{T})  = nfields(T)

Base.first(td::TableDomain) = first(td.interval)
Base.last(td::TableDomain) = last(td.interval)

mins(td::TableDomain) = first(td.boundingrect)
maxes(td::TableDomain) = last(td.boundingrect)

function Base.merge(d1::TableDomain, d2::TableDomain, collisions=false)
    n = collisions || isnull(d1.nrows) || isnull(d2.nrows) ?
        Nullable{Int}() :
        Nullable(get(d1.nrows) + get(d2.nrows))

    interval = Interval(min(first(d1), first(d2)), max(last(d1), last(d2)))
    boundingrect = Interval(map(min, mins(d1), mins(d2)), map(max, maxes(d1), maxes(d2)))
    TableDomain(interval, boundingrect, n)
end
Base.merge(d1::TableDomain, d2::EmptyDomain) = d1
Base.merge(d1::EmptyDomain, d2::Union{TableDomain, EmptyDomain}) = d2


"""
Create an `NDSparse` lookup table from a bunch of `TableDomain`s
"""
function chunks_index(subdomains, chunks, lengths)

    index = Columns(map(x->Array{Interval{typeof(x)}}(0),
                        first(subdomains[1].interval))...)

    for subd in subdomains
        int=subd.interval
        push!(index, map(Interval, first(int), last(int)))
    end

    NDSparse(index, Columns(chunks, lengths, names=[:chunk, :length]))
end

"""
`fromchunks(chunks::AbstractArray)`

Convenience function to create a DTable from an array of chunks.
The chunks must be non-Thunks. Omits empty chunks in the output.
"""
function fromchunks(chunks::AbstractArray)

    subdomains = map(domain, chunks)
    nzidxs = find(x->!isempty(x), subdomains)
    subdomains = subdomains[nzidxs]
    chunks = chunks[nzidxs]

    DTable(Cat(promote_type(map(chunktype, chunks)...),
            reduce(merge, subdomains),
            nothing,
            chunks_index(subdomains, chunks, map(nrows, subdomains)),
        )
    )
end

### Distribute a NDSparse into a DTable

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
    chunkmap = chunks_index(subdomains, chunks, nrows.(domain.(chunks)))

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

# util
function subdomain(nds, r)
    # TODO: speed it up
    domain(subtable(nds, r))
end

function withchunksindex(f, dt::DTable)
    cs = f(chunks(dt))
    DTable(Cat(chunktype(dt.dag), domain(dt.dag), nothing, cs))
end

"""
`mapchunks(f, nds::NDSparse; keeplengths=true)`

Apply a function to the chunk objects in an index.
Returns an NDSparse. if `keeplength` is false, the output
lengths will all be Nullable{Int}
"""
function mapchunks(f, nds::NDSparse; keeplengths=true)
    cols = astuple(nds.data.columns)
    outchunks = map(f, cols[1])
    outlengths = keeplengths ? cols[2] : Array{Nullable{Int}}(length(cols[2]))
    NDSparse(nds.index,
             Columns(outchunks, outlengths, names=[:chunk, :length]))
end


"""
`mapchunks(f, nds::NDSparse; keeplengths=true)`

Apply a function to the chunk objects in the index of a DTable.
Returns a new DTable. if `keeplength` is false, the output
lengths will all be Nullable{Int}
"""
function mapchunks(f, dt::DTable; keeplengths=true)
    withchunksindex(dt) do cs
        mapchunks(f, cs, keeplengths=keeplengths)
    end
end

import Dagger: thunkize, istask
# Teach dagger how to turn a Cat of NDSparse thunks into
# a single thunk for scheduler execution
function thunkize{S<:NDSparse}(ctx, c::Cat{S})
    if any(istask, chunks(c).data.columns.chunk)
        thunks = map(x -> thunkize(ctx, x), chunks(c).data.columns.chunk)
        Thunk(thunks...; meta=true) do results...
            fromchunks([results...])
        end
    else
        c
    end
end

function Dagger.gather{S<:NDSparse}(ctx, chunk::Cat{S})
    ps_input = chunks(chunk).data.columns.chunk
    ps = Array{chunktype(chunk)}(size(ps_input))
    @sync for i in 1:length(ps_input)
        @async ps[i] = gather(ctx, ps_input[i])
    end
    reduce(merge, ps)
end
