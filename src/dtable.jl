import Dagger: Domain, AbstractChunk, Thunk, chunktype,
               domain, domainchunks, tochunk, chunks, Cat


# re-export the essentials
export distribute, chunks, compute, gather

immutable DTable{T}
    dag::Cat{T}    # A concatenation of chunks
end

Dagger.compute(ctx, dt::DTable) = compute(ctx, dt.dag)
Dagger.compute(dt::DTable) = compute(Context(), dt)
Dagger.chunks(dt::DTable) = chunks(dt.dag)


const IndexTuple = Union{Tuple, NamedTuple}

"""
`TableDomain(interval, nrows)`

metadata about an NDSparse chunk. When storing metadata about a chunk we must be
conservative about what we store. i.e. it is ok to store that a chunk has more
indices than what it actually contains.

- `interval`: An `Interval` object with the first and the last index tuples.
- `boundingrect`: An `Interval` object with the lowest and the highest indices as tuples.
- `nrows`: A `Nullable{Int}` of number of rows in the NDSparse, if knowable
           (See design doc section on "Knowability of chunk size")
"""
immutable TableDomain{T<:IndexTuple} <: Domain
    interval::Interval{T}
    boundingrect::Interval{T}
    nrows::Nullable{Int}
end

immutable EmptyDomain{T} <: Domain end

# Teach dagger how to automatically figure out the
# metadata about an NDSparse chunk.
function Dagger.domain(nd::NDSparse)
    if isempty(nd)
        return EmptyDomain{eltype(nd.index)}()
    end

    interval = Interval(first(nd.index), last(nd.index))
    cs = astuple(nd.index.columns)
    extr = map(extrema, cs)
    boundingrect = Interval(map(first, extr), map(last, extr))
    return TableDomain(interval, boundingrect, Nullable{Int}(length(nd)))
end

Base.eltype{T}(::TableDomain{T}) = T
Base.eltype{T}(::EmptyDomain{T}) = T

Base.isempty(::EmptyDomain) = true
Base.isempty(::TableDomain) = false

nrows(td::TableDomain) = td.nrows
nrows(td::EmptyDomain) = Nullable(0)

Base.ndims{T}(::TableDomain{T})  = nfields(T)
Base.ndims{T}(::EmptyDomain{T})  = nfields(T)

Base.first(td::TableDomain) = first(td.interval)
Base.last(td::TableDomain) = last(td.interval)

mins(td::TableDomain) = first(td.boundingrect)
maxes(td::TableDomain) = last(td.boundingrect)

function Base.merge(d1::TableDomain, d2::TableDomain, collisions=true)
    n = collisions || isnull(d1.nrows) || isnull(d2.nrows) ?
        Nullable{Int}() :
        Nullable(get(d1.nrows) + get(d2.nrows))

    interval = merge(d1.interval, d2.interval)
    boundingrect = merge(d1.boundingrect, d2.boundingrect)
    TableDomain(interval, boundingrect, n)
end
Base.merge(d1::TableDomain, d2::EmptyDomain) = d1
Base.merge(d1::EmptyDomain, d2::Union{TableDomain, EmptyDomain}) = d2

function Base.intersect(d1::TableDomain, d2::TableDomain)
    interval = intersect(d1.interval, d2.interval)
    boundingrect = intersect(d1.boundingrect, d2.boundingrect)
    TableDomain(interval, boundingrect, Nullable{Int}())
end

function Base.intersect(d1::EmptyDomain, d2::Union{TableDomain,EmptyDomain})
    d1
end

"""
`chunks_index(subdomains, chunks, lengths)`

- `subdomains`: a vector of subdomains
- `chunks`: a vector of chunks for those corresponding subdomains
- `lengths`: a vector of nullable Int

Create an lookup table from a bunch of `TableDomain`s
This lookup table is itself an NDSparse object indexed by the
first and last indices in the chunks. We enforce the constraint
that the chunks must be disjoint to make such an arrangement
possible. But this is kind of silly though since all the lookups
are best done on the bounding boxes. So,
TODO: use an RTree of bounding boxes here.
"""
function chunks_index(subdomains, chunks, lengths)

    index = Columns(map(x->Array{Interval{typeof(x)}}(0),
                        first(subdomains[1].interval))...)
    boundingrects = Columns(map(x->Array{Interval{typeof(x)}}(0),
                             first(subdomains[1].interval))...)

    for subd in subdomains
        push!(index, map(Interval, first(subd), last(subd)))
        push!(boundingrects, map(Interval, mins(subd), maxes(subd)))
    end

    NDSparse(index, Columns(boundingrects,
                            chunks, lengths,
                            names=[:boundingrect, :chunk, :length]))
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
            nothing, # We in JuliaDB don't make use of this domainchunks field
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

# DTable utilities
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
    cols = nds.data.columns
    outchunks = map(f, cols.chunk)
    outlengths = keeplengths ? cols.length : Array{Nullable{Int}}(length(cols.length))
    NDSparse(nds.index,
             Columns(cols.boundingrect,
                     outchunks, outlengths,
                     names=[:boundingrect, :chunk, :length]))
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
