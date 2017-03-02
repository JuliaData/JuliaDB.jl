import Dagger: Domain, chunktype, domain, tochunk,
               chunks, compute, gather


# re-export the essentials
export distribute, chunks, compute, gather

const IndexTuple = Union{Tuple, NamedTuple}

immutable DTable{T,I} # T<:NDSparse
    index_space::I
    chunks::NDSparse
end

function DTable{T<:NDSparse, I}(::Type{T}, index_space::I, cs)
    DTable{T,I}(index_space, cs)
end

chunks(dt::DTable) = dt.chunks
tabletype{T}(dt::DTable{T}) = T

"""
Compute any delayed-evaluation in the distributed table.

The computed data is left on the worker processes.

The first ctx is an optional Dagger.Context object
enumerating processes where any unevaluated chunks must be computed

TODO: Spill to disk
"""
function compute(ctx, t::DTable)
    chunkcol = chunks(t).data.columns.chunk
    if any(Dagger.istask, )
        thunks = chunkcol
        # we need to splat `thunks` so that Dagger knows the inputs
        # are thunks and they need to be staged for scheduling
        vec_thunk = delayed((refs...) -> [refs...]; meta=true)(thunks...)
        chunks = compute(ctx, vec_thunk) # returns a vector of Chunk objects
        fromchunks(chunks)
    else
        t
    end
end

"""
Gather data in a DTable into an NDSparse object

The first ctx is an optional Dagger.Context object
enumerating processes where any unevaluated chunks must be computed
"""
function gather(ctx, c::DTable)
    ps_input = chunks(c).data.columns.chunk
    gather(ctx, Dagger.treereduce(delayed(merge), ps_input))
end


"""
`mapchunks(f, nds::NDSparse; keeplengths=true)`

Delayed application of a function to each chunk in an DTable.
Returns a new DTable. if `keeplength` is false, the output
lengths will all be `Nullable{Int}()`
"""
function mapchunks(f, dt::DTable; keeplengths=true)
    withchunksindex(dt) do cs
        mapchunks(f, cs, keeplengths=keeplengths)
    end
end

"""
`IndexSpace(interval, boundingrect, nrows)`

metadata about an NDSparse chunk. When storing metadata about a chunk we must be
conservative about what we store. i.e. it is ok to store that a chunk has more
indices than what it actually contains.

- `interval`: An `Interval` object with the first and the last index tuples.
- `boundingrect`: An `Interval` object with the lowest and the highest indices as tuples.
- `nrows`: A `Nullable{Int}` of number of rows in the NDSparse, if knowable
           (See design doc section on "Knowability of chunk size")
"""
immutable IndexSpace{T<:IndexTuple}
    interval::Interval{T}
    boundingrect::Interval{T}
    nrows::Nullable{Int}
end

immutable EmptySpace{T} <: Domain end

# Teach dagger how to automatically figure out the
# metadata (in dagger parlance "domain") about an NDSparse chunk.
function Dagger.domain(nd::NDSparse)
    if isempty(nd)
        return EmptySpace{eltype(nd.index)}()
    end

    interval = Interval(first(nd.index), last(nd.index))
    cs = astuple(nd.index.columns)
    extr = map(extrema, cs)
    boundingrect = Interval(map(first, extr), map(last, extr))
    return IndexSpace(interval, boundingrect, Nullable{Int}(length(nd)))
end

Base.eltype{T}(::IndexSpace{T}) = T
Base.eltype{T}(::EmptySpace{T}) = T

Base.isempty(::EmptySpace) = true
Base.isempty(::IndexSpace) = false

nrows(td::IndexSpace) = td.nrows
nrows(td::EmptySpace) = Nullable(0)

Base.ndims{T}(::IndexSpace{T})  = nfields(T)
Base.ndims{T}(::EmptySpace{T})  = nfields(T)

Base.first(td::IndexSpace) = first(td.interval)
Base.last(td::IndexSpace) = last(td.interval)

mins(td::IndexSpace) = first(td.boundingrect)
maxes(td::IndexSpace) = last(td.boundingrect)

function Base.merge(d1::IndexSpace, d2::IndexSpace, collisions=true)
    n = collisions || isnull(d1.nrows) || isnull(d2.nrows) ?
        Nullable{Int}() :
        Nullable(get(d1.nrows) + get(d2.nrows))

    interval = merge(d1.interval, d2.interval)
    boundingrect = merge(d1.boundingrect, d2.boundingrect)
    IndexSpace(interval, boundingrect, n)
end
Base.merge(d1::IndexSpace, d2::EmptySpace) = d1
Base.merge(d1::EmptySpace, d2::Union{IndexSpace, EmptySpace}) = d2

function Base.intersect(d1::IndexSpace, d2::IndexSpace)
    interval = intersect(d1.interval, d2.interval)
    boundingrect = intersect(d1.boundingrect, d2.boundingrect)
    IndexSpace(interval, boundingrect, Nullable{Int}())
end

function Base.intersect(d1::EmptySpace, d2::Union{IndexSpace,EmptySpace})
    d1
end

"""
`chunks_index(subdomains, chunks, lengths)`

- `subdomains`: a vector of subdomains
- `chunks`: a vector of chunks for those corresponding subdomains
- `lengths`: a vector of nullable Int

Create an lookup table from a bunch of `IndexSpace`s
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
    typ = promote_type(map(chunktype, chunks)...)

    idxs = reduce(merge, subdomains)
    DTable(typ, idxs,
           chunks_index(subdomains, chunks[nzidxs], map(nrows, subdomains)))
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

    chunks = map(r->tochunk(subtable(nds, r)), ranges)
    fromchunks(chunks)
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
    DTable(tabletype(dt), dt.index_space, cs)
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

