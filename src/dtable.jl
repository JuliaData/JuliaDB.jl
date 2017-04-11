import Dagger: Domain, chunktype, domain, tochunk,
               chunks, compute, gather


# re-export the essentials
export distribute, chunks, compute, gather

const IndexTuple = Union{Tuple, NamedTuple}

"""
A distributed table. Can be constructed using [loadfiles](@ref),
[ingest](@ref) or [distribute](@ref)
"""
immutable DTable{K,V} # T<:Table
    chunks::Table
end

chunks(dt::DTable) = dt.chunks

Base.eltype(dt::DTable) = eltype(chunktype(first(chunks(dt)).chunk))
IndexedTables.dimlabels(dt::DTable) = dimlabels(chunktype(first(chunks(dt)).chunk))
Base.ndims(dt::DTable) = ndims(dt.chunks)

"""
    compute(t::DTable, allowoverlap=true)

Computes any delayed-evaluations in the `DTable`.
The computed data is left on the worker processes.
Subsequent operations on the results will reuse the chunks.

If `allowoverlap` is false then the computed data is resorted to have no
chunks with overlapping index ranges if necessary.

If you expect the result of some operation to be used more than once,
it's better to compute it once and then use it many times.

See also [`gather`](@ref).

!!! warning
    `compute(t)` requires at least as much memory as the size of the
    result of the computing `t`. If the result is expected to be big,
    try `compute(save(t, "output_dir"))` instead. See [`save`](@ref) for more.
"""
compute(t::DTable, allowoverlap=true) = compute(Dagger.Context(), t, allowoverlap)

function compute(ctx, t::DTable, allowoverlap=true)
    chunkcol = chunks(t).data.columns.chunk
    if any(Dagger.istask, chunkcol)
        # we need to splat `thunks` so that Dagger knows the inputs
        # are thunks and they need to be staged for scheduling
        vec_thunk = delayed((refs...) -> [refs...]; meta=true)(chunkcol...)
        cs = compute(ctx, vec_thunk) # returns a vector of Chunk objects
        Base.foreach(Dagger.persist!, cs)
        fromchunks(cs, allowoverlap=allowoverlap)
    else
        t
    end
end

"""
    gather(t::DTable)

Gets distributed data in a DTable `t` and merges it into [IndexedTable](@ref) object

!!! warning
    `gather(t)` requires at least as much memory as the size of the
    result of the computing `t`. If the result is expected to be big,
    try `compute(save(t, "output_dir"))` instead. See [`save`](@ref) for more.
    This data can be loaded later using [`load`](@ref).
"""
gather(t::DTable) = gather(Dagger.Context(), t)

function gather{T}(ctx, dt::DTable{T})
    cs = chunks(dt).data.columns.chunk
    if length(cs) > 0
        gather(ctx, treereduce(delayed(_merge), cs))
    else
        error("Empty table")
    end
end

# Fast-path merge if the data don't overlap
function _merge(f, a::Table, b::Table)
    if isempty(a)
        b
    elseif isempty(b)
        a
    elseif last(a.index) < first(b.index)
        # can hcat
        Table(vcat(a.index, b.index), vcat(a.data, b.data))
    elseif last(b.index) < first(a.index)
        _merge(b, a)
    else
        f(a, b) # Keep equal index elements
    end
end

_merge(f, x::Table) = x
function _merge(f, x::Table, y::Table, ys::Table...)
    _merge(f, _merge(f, x,y), _merge(f, ys...))
end

_merge(x::Table, y::Table...) = _merge((a,b) -> merge(a, b, agg=nothing), x, y...)

"""
    map(f, t::DTable)

Applies a function `f` on every element in the data of table `t`.
"""
Base.map(f, dt::DTable) = mapchunks(c->map(f, c), dt)

function Base.reduce(f, dt::DTable)
    cs = mapchunks(c->reduce(f, c), chunks(dt))
    gather(treereduce(delayed(f), cs.data.columns.chunk))
end

"""
    IndexSpace(interval, boundingrect, nrows)

Metadata about a chunk of a DTable.

- `interval`: An `Interval` object with the first and the last index tuples.
- `boundingrect`: An `Interval` object with the lowest and the highest indices as tuples.
- `nrows`: A `Nullable{Int}` of number of rows in the Table, if knowable
           (See design doc section on "Knowability of chunk size")
"""
immutable IndexSpace{T<:IndexTuple}
    interval::Interval{T}
    boundingrect::Interval{T}
    nrows::Nullable{Int}
end

immutable EmptySpace{T} <: Domain end

# Teach dagger how to automatically figure out the
# metadata (in dagger parlance "domain") about an Table chunk.
function Dagger.domain(nd::Table)
    if isempty(nd)
        return EmptySpace{eltype(nd.index)}()
    end

    interval = Interval(first(nd.index), last(nd.index))
    cs = astuple(nd.index.columns)
    extr = map(extrema, cs)
    boundingrect = Interval(map(first, extr), map(last, extr))
    return IndexSpace(interval, boundingrect, Nullable{Int}(length(nd)))
end

function subindexspace(nd::IndexedTable, r)
    if isempty(r)
        return EmptySpace{eltype(nd.index)}()
    end
    interval = Interval(nd.index[first(r)], nd.index[last(r)])
    cs = astuple(nd.index.columns)
    extr = map(c -> extrema_range(c, r), cs)
    boundingrect = Interval(map(first, extr), map(last, extr))
    return IndexSpace(interval, boundingrect, Nullable{Int}(length(r)))
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
`chunks_index(subdomains, chunks)`

- `subdomains`: a vector of subdomains
- `chunks`: a vector of chunks for those corresponding subdomains

Create an lookup table from a bunch of `IndexSpace`s
This lookup table is itself an Table object indexed by the
first and last indices in the chunks. We enforce the constraint
that the chunks must be disjoint to make such an arrangement
possible. But this is kind of silly though since all the lookups
are best done on the bounding boxes. So,
TODO: use an RTree of bounding boxes here.
"""
function chunks_index(subdomains, chunks)

    index = Columns(map(x->Array{Interval{typeof(x)}}(0),
                        first(subdomains[1].interval))...)
    boundingrects = Columns(map(x->Array{Interval{typeof(x)}}(0),
                             first(subdomains[1].boundingrect))...)

    for subd in subdomains
        push!(index, map(Interval, first(subd), last(subd)))
        push!(boundingrects, map(Interval, mins(subd), maxes(subd)))
    end

    Table(index, Columns(boundingrects,
                            chunks, map(x->x.nrows, subdomains),
                            names=[:boundingrect, :chunk, :length]))
end

# given a chunks index constructed above, give an array of
# index spaces spanned by the chunks in the index
function index_spaces(t::Table)
    intervals = map(x-> Interval(map(first, x), map(last, x)), t.index)
    boundingrects = map(x-> Interval(map(first, x), map(last, x)), t.data.columns.boundingrect)
    map(IndexSpace, intervals, boundingrects, t.data.columns.length)
end

function trylength(t::DTable)
    len = Nullable(0)
    for l in chunks(t).data.columns.length
        if !isnull(l) && !isnull(len)
            len = Nullable(get(len) + get(l))
        else
            return Nullable{Int}()
        end
    end
    return len
end

"""
The length of the `DTable` if it can be computed. Will throw an error if not.
You can get the length of such tables after calling [`compute`](@ref) on them.
"""
function Base.length(t::DTable)
    l = trylength(t)
    if isnull(l)
        error("The length of the DTable is not yet known since some of its parts are not yet computed. Call `compute` to compute them, and then call `length` on the result of `compute`.")
    else
        get(l)
    end
end

function has_overlaps(subdomains, closed=false)
    subdomains = sort(subdomains, by = first)
    lasts = map(last, subdomains)
    for i = 1:length(subdomains)
        s_i = first(subdomains[i])
        j = searchsortedfirst(lasts, s_i)

        # allow repeated indices between chunks
        if j != i && j <= length(lasts) && isless(s_i, lasts[j])
            return true
        elseif closed && j != i && j <= length(lasts) && s_i == lasts[j]
            return true
        end
    end
    return false
end

"""
`fromchunks(chunks::AbstractArray, [subdomains::AbstracArray]; allowoverlap=false)`

Convenience function to create a DTable from an array of chunks.
The chunks must be non-Thunks. Omits empty chunks in the output.
"""
function fromchunks(chunks::AbstractArray,
                    subdomains::AbstractArray = map(domain, chunks);
                    KV = getkvtypes(chunks),
                    allowoverlap = false)

    nzidxs = find(x->!isempty(x), subdomains)
    subdomains = subdomains[nzidxs]

    dt = DTable{KV...}(chunks_index(subdomains, chunks[nzidxs]))

    if !allowoverlap && has_overlaps(subdomains)
        return rechunk(dt)
    end
    return dt
end

function getkvtypes{N<:Table}(::Type{N})
    N.parameters[2], N.parameters[1]
end

function getkvtypes(xs::AbstractArray)
    kvtypes = getkvtypes.(chunktype.(xs))
    K, V = kvtypes[1]
    for (Tk, Tv) in kvtypes[2:end]
        K = promote_type(Tk, K)
        V = promote_type(Tv, V)
    end
    K, V
end

### Distribute a Table into a DTable

"""
    distribute(itable::IndexedTable, rowgroups::AbstractArray)

Distributes an IndexedTable object into a DTable by splitting it up into chunks
of `rowgroups` elements. `rowgroups` is a vector specifying the number of
rows in the chunks.

Returns a `DTable`.
"""
function distribute{V,K}(nds::Table{V,K}, rowgroups::AbstractArray)
    splits = cumsum([0, rowgroups;])

    if splits[end] != length(nds)
        throw(ArgumentError("the row groups don't add up to total number of rows"))
    end

    ranges = map(UnitRange, splits[1:end-1].+1, splits[2:end])

    # this works around locality optimizations in Dagger to make
    # sure that the parts get distributed instead of being left on
    # the master process - which would lead to all operations being serial.
    chunks = map(r->delayed(identity)(subtable(nds, r)), ranges)
    subdomains = map(r->subindexspace(nds, r), ranges)
    fromchunks(chunks, subdomains, KV = (K, V))
end

"""
    distribute(itable::IndexedTable, nchunks::Int=nworkers())

Distributes an IndexedTable object into a DTable of `nchunks` chunks
of approximately equal size.

Returns a `DTable`.
"""
function distribute(nds::Table, nchunks::Int=nworkers())
    N = length(nds)
    q, r = divrem(N, nchunks)
    nrows = vcat(collect(_repeated(q, nchunks)))
    nrows[end] += r
    distribute(nds, nrows)
end

# DTable utilities
function subdomain(nds, r)
    # TODO: speed it up
    domain(subtable(nds, r))
end

"""
    mapchunks(f, t::DTable; keeplengths=true)

Applies a function to each chunk in `t`. Returns a new DTable.
If `keeplength` is false, this means that the lengths of the output
chunks is unknown before [`compute`](@ref). This function is used
internally by many DTable operations.
"""
function mapchunks{K,V}(f, dt::DTable{K,V}; keeplengths=true)
    cs = chunks(dt)
    cols = cs.data.columns
    outchunks = map(delayed(f), cols.chunk)
    outlengths = keeplengths ? cols.length : fill(Nullable{Int}(), length(cols.length))
    t = Table(cs.index,
              Columns(cols.boundingrect,
                  outchunks, outlengths,
                  names=[:boundingrect, :chunk, :length]))
    DTable{K, V}(t)
end

