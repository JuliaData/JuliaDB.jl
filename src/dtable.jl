import Dagger: Domain, chunktype, domain, tochunk,
               chunks, compute, gather


# re-export the essentials
export distribute, chunks, compute, gather

const IndexTuple = Union{Tuple, NamedTuple}

"""
    IndexSpace(interval, boundingrect, nrows)

Metadata about an `IndexedTable`, a chunk of a DTable.

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

boundingrect(x::IndexSpace) = x.boundingrect
interval(x::IndexSpace) = x.interval

"""
A distributed table. Can be constructed using [loadfiles](@ref),
[ingest](@ref) or [distribute](@ref)
"""
immutable DTable{K,V}
    subdomains::Vector{IndexSpace{K}}
    chunks::Vector
end

Base.eltype{K,V}(dt::DTable{K,V}) = V
IndexedTables.dimlabels(dt::DTable) = dimlabels(chunktype(first(dt.chunks))) # XXX: doesn't work if first chunk is a thunk
Base.ndims{K}(dt::DTable{K}) = nfields(K)

"""
    compute(t::DTable; allowoverlap, closed)

Computes any delayed-evaluations in the `DTable`.
The computed data is left on the worker processes.
Subsequent operations on the results will reuse the chunks.

If `allowoverlap` is false then the computed data is re-sorted if required to have no
chunks with overlapping index ranges if necessary.

If `closed` is true then the computed data is re-sorted if required to have no
chunks with overlapping OR continuous boundaries.

See also [`gather`](@ref).

!!! warning
    `compute(t)` requires at least as much memory as the size of the
    result of the computing `t`. You usually don't need to do this for the whole dataset.
    If the result is expected to be big, try `compute(save(t, "output_dir"))` instead.
    See [`save`](@ref) for more.
"""
compute(t::DTable; kwargs...) = compute(Dagger.Context(), t; kwargs...)

function compute(ctx, t::DTable; allowoverlap=false, closed=false)
    if any(Dagger.istask, t.chunks)
        # we need to splat `thunks` so that Dagger knows the inputs
        # are thunks and they need to be staged for scheduling
        vec_thunk = delayed((refs...) -> [refs...]; meta=true)(t.chunks...)
        cs = compute(ctx, vec_thunk) # returns a vector of Chunk objects
        t = fromchunks(cs, allowoverlap=allowoverlap, closed=closed)
        cache_thunks(t)
    else
        t
    end
end

"""
    gather(t::DTable)

Gets distributed data in a DTable `t` and merges it into
[IndexedTable](#IndexedTables.IndexedTable) object

!!! warning
    `gather(t)` requires at least as much memory as the size of the
    result of the computing `t`. If the result is expected to be big,
    try `compute(save(t, "output_dir"))` instead. See [`save`](@ref) for more.
    This data can be loaded later using [`load`](@ref).
"""
gather(t::DTable) = gather(Dagger.Context(), t)

function gather{T}(ctx, dt::DTable{T})
    cs = dt.chunks
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
    treereduce((a,b)->_merge(f, a, b), [x,y,ys...])
end

_merge(x::Table, y::Table...) = _merge((a,b) -> merge(a, b, agg=nothing), x, y...)

"""
    map(f, t::DTable)

Applies a function `f` on every element in the data of table `t`.
"""
Base.map(f, dt::DTable) = mapchunks(c->map(f, c), dt)

function Base.reduce(f, dt::DTable)
    cs = map(delayed(c->reduce(f, c)), dt.chunks)
    gather(treereduce(delayed(f), cs))
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
    extr = map(extrema, cs[2:end]) # we use first and last value of first column
    boundingrect = Interval((first(cs[1]), map(first, extr)...),
                            (last(cs[1]), map(last, extr)...))
    return IndexSpace(interval, boundingrect, Nullable{Int}(length(nd)))
end

function subindexspace(nd::IndexedTable, r)
    if isempty(r)
        return EmptySpace{eltype(nd.index)}()
    end
    interval = Interval(nd.index[first(r)], nd.index[last(r)])
    cs = astuple(nd.index.columns)
    extr = map(c -> extrema_range(c, r), cs[2:end])
    boundingrect = Interval((cs[1][first(r)], map(first, extr)...),
                            (cs[1][last(r)], map(last, extr)...))
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

# given a chunks index constructed above, give an array of
# index spaces spanned by the chunks in the index
function index_spaces(t::Table)
    intervals = map(x-> Interval(map(first, x), map(last, x)), t.index)
    boundingrects = map(x-> Interval(map(first, x), map(last, x)), t.data.columns.boundingrect)
    map(IndexSpace, intervals, boundingrects, t.data.columns.length)
end

function trylength(t::DTable)
    len = Nullable(0)
    for l in map(x->x.nrows, t.subdomains)
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
    if !issorted(subdomains, by=first)
        subdomains = sort(subdomains, by = first)
    end
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
                    closed = false,
                    allowoverlap = false)

    nzidxs = find(x->!isempty(x), subdomains)
    subdomains = subdomains[nzidxs]

    dt = DTable{KV...}(subdomains, chunks[nzidxs])

    if !allowoverlap && has_overlaps(subdomains, closed)
        return rechunk(dt, closed=closed)
    else
        return dt
    end
end

function cache_thunks(dt::DTable)
    for c in dt.chunks
        if isa(c, Dagger.Thunk)
            Dagger.cache_result!(c)
        end
    end
    dt
end

function getkvtypes{N<:Table}(::Type{N})
    N.parameters[2], N.parameters[1]
end

function getkvtypes(xs::AbstractArray)
    kvtypes = getkvtypes.(chunktype.(xs))
    K, V = kvtypes[1]
    for (Tk, Tv) in kvtypes[2:end]
        K = map_params(promote_type, Tk, K)
        V = map_params(promote_type, Tv, V)
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
function distribute{V,K}(nds::Table{V,K}, rowgroups::AbstractArray;
                        allowoverlap = false, closed = false)
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
    cache_thunks(fromchunks(chunks, subdomains, KV = (K, V),
                            allowoverlap=allowoverlap, closed=closed))
end

"""
    distribute(itable::IndexedTable, nchunks::Int=nworkers())

Distributes an IndexedTable object into a DTable of `nchunks` chunks
of approximately equal size.

Returns a `DTable`.
"""
function distribute(nds::Table, nchunks::Int=nworkers();
                    allowoverlap = false, closed = false)
    N = length(nds)
    q, r = divrem(N, nchunks)
    nrows = vcat(collect(_repeated(q, nchunks)))
    nrows[end] += r
    distribute(nds, nrows; allowoverlap=allowoverlap, closed=closed)
end

"""
    mapchunks(f, t::DTable; keeplengths=true)

Applies a function to each chunk in `t`. Returns a new DTable.
If `keeplength` is false, this means that the lengths of the output
chunks is unknown before [`compute`](@ref). This function is used
internally by many DTable operations.
"""
function mapchunks{K,V}(f, t::DTable{K,V}; keeplengths=true)
    chunks = map(delayed(f), t.chunks)
    if keeplengths
        DTable{K, V}(t.subdomains, chunks)
    else
        DTable{K, V}(map(null_length, t.subdomains), chunks)
    end
end

function null_length(x::IndexSpace)
    IndexSpace(x.interval, x.boundingrect, Nullable{Int}())
end
