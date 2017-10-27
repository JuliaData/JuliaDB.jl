import Base: collect

import Dagger: chunktype, domain, tochunk, distribute,
               chunks, Context, compute, gather, free!

import IndexedTables: eltypes, astuple, colnames


# re-export the essentials
export distribute, chunks, compute, gather, free!

const IndexTuple = Union{Tuple, NamedTuple}

"""
    IndexSpace(interval, boundingrect, nrows)

Metadata about an `NDSparse`, a chunk of a DNDSparse.

- `interval`: An `Interval` object with the first and the last index tuples.
- `boundingrect`: An `Interval` object with the lowest and the highest indices as tuples.
- `nrows`: A `Nullable{Int}` of number of rows in the NDSparse, if knowable
           (See design doc section on "Knowability of chunk size")
"""
struct IndexSpace{T<:IndexTuple}
    interval::Interval{T}
    boundingrect::Interval{T}
    nrows::Nullable{Int}
end

boundingrect(x::IndexSpace) = x.boundingrect
interval(x::IndexSpace) = x.interval

"""
A distributed NDSparse datastructure.
Can be constructed using [loadfiles](@ref),
[ingest](@ref) or [distribute](@ref)
"""
struct DNDSparse{K,V}
    subdomains::Vector{IndexSpace{K}}
    chunks::Vector
end

Base.eltype(dt::DNDSparse{K,V}) where {K,V} = V
IndexedTables.dimlabels(dt::DNDSparse{K}) where {K} = fieldnames(K)
Base.ndims(dt::DNDSparse{K}) where {K} = nfields(K)
keytype(dt::DNDSparse{K}) where {K} = astuple(K)

# TableLike API
Base.@pure function IndexedTables.colnames{K,V}(t::DNDSparse{K,V})
    dnames = fieldnames(V)
    if all(x->isa(x, Integer), dnames)
        dnames = map(x->x+nfields(K), dnames)
    end
    vcat(fieldnames(K), dnames)
end

const compute_context = Ref{Union{Void, Context}}(nothing)
get_context() = compute_context[] == nothing ? Context() : compute_context[]

"""
    compute(t::DNDSparse; allowoverlap, closed)

Computes any delayed-evaluations in the `DNDSparse`.
The computed data is left on the worker processes.
Subsequent operations on the results will reuse the chunks.

If `allowoverlap` is false then the computed data is re-sorted if required to have no
chunks with overlapping index ranges if necessary.

If `closed` is true then the computed data is re-sorted if required to have no
chunks with overlapping OR continuous boundaries.

See also [`collect`](@ref).

!!! warning
    `compute(t)` requires at least as much memory as the size of the
    result of the computing `t`. You usually don't need to do this for the whole dataset.
    If the result is expected to be big, try `compute(save(t, "output_dir"))` instead.
    See [`save`](@ref) for more.
"""
compute(t::DNDSparse; kwargs...) = compute(get_context(), t; kwargs...)

function compute(ctx, t::DNDSparse; allowoverlap=false, closed=false)
    if any(Dagger.istask, t.chunks)
        # we need to splat `thunks` so that Dagger knows the inputs
        # are thunks and they need to be staged for scheduling
        vec_thunk = delayed((refs...) -> [refs...]; meta=true)(t.chunks...)
        cs = compute(ctx, vec_thunk) # returns a vector of Chunk objects
        t1 = fromchunks(cs, allowoverlap=allowoverlap, closed=closed)
        compute(t1)
    else
        map(Dagger.unrelease, t.chunks) # don't let this be freed
        foreach(Dagger.persist!, t.chunks)
        t
    end
end

function free!(t::DNDSparse)
    foreach(c -> free!(c, force=true), t.chunks)
end

"""
    collect(t::DNDSparse)

Gets distributed data in a DNDSparse `t` and merges it into
[NDSparse](#IndexedTables.NDSparse) object

!!! warning
    `collect(t)` requires at least as much memory as the size of the
    result of the computing `t`. If the result is expected to be big,
    try `compute(save(t, "output_dir"))` instead. See [`save`](@ref) for more.
    This data can be loaded later using [`load`](@ref).
"""
collect(t::DNDSparse) = collect(get_context(), t)

function collect(ctx::Context, dt::DNDSparse{T}) where T
    cs = dt.chunks
    if length(cs) > 0
        collect(ctx, treereduce(delayed(_merge), cs))
    else
        error("Empty table")
    end
end

# Fast-path merge if the data don't overlap
function _merge(f, a::NDSparse, b::NDSparse)
    if isempty(a)
        b
    elseif isempty(b)
        a
    elseif last(a.index) < first(b.index)
        # can vcat
        NDSparse(vcat(a.index, b.index), vcat(a.data, b.data),
              presorted=true, copy=false)
    elseif last(b.index) < first(a.index)
        _merge(b, a)
    else
        f(a, b) # Keep equal index elements
    end
end

_merge(f, x::NDSparse) = x
function _merge(f, x::NDSparse, y::NDSparse, ys::NDSparse...)
    treereduce((a,b)->_merge(f, a, b), [x,y,ys...])
end

_merge(x::NDSparse, y::NDSparse...) = _merge((a,b) -> merge(a, b, agg=nothing), x, y...)

"""
    map(f, t::DNDSparse)

Applies a function `f` on every element in the data of table `t`.
"""
Base.map(f, dt::DNDSparse) = mapchunks(c->map(f, c), dt)

function Base.reduce(f, dt::DNDSparse)
    cs = map(delayed(c->reduce(f, c)), dt.chunks)
    collect(get_context(), treereduce(delayed(f), cs))
end

struct EmptySpace{T} end

# Teach dagger how to automatically figure out the
# metadata (in dagger parlance "domain") about an NDSparse chunk.
function Dagger.domain(nd::NDSparse)
    T = eltype(keys(nd))

    if isempty(nd)
        return EmptySpace{T}()
    end

    wrap = T<:NamedTuple ? T : tuple

    interval = Interval(wrap(first(nd.index)...), wrap(last(nd.index)...))
    cs = astuple(nd.index.columns)
    extr = map(extrema, cs[2:end]) # we use first and last value of first column
    boundingrect = Interval(wrap(first(cs[1]), map(first, extr)...),
                            wrap(last(cs[1]), map(last, extr)...))
    return IndexSpace(interval, boundingrect, Nullable{Int}(length(nd)))
end

function subindexspace(t::Union{NDSparse, NextTable}, r)
    ks = primarykeys(t)
    T = eltype(typeof(ks))
    wrap = T<:NamedTuple ? T : tuple

    if isempty(r)
        return EmptySpace{T}()
    end

    interval = Interval(wrap(ks[first(r)]...), wrap(ks[last(r)]...))
    cs = astuple(columns(ks))
    extr = map(c -> extrema_range(c, r), cs[2:end])

    boundingrect = Interval(wrap(cs[1][first(r)], map(first, extr)...),
                            wrap(cs[1][last(r)], map(last, extr)...))
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
    boundingrect = boxmerge(d1.boundingrect, d2.boundingrect)
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
function index_spaces(t::NDSparse)
    intervals = map(x-> Interval(map(first, x), map(last, x)), t.index)
    boundingrects = map(x-> Interval(map(first, x), map(last, x)), t.data.columns.boundingrect)
    map(IndexSpace, intervals, boundingrects, t.data.columns.length)
end

function trylength(t::DNDSparse)
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
The length of the `DNDSparse` if it can be computed. Will throw an error if not.
You can get the length of such tables after calling [`compute`](@ref) on them.
"""
function Base.length(t::DNDSparse)
    l = trylength(t)
    if isnull(l)
        error("The length of the DNDSparse is not yet known since some of its parts are not yet computed. Call `compute` to compute them, and then call `length` on the result of `compute`.")
    else
        get(l)
    end
end

function _has_overlaps(firsts, lasts, closed)
    if !issorted(firsts)
        p = sortperm(firsts)
        firsts = firsts[p]
        lasts = lasts[p]
    end

    for i = 1:length(firsts)
        s_i = firsts[i]
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

function has_overlaps(subdomains, closed=false)
    _has_overlaps(first.(subdomains), last.(subdomains), closed)
end

function has_overlaps(subdomains, dims::AbstractVector)
    sub(x) = x[dims]
    fs = sub.(first.(subdomains))
    ls = sub.(last.(subdomains))
    _has_overlaps(fs, ls, true)
end

function with_overlaps(f, t::DNDSparse{K,V}, closed=false) where {K,V}
    subdomains = t.subdomains
    chunks = t.chunks

    if isempty(subdomains)
        return t
    end

    if !issorted(subdomains, by=first)
        perm = sortperm(subdomains, by = first)
        subdomains = subdomains[perm]
        chunks = chunks[perm]
    end

    stack = [subdomains[1]]
    groups = [[1]]
    for i in 2:length(subdomains)
        sub = subdomains[i]
        if hasoverlap(stack[end].interval, sub.interval)
            stack[end] = merge(stack[end], sub)
            push!(groups[end], i)
        else
            push!(stack, sub)
            push!(groups, [i])
        end
    end

    cs = collect([f(chunks[group]) for group in groups])
    DNDSparse{K,V}(stack, cs)
end

"""
`fromchunks(chunks::AbstractArray, [subdomains::AbstracArray]; allowoverlap=false)`

Convenience function to create a DNDSparse from an array of chunks.
The chunks must be non-Thunks. Omits empty chunks in the output.
"""
function fromchunks(chunks::AbstractArray,
                    subdomains::AbstractArray = map(domain, chunks);
                    KV = getkvtypes(chunks),
                    closed = false,
                    allowoverlap = false)

    nzidxs = find(x->!isempty(x), subdomains)
    subdomains = subdomains[nzidxs]

    dt = DNDSparse{KV...}(subdomains, chunks[nzidxs])

    if !allowoverlap && has_overlaps(subdomains, closed)
        return rechunk(dt, closed=closed)
    else
        return dt
    end
end

function cache_thunks(t)
    for c in t.chunks
        if isa(c, Dagger.Thunk)
            Dagger.cache_result!(c)
        end
    end
    t
end

function getkvtypes{N<:NDSparse}(::Type{N})
    eltype(N.parameters[3]), N.parameters[1]
end

_promote_type(T,S) = promote_type(T,S)
_promote_type(T::Type{<:IndexTuple}, S::Type{<:IndexTuple}) = map_params(_promote_type, T, S)

function getkvtypes(xs::AbstractArray)
    kvtypes = getkvtypes.(chunktype.(xs))
    K, V = kvtypes[1]
    for (Tk, Tv) in kvtypes[2:end]
        K = _promote_type(Tk, K)
        V = _promote_type(Tv, V)
    end
    (K, V)
end

### Distribute a NDSparse into a DNDSparse

"""
    distribute(itable::NDSparse, rowgroups::AbstractArray)

Distributes an NDSparse object into a DNDSparse by splitting it up into chunks
of `rowgroups` elements. `rowgroups` is a vector specifying the number of
rows in the chunks.

Returns a `DNDSparse`.
"""
function distribute(nds::NDSparse{V}, rowgroups::AbstractArray;
                     allowoverlap = false, closed = false) where V
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
    realK = eltypes(typeof(nds.index.columns))
    cache_thunks(fromchunks(chunks, subdomains, KV = (realK, V),
                            allowoverlap=allowoverlap, closed=closed))
end

"""
    distribute(itable::NDSparse, nchunks::Int=nworkers())

Distributes an NDSparse object into a DNDSparse of `nchunks` chunks
of approximately equal size.

Returns a `DNDSparse`.
"""
function distribute(nds::NDSparse, nchunks::Int=nworkers();
                    allowoverlap = false, closed = false)
    N = length(nds)
    q, r = divrem(N, nchunks)
    nrows = vcat(collect(_repeated(q, nchunks)))
    nrows[end] += r
    distribute(nds, nrows; allowoverlap=allowoverlap, closed=closed)
end

"""
    mapchunks(f, t::DNDSparse; keeplengths=true)

Applies a function to each chunk in `t`. Returns a new DNDSparse.
If `keeplength` is false, this means that the lengths of the output
chunks is unknown before [`compute`](@ref). This function is used
internally by many DNDSparse operations.
"""
function mapchunks(f, t::DNDSparse{K,V}; keeplengths=true) where {K,V}
    chunks = map(delayed(f), t.chunks)
    if keeplengths
        DNDSparse{K, V}(t.subdomains, chunks)
    else
        DNDSparse{K, V}(map(null_length, t.subdomains), chunks)
    end
end

function null_length(x::IndexSpace)
    IndexSpace(x.interval, x.boundingrect, Nullable{Int}())
end

function subtable{K, V}(t::DNDSparse{K,V}, idx::UnitRange)
    if isnull(trylength(t))
        t = compute(t)
    end
    if isempty(idx)
        return DNDSparse{K, V}(similar(t.subdomains, 0), similar(t.chunks, 0))
    end
    ls = map(x->get(nrows(x)), t.subdomains)
    cumls = cumsum(ls)
    i = searchsortedlast(cumls, first(idx))
    j = searchsortedfirst(cumls, last(idx))

    # clip first and last chunks
    strt = first(idx) - get(cumls, i-1, 0)
    fin = cumls[j] - last(idx)

    ds = t.subdomains[i:j]
    cs = convert(Vector{Any}, t.chunks[i:j])
    if i==j
        cs[1] = delayed(x->subtable(x, strt:length(x)-fin))(cs[1])
        i = ds[1]
        ds[1] = IndexSpace(i.interval, i.boundingrect, Nullable(length(idx)))
    else
        cs[1] = delayed(x->subtable(x, strt:length(x)))(cs[1])
        cs[end] = delayed(x->subtable(x, 1:length(x)-fin))(cs[end])
        ds[1] = null_length(ds[1])
        ds[end] = null_length(ds[2])
    end

    DNDSparse{K,V}(ds, cs)
end

Base.@deprecate_binding DTable DNDSparse
