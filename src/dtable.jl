using IndexedTables
using Dagger

import Dagger: Domain, DomainSplit, AbstractChunk, Thunk,
               domain, tochunk, chunks, Cat

export compute, gather

#include("interval.jl")
using IntervalTrees

"""
A Distributed NDSparse table
"""
immutable DTable
    dag::AbstractChunk
end
Dagger.compute(ctx, d::DTable) = DTable(compute(ctx, d.dag))
Dagger.gather(ctx, d::DTable) = gather(compute(ctx, d.dag))

chunks(dt::DTable) = chunks(dt.dag)


function Base.show(io::IO, ::MIME"text/plain", dt::DTable)
    print(io, "DTable(chunks=$(size(chunks(dt.dag))))")
end

# Dagger's distribution interface

"""
TableDomain tracks the range of values in the index columns of an
IndexedTable
"""
immutable TableDomain{I} <: Domain
    interval::I
end

function TableDomain(firsts::Tuple, lasts::Tuple)
    TableDomain(Interval(firsts, lasts))
end

"""
returns a TableDomain with the range of indices in an NDSparse
"""
function Dagger.domain(x::NDSparse)
    cols = x.index.columns #::Tuple
    # Intervals of first element, last element in each column
    subdomain(x, 1:length(x))
end

function subdomain(nds::NDSparse, r::UnitRange)
    TableDomain(nds.index[first(r)], nds.index[last(r)])
end

function concatdomain(x::TableDomain, y::TableDomain)
    f = min(first(x.interval), first(y.interval))
    l = max(last(x.interval), last(y.interval))
    TableDomain(f, l)
end

function _merge(x::NDSparse, y::NDSparse)
    dx = domain(x)
    dy = domain(y)
    if dx.interval.last < dy.interval.first
        # fast path
        @show "fastp1"
        vcat(x, y)
    elseif dy.interval.last < dx.interval.first
        @show "fastp2"
        vcat(y, x)
    else
        @show "merge"
        merge(x,y)
    end
end

"""
concatenate many chunks
"""
function Dagger.cat_data{T<:NDSparse}(::Type{T}, td::DomainSplit, xs::AbstractArray)
    out = xs[1]
    for i=2:length(xs)
        out = merge(out, xs[i])
    end
    out
end

function subtable(nds::NDSparse, r::UnitRange)
    NDSparse(nds.index[r], nds.data[r], presorted=true)
end

IntervalTrees.IntervalValue(interval, val) =
    IntervalValue(interval.first, interval.last, val)

function itree(xs::AbstractArray)
    intvs = [IntervalValue(x.interval, i) for (i, x) in enumerate(xs)]
    @show typeof(intvs)
    @show (intvs)
    IntervalTrees.IntervalTree{eltype(intvs), Int}(intvs)
end

immutable TableDomainSet
    # interval tree helps fast lookup of an index
    itree::IntervalTree
    domains::AbstractArray
end

TableDomainSet(xs::AbstractArray) =
    TableDomainSet(itree(xs), xs)

"""
    distribute(nds::NDSparse, nrows::AbstractArray)

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
    subdomains = itree(map(r -> subdomain(nds, r), ranges))

    dmn = DomainSplit(domain(nds), subdomains)
    chunks = map(r->tochunk(subtable(nds, r)), ranges)

    DTable(Cat(typeof(nds), dmn, chunks))
end

"""
    distribute(nds::NDSparse, nchunks::Int=nworkers())

Distribute an NDSpase object into `nchunks` chunks of equal size.

Returns a `DTable`.
"""
function distribute(nds::NDSparse, nchunks=nworkers())
    N = length(nds)
    q, r = divrem(N, nchunks)
    nrows = vcat(collect(repeated(q, nchunks-1)), r == 0 ? q : r)
    distribute(nds, nrows)
end
