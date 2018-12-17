# Extract a column as a Dagger array
export DColumns, column, columns, rows, pairs

import Base: keys, values, iterate
import IndexedTables: DimName, Columns, column, columns,
       rows, pkeys, pairs, Tup, namedtuple, itable

import Dagger: DomainBlocks, ArrayDomain, DArray,
                ArrayOp, domainchunks, chunks, Distribute

import Base.Iterators: PartitionIterator

function Iterators.partition(t::DDataset, n::Integer)
    PartitionIterator(t, n)
end

struct PartIteratorState{T}
    chunkno::Int
    chunk::T
    used::Int
end

function iterate(p::PartitionIterator{<:DDataset})
    state = if !isempty(p.c.chunks)
        PartIteratorState(1, collect(p.c.chunks[1]), 0)
    else
        PartIteratorState(1, collect(p.c), 0)
    end
    return iterate(p, state)
end

function iterate(t::PartitionIterator{<:DDataset}, p::PartIteratorState)
    if p.chunkno == length(t.c.chunks) && p.used >= length(p.chunk)
        return nothing
    end
    if p.used + t.n <= length(p.chunk)
        # easy
        nextpart = subtable(p.chunk, p.used+1:(p.used+t.n))
        return nextpart, PartIteratorState(p.chunkno, p.chunk, p.used + t.n)
    else
        part = subtable(p.chunk, p.used+1:length(p.chunk))
        required = t.n - length(part)
        r = required
        chunkno = p.chunkno
        used = length(p.chunk)
        nextchunk = p.chunk
        while r > 0
            chunkno += 1
            if chunkno > length(t.c.chunks)
                # we're done, last chunk
                return part, PartIteratorState(chunkno-1, nextchunk, used)
            else
                nextchunk = collect(t.c.chunks[chunkno])
                if r > length(nextchunk)
                    part = _merge(part, nextchunk)
                    r -= length(nextchunk)
                    used = length(nextchunk)
                else
                    part = _merge(part, subtable(nextchunk, 1:r))
                    used = r
                    r = 0
                end
            end
        end
        return part, PartIteratorState(chunkno, nextchunk, used)
    end
end

Base.eltype(iter::PartitionIterator{<:DIndexedTable}) = IndexedTable
Base.eltype(iter::PartitionIterator{<:DNDSparse}) = NDSparse

function DColumns(arrays::Tup)
    if length(arrays) == 0
        error("""DColumns must be constructed with at least
                 one column.""")
    end

    i = findfirst(x->isa(x, ArrayOp), arrays)
    wrap = isa(arrays, Tuple) ? tuple :
                                namedtuple(keys(arrays)...)∘tuple
    if i == 0
        error("""At least 1 array passed to
                 DColumns must be a DArray""")
    end

    darrays = asyncmap(arrays) do x
        isa(x, ArrayOp) ? compute(get_context(), x) : x
    end

    dist = domainchunks(darrays[i])
    darrays = map(darrays) do x
        if isa(x, DArray)
            domainchunks(x) == dist ?
                x : error("Distribution incompatible")
        else
            Distribute(dist, x)
        end
    end

    darrays = asyncmap(darrays) do x
        compute(get_context(), x)
    end

    if length(darrays) == 1
        cs = chunks(darrays[1])
        chunkmatrix = reshape(cs, length(cs), 1)
    else
        chunkmatrix = reduce(hcat, map(chunks, darrays))
    end
    cs = mapslices(x -> delayed((c...) -> Columns(wrap(c...)))(x...), chunkmatrix, 2)[:]
    T = isa(arrays, Tuple) ? Tuple{map(eltype, arrays)...} :
        wrap{map(eltype, arrays)...}

    DArray(T, domain(darrays[1]), domainchunks(darrays[1]), cs, dvcat)
end

function itable(keycols::DArray, valuecols::DArray)
    cs = map(delayed(itable), chunks(keycols), chunks(valuecols))
    cs1 = compute(get_context(),
                  delayed((xs...) -> [xs...]; meta=true)(cs...))
    fromchunks(cs1)
end

function extractarray(t, f)
    fromchunks(map(delayed(f), t.chunks))
end

# TODO: do this lazily after compute
# technically it's not necessary to communicate here

function Base.getindex(d::ColDict{<:DArray})
    rows(table(d.columns...; names=d.names))
end

function columns(t::Union{DDataset, DArray})
    cs = delayedmap(t.chunks) do c
        x = columns(c)
        if isa(x, AbstractArray)
            tochunk(x)
        elseif isa(x, Tup)
            map(tochunk, IndexedTables.astuple(x))
        else
            # this should never happen
            error("Columns $which could not be extracted")
        end
    end
    tuples = collect(get_context(), treereduce(delayed(vcat), cs))
    if length(cs) == 1
        tuples = [tuples]
    end

    if isa(tuples[1], Tup)
        arrays = map((xs...)->fromchunks([xs...]), tuples...)
        if t isa DDataset
            names = colnames(t)
        else
            names = fieldnames(eltype(t))
        end
        if all(x -> x isa Symbol, names)
            IndexedTables.namedtuple(names...)(arrays)
        else
            arrays
        end
    else
        fromchunks(tuples)
    end
end

function columns(t::Union{DDataset, DArray}, which::Tuple)
    columns(rows(t, which))
end

# TODO: make sure this is a DArray of Columns!!
Base.@pure IndexedTables.colnames(t::DArray{T}) where T<:Tup = fieldnames(T)

isarrayselect(x) = x isa AbstractArray || x isa Pair{<:Any, <:AbstractArray}

function dist_selector(t, f, which::Tup)
    if any(isarrayselect, which)
        refholder = []
        t1 = compute(t)
        w1 = map(which) do x
            isarrayselect(x) ? distfor(t1, x, refholder) : x
        end
        # this repeats the non-chunks to all other chunks,
        # then queries with the corresponding chunks
        broadcast(t1.chunks, w1...) do x...
            delayed((inp...)->f(inp[1], inp[2:end]))(x...)
        end |> fromchunks
    else
        extractarray(t, x->f(x, which))
    end
end

function dist_selector(t, f, which::AbstractArray)
    which
end

function dist_selector(t, f, which)
    extractarray(t, x->f(x,which))
end

function distfor(t, x::AbstractArray, refholder)
    y = rows(t)
    if length(y) != length(x)
        error("Input column is not the same length as the table")
    end
    d = distribute(x, domainchunks(y))
    push!(refholder, d)
    d.chunks
end

function distfor(t, x::Pair{<:Any, <:AbstractArray}, refholder)
    cs = distfor(t, x[2], refholder)
    [delayed(c->x[1]=>c)(c) for c in cs]
end

function rows(t::Union{DDataset, DArray})
    extractarray(t, rows)
end

function rows(t::Union{DDataset, DArray}, which)
    dist_selector(t, rows, which)
end

function pkeys(t::DIndexedTable)
    if isempty(t.pkey)
        Columns((Base.OneTo(length(compute(t))),))
    else
        extractarray(t, pkeys)
    end
end
pkeys(t::DNDSparse) = keys(t)

for f in [:keys, :values]
    @eval function $f(t::DNDSparse)
        extractarray(t, x -> $f(x))
    end

    @eval function $f(t::DNDSparse, which)
        dist_selector(t, $f, which)
    end
end

function column(t::DDataset, name)
    extractarray(t, x -> column(x, name))
end
function column(t::DDataset, xs::AbstractArray)
    # distribute(xs, rows(t).subdomains)
    xs
end

function pairs(t::DNDSparse)
    extractarray(t, x -> map(Pair, x.index, x.data))
end
