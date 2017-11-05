# Extract a column as a Dagger array
export DColumns, column, columns, rows, pairs

import Base: keys, values
import IndexedTables: DimName, Columns, column, columns,
       rows, pkeys, pairs, Tup, namedtuple, itable

import Dagger: DomainBlocks, ArrayDomain, DArray,
                ArrayOp, domainchunks, chunks, Distribute

import Base.Iterators: PartitionIterator, start, next, done

const TableLike = Union{DNDSparse, DNextTable}

function Iterators.partition(t::TableLike, n::Integer)
    PartitionIterator(t, n)
end

struct PartIteratorState{T}
    chunkno::Int
    chunk::T
    used::Int
end

function start(p::PartitionIterator{<:TableLike})
    c = collect(p.c.chunks[1])
    p = PartIteratorState(1, c, 0)
end

function done(t::PartitionIterator{<:TableLike}, p::PartIteratorState)
    p.chunkno == length(t.c.chunks) && p.used >= length(p.chunk)
end

function next(t::PartitionIterator{<:TableLike}, p::PartIteratorState)
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
                    r = 0
                    used = r
                end
            end
        end
        return part, PartIteratorState(chunkno, nextchunk, used)
    end
end

Base.eltype(iter::PartitionIterator{<:DNextTable}) = NextTable
Base.eltype(iter::PartitionIterator{<:DNDSparse}) = NDSparse

function DColumns(arrays::Tup)
    if length(arrays) == 0
        error("""DColumns must be constructed with at least
                 one column.""")
    end

    i = findfirst(x->isa(x, ArrayOp), arrays)
    wrap = isa(arrays, Tuple) ? tuple :
                                namedtuple(fieldnames(arrays)...)
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

    DArray(T, domain(darrays[1]), domainchunks(darrays[1]), cs, (i, x...)->vcat(x...))
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

function columns(t::Union{TableLike, DArray}, which::Tuple...)

    cs = map(delayed(x->columns(x, which...)), t.chunks)
    f = delayed() do c
        map(tochunk, c)
    end

    tuples = collect(get_context(), treereduce(delayed(vcat), map(f, cs)))

    if isa(tuples, Tuple)
        tuples = [tuples]
    end

    # tuples is a vector of tuples
    map(tuples...) do cstup...
        cs = [cstup...]
        T = chunktype(cs[1])
        ls = length.(domain.(cs))
        d = ArrayDomain((1:sum(ls),))
        dchunks = DomainBlocks((1,), (cumsum(ls),))
        DArray(eltype(T), d, dchunks, cs, (i, x...) -> vcat(x...))
    end
end

Base.@pure IndexedTables.colnames(t::DArray{T}) where T<:Tup = fieldnames(T)

for f in [:rows, :pkeys]
    @eval function $f(t::TableLike)
        extractarray(t, x -> $f(x))
    end

    if f !== :pkeys
        @eval function $f(t::TableLike, which::Union{Int, Symbol})
            extractarray(t, x -> $f(x, which))
        end
    end
end

for f in [:keys, :values]
    @eval function $f(t::DNDSparse)
        extractarray(t, x -> $f(x))
    end

    @eval function $f(t::DNDSparse, which::Union{Int, Symbol})
        extractarray(t, x -> $f(x, which))
    end
end

function column(t::TableLike, name)
    extractarray(t, x -> column(x, name))
end
function column(t::TableLike, xs::AbstractArray)
    # distribute(xs, rows(t).subdomains)
    xs
end

function pairs(t::DNDSparse)
    extractarray(t, x -> map(Pair, x.index, x.data))
end
