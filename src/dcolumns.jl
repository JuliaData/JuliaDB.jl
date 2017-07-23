# Extract a column as a Dagger array
export getindexcol, getdatacol, dindex, ddata,
       DColumns, column, columns, rows, pairs, as

import Base: keys, values
import IndexedTables: DimName, Columns, column, columns,
       rows, pairs, as, Tup, namedtuple, itable

import Dagger: DomainBlocks, ArrayDomain, DArray,
                ArrayOp, domainchunks, chunks, Distribute

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

    cs = map(delayed((xs...)->Columns(wrap(xs...))),
        map(chunks, darrays)...)
    T = isa(arrays, Tuple) ? Tuple{map(eltype, arrays)...} :
        wrap{map(eltype, arrays)...}
    DArray{T, 1}(domain(darrays[1]), domainchunks(darrays[1]), cs)
end

function itable(keycols::DArray, valuecols::DArray)
    cs = map(delayed(itable), chunks(keycols), chunks(valuecols))
    cs1 = compute(get_context(),
                  delayed((xs...) -> [xs...]; meta=true)(cs...))
    fromchunks(cs1)
end

function extractarray(t::DTable, accessor)
    arraymaker = function (cs_tup...)
        cs = [cs_tup...]
        lengths = length.(domain.(cs))
        dmnchunks = DomainBlocks((1,), (cumsum(lengths),))
        T = eltype(chunktype(cs[1]))
        n = ndims(chunktype(cs[1]))
        DArray{T,n}(ArrayDomain(1:sum(lengths)), dmnchunks, [cs...])
    end

    cs = map(delayed(accessor), t.chunks)
    compute(delayed(arraymaker; meta=true)(cs...))
end

function column(t::DTable, name)
    extractarray(t, x -> column(x, name))
end

for f in [:columns, :rows, :keys, :values]
    @eval function $f(t::DTable, which...)
        extractarray(t, x -> $f(x, which...))
    end
end

Base.@deprecate getindexcol(t::DTable, dim) keys(t, dim)
Base.@deprecate getdatacol(t::DTable, dim)  values(t, dim)
Base.@deprecate dindex(t::DTable) keys(t)
Base.@deprecate ddata(t::DTable)  values(t)
