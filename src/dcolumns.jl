# Extract a column as a Dagger array
export getindexcol, getdatacol, dindex, ddata,
       DColumns, column, columns, rows, pairs, as

import Base: keys, values
import IndexedTables: DimName, Columns, column, columns,
       rows, pairs, as, As, Tup, namedtuple, itable

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

function extractarray(t::Union{DTable,DArray}, accessor)
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

isas(d) = isa(d, As) && d.f !== identity

function columns(t::Union{DTable, DArray}, which::Tuple)
    if any(isas, which)
        _columns_as(t, which)
    end

    cs = map(delayed(x->columns(x, which)), t.chunks)
    f = delayed() do c
        map(tochunk, c)
    end

    tuples = collect(get_context(), treereduce(delayed(vcat), map(f, cs)))

    # tuples is a vector of tuples
    map(tuples...) do cstup...
        cs = [cstup...]
        T = chunktype(cs[1])
        ls = length.(domain.(cs))
        d = ArrayDomain((1:sum(ls),))
        dchunks = DomainBlocks((1,), (cumsum(ls),))
        DArray{eltype(T), 1}(d, dchunks, cs)
    end
end

function _columns_as(t, which)
    stripas(w) = isa(w, As) ? w.src : w
    which_ = ntuple(i->as(stripas(which[i]), i), length(which))
    cs = columns(t, which_)
    asvecs = find(isas, which)
    outvecs = Any[cs...]
    outvecs[asvecs] = map((w,x) -> w.f(x), which[asvecs], cs[asvecs])
    tup = IndexedTables._output_tuple(which)
    tup(outvecs...)
end

for f in [:rows, :keys, :values]
    @eval function $f(t::Union{DTable, ArrayOp}, which::Tuple)
        if !any(isas, which)
            # easy
            extractarray(t, x -> $f(x, which))
        else
            DColumns(columns($f(t), which))
        end
    end
end

for f in [:columns, :rows, :keys, :values]
    @eval function $f(t::DTable)
        extractarray(t, x -> $f(x))
    end

    @eval function $f(t::DTable, which::Union{Int, Symbol})
        extractarray(t, x -> $f(x, which))
    end

    @eval function $f(t::DTable, which::As)
        which.f($f(t, which.src))
    end
end

function pairs(t::DTable)
    extractarray(t, x -> map(Pair, x.index, x.data))
end

Base.@deprecate getindexcol(t::DTable, dim) keys(t, dim)
Base.@deprecate getdatacol(t::DTable, dim)  values(t, dim)
Base.@deprecate dindex(t::DTable) keys(t)
Base.@deprecate ddata(t::DTable)  values(t)
