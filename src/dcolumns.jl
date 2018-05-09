# Extract a column as a Dagger array
export getindexcol, getdatacol, dindex, ddata,
       DColumns, column, columns, rows, pairs, as

import Base: keys, values
import IndexedTables: DimName, Columns, column, columns,
       rows, pairs, as, As, Tup, namedtuple, itable

import Dagger: DomainBlocks, ArrayDomain, DArray,
                ArrayOp, domainchunks, chunks, Distribute

const DColumns = DArray{<:Tup}

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

function itable(keycols::DColumns, valuecols::DColumns)
    cs = map(delayed(itable), chunks(keycols), chunks(valuecols))
    cs1 = compute(get_context(),
                  delayed((xs...) -> [xs...]; meta=true)(cs...))
    fromchunks(cs1)
end

function extractarray(t::Union{DNDSparse,DColumns}, accessor)
    arraymaker = function (cs_tup...)
        cs = [cs_tup...]
        lengths = length.(domain.(cs))
        dmnchunks = DomainBlocks((1,), (cumsum(lengths),))
        T = promote_eltype_chunktypes(cs)
        DArray(T, ArrayDomain(1:sum(lengths)), dmnchunks, [cs...], (i, x...)->vcat(x...))
    end

    cs = map(delayed(accessor), t.chunks)
    compute(delayed(arraymaker; meta=true)(cs...))
end

isas(d) = isa(d, As) && d.f !== identity

function columns(t::Union{DNDSparse, DColumns}, which::Tuple...)
    if !isempty(which) && any(isas, which[1])
        return _columns_as(t, which...)
    end

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

function _columns_as(t, which)
    stripas(w) = isa(w, As) ? w.src : w
    which_ = ntuple(i->as(stripas(which[i]), i), length(which))
    cs = columns(t, which_)
    asvecs = find(isas, which)
    outvecs = Any[cs...]
    outvecs[asvecs] = map((w,x) -> w.f(x), [which[asvecs]...], outvecs[asvecs])
    tup = IndexedTables._output_tuple(which)
    tup(outvecs...)
end

for f in [:rows, :keys, :values]
    @eval function $f(t::Union{DNDSparse, ArrayOp}, which::Tuple)
        if !any(isas, which)
            # easy
            extractarray(t, x -> $f(x, which))
        else
            DColumns(columns($f(t), which))
        end
    end
end

for f in [:rows, :keys, :values]
    @eval function $f(t::DNDSparse)
        extractarray(t, x -> $f(x))
    end

    @eval function $f(t::DNDSparse, which::Union{Int, Symbol})
        extractarray(t, x -> $f(x, which))
    end

    @eval function $f(t::DNDSparse, which::As)
        which.f($f(t, which.src))
    end
end

function column(t::DNDSparse, name)
    extractarray(t, x -> column(x, name))
end

columns(t::DNDSparse, which::Union{Int,Symbol,As}) = column(t, which)

function pairs(t::DNDSparse)
    extractarray(t, x -> map(Pair, x.index, x.data))
end

Base.@deprecate getindexcol(t::DNDSparse, dim) keys(t, dim)
Base.@deprecate getdatacol(t::DNDSparse, dim)  values(t, dim)
Base.@deprecate dindex(t::DNDSparse) keys(t)
Base.@deprecate ddata(t::DNDSparse)  values(t)
