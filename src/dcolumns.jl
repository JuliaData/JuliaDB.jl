# Extract a column as a Dagger array
export dindex, ddata

import IndexedTables: DimName
import Dagger: DomainBlocks, ArrayDomain, Cat, ComputedArray

function extractarray(t::DTable, accessor)
    arraymaker = function (cs_tup...)
        cs = [cs_tup...]
        lengths = length.(domain.(cs))
        dmnchunks = DomainBlocks((1,), (cumsum(lengths),))
        Cat(chunktype(cs[1]), ArrayDomain(1:sum(lengths)), dmnchunks, [cs...])
    end

    cs = map(delayed(accessor), t.chunks)
    ComputedArray(compute(delayed(arraymaker;meta=true)(cs...)))
end

dindex(t::DTable) = extractarray(t, nd -> nd.index)
ddata(t::DTable) = extractarray(t, nd -> nd.data)
function ddata(t::DTable, dim::DimName)
    extractarray(t, nd -> dim == 1 && isa(nd.data, Vector) ?
                          nd : nd.data.columns[dim])
end
function dindex(t::DTable, dim::DimName)
    extractarray(t, nd -> dim == 1 && isa(nd.index, Vector) ?
                          nd : nd.index.columns[dim])
end

Base.@deprecate getdatacol(t, dim) ddata(t, dim)
Base.@deprecate getindexcol(t, dim) dindex(t, dim)
