# Extract a column as a Dagger array
export getindexcol, getdatacol, dindex, ddata

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

function getindexcol(t::DTable, dim::DimName)
    extractarray(t, nd -> nd.index.columns[dim])
end

function getdatacol(t::DTable, dim::DimName)
    extractarray(t, nd -> nd.data.columns[dim])
end

function dindex(t::DTable)
    extractarray(t, nd -> nd.index)
end

function ddata(t::DTable)
    extractarray(t, nd -> nd.data)
end
