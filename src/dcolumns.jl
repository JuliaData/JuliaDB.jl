# Extract a column as a Dagger array
export getindexcol, getdatacol, dindex, ddata

import IndexedTables: DimName
import Dagger: DomainBlocks, ArrayDomain, DArray

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

function getindexcol(t::DTable, dim::DimName)
    extractarray(t, nd -> nd.index.columns[dim])
end

function getdatacol(t::DTable, dim::DimName)
    extractarray(t, nd -> nd.data.columns[dim])
end

function dindex(t::DTable)
    extractarray(t, nd -> nd.index)
end

function dindex(t::DTable, dims::Vector)
    extractarray(t, nd -> Columns(nd.index.columns[[dims...]]))
end

function ddata(t::DTable)
    extractarray(t, nd -> nd.data)
end
